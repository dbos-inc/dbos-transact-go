package dbos

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"log/slog"
	"math"
	"math/rand"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	_DBOS_INTERNAL_QUEUE_NAME        = "_dbos_internal_queue"
	_DEFAULT_MAX_TASKS_PER_ITERATION = 100
)

// RateLimiter configures rate limiting for workflow queue execution.
// Rate limits prevent overwhelming external services and provide backpressure.
type RateLimiter struct {
	Limit  int     // Maximum number of workflows to start within the period
	Period float64 // Time period in seconds for the rate limit
}

// WorkflowQueue defines a named queue for workflow execution.
// Queues provide controlled workflow execution with concurrency limits, priority scheduling, and rate limiting.
type WorkflowQueue struct {
	Name                 string       `json:"name"`                        // Unique queue name
	WorkerConcurrency    *int         `json:"workerConcurrency,omitempty"` // Max concurrent workflows per executor
	GlobalConcurrency    *int         `json:"concurrency,omitempty"`       // Max concurrent workflows across all executors
	PriorityEnabled      bool         `json:"priorityEnabled,omitempty"`   // Enable priority-based scheduling
	RateLimit            *RateLimiter `json:"rateLimit,omitempty"`         // Rate limiting configuration
	MaxTasksPerIteration int          `json:"maxTasksPerIteration"`        // Max workflows to dequeue per iteration
}

// QueueOption is a functional option for configuring a workflow queue
type QueueOption func(*WorkflowQueue)

// WithWorkerConcurrency limits the number of workflows this executor can run concurrently from the queue.
// This provides per-executor concurrency control.
func WithWorkerConcurrency(concurrency int) QueueOption {
	return func(q *WorkflowQueue) {
		q.WorkerConcurrency = &concurrency
	}
}

// WithGlobalConcurrency limits the total number of workflows that can run concurrently from the queue
// across all executors. This provides global concurrency control.
func WithGlobalConcurrency(concurrency int) QueueOption {
	return func(q *WorkflowQueue) {
		q.GlobalConcurrency = &concurrency
	}
}

// WithPriorityEnabled enables priority-based scheduling for the queue.
// When enabled, workflows with lower priority numbers are executed first.
func WithPriorityEnabled(enabled bool) QueueOption {
	return func(q *WorkflowQueue) {
		q.PriorityEnabled = enabled
	}
}

// WithRateLimiter configures rate limiting for the queue to prevent overwhelming external services.
// The rate limiter enforces a maximum number of workflow starts within a time period.
func WithRateLimiter(limiter *RateLimiter) QueueOption {
	return func(q *WorkflowQueue) {
		q.RateLimit = limiter
	}
}

// WithMaxTasksPerIteration sets the maximum number of workflows to dequeue in a single iteration.
// This controls batch sizes for queue processing.
func WithMaxTasksPerIteration(maxTasks int) QueueOption {
	return func(q *WorkflowQueue) {
		q.MaxTasksPerIteration = maxTasks
	}
}

// NewWorkflowQueue creates a new workflow queue with the specified name and configuration options.
// The queue must be created before workflows can be enqueued to it using the WithQueue option in RunAsWorkflow.
// Queues provide controlled execution with support for concurrency limits, priority scheduling, and rate limiting.
//
// Example:
//
//	queue := dbos.NewWorkflowQueue(ctx, "email-queue",
//	    dbos.WithWorkerConcurrency(5),
//	    dbos.WithRateLimiter(&dbos.RateLimiter{
//	        Limit:  100,
//	        Period: 60.0, // 100 workflows per minute
//	    }),
//	    dbos.WithPriorityEnabled(true),
//	)
//
//	// Enqueue workflows to this queue:
//	handle, err := dbos.RunAsWorkflow(ctx, SendEmailWorkflow, emailData, dbos.WithQueue("email-queue"))
func NewWorkflowQueue(dbosCtx DBOSContext, name string, options ...QueueOption) WorkflowQueue {
	ctx, ok := dbosCtx.(*dbosContext)
	if !ok {
		return WorkflowQueue{} // Do nothing if the concrete type is not dbosContext
	}
	if ctx.launched.Load() {
		panic("Cannot register workflow queue after DBOS has launched")
	}
	ctx.logger.Debug("Creating new workflow queue", "queue_name", name)

	if _, exists := ctx.queueRunner.workflowQueueRegistry[name]; exists {
		panic(newConflictingRegistrationError(name))
	}

	// Create queue with default settings
	q := WorkflowQueue{
		Name:                 name,
		WorkerConcurrency:    nil,
		GlobalConcurrency:    nil,
		PriorityEnabled:      false,
		RateLimit:            nil,
		MaxTasksPerIteration: _DEFAULT_MAX_TASKS_PER_ITERATION,
	}

	// Apply functional options
	for _, option := range options {
		option(&q)
	}

	// Register the queue in the global registry
	ctx.queueRunner.workflowQueueRegistry[name] = q

	return q
}

type queueRunner struct {
	logger *slog.Logger

	// Queue runner iteration parameters
	baseInterval    float64
	minInterval     float64
	maxInterval     float64
	backoffFactor   float64
	scalebackFactor float64
	jitterMin       float64
	jitterMax       float64

	// Queue registry
	workflowQueueRegistry map[string]WorkflowQueue

	// Channel to signal completion back to the DBOS context
	completionChan chan struct{}
}

func newQueueRunner(logger *slog.Logger) *queueRunner {
	return &queueRunner{
		baseInterval:          1.0,
		minInterval:           1.0,
		maxInterval:           120.0,
		backoffFactor:         2.0,
		scalebackFactor:       0.9,
		jitterMin:             0.95,
		jitterMax:             1.05,
		workflowQueueRegistry: make(map[string]WorkflowQueue),
		completionChan:        make(chan struct{}, 1),
		logger:                logger.With("service", "queue_runner"),
	}
}

func (qr *queueRunner) listQueues() []WorkflowQueue {
	queues := make([]WorkflowQueue, 0, len(qr.workflowQueueRegistry))
	for _, queue := range qr.workflowQueueRegistry {
		queues = append(queues, queue)
	}
	return queues
}

func (qr *queueRunner) run(ctx *dbosContext) {
	pollingInterval := qr.baseInterval

	for {
		hasBackoffError := false

		// Iterate through all queues in the registry
		for queueName, queue := range qr.workflowQueueRegistry {
			// Call DequeueWorkflows for each queue
			dequeuedWorkflows, err := ctx.systemDB.dequeueWorkflows(ctx, dequeueWorkflowsInput{
				queue:              queue,
				executorID:         ctx.executorID,
				applicationVersion: ctx.applicationVersion,
			})
			if err != nil {
				if pgErr, ok := err.(*pgconn.PgError); ok {
					switch pgErr.Code {
					case pgerrcode.SerializationFailure:
						hasBackoffError = true
					case pgerrcode.LockNotAvailable:
						hasBackoffError = true
					}
				} else {
					qr.logger.Error("Error dequeuing workflows from queue", "queue_name", queueName, "error", err)
				}
				continue
			}

			if len(dequeuedWorkflows) > 0 {
				qr.logger.Debug("Dequeued workflows from queue", "queue_name", queueName, "workflows", dequeuedWorkflows)
			}
			for _, workflow := range dequeuedWorkflows {
				// Find the workflow in the registry

				wfName, ok := ctx.workflowCustomNametoFQN.Load(workflow.name)
				if !ok {
					qr.logger.Error("Workflow not found in registry", "workflow_name", workflow.name)
					continue
				}

				registeredWorkflowAny, exists := ctx.workflowRegistry.Load(wfName.(string))
				if !exists {
					qr.logger.Error("workflow function not found in registry", "workflow_name", workflow.name)
					continue
				}
				registeredWorkflow, ok := registeredWorkflowAny.(workflowRegistryEntry)
				if !ok {
					qr.logger.Error("invalid workflow registry entry type", "workflow_name", workflow.name)
					continue
				}

				// Deserialize input
				var input any
				if len(workflow.input) > 0 {
					inputBytes, err := base64.StdEncoding.DecodeString(workflow.input)
					if err != nil {
						qr.logger.Error("failed to decode input for workflow", "workflow_id", workflow.id, "error", err)
						continue
					}
					buf := bytes.NewBuffer(inputBytes)
					dec := gob.NewDecoder(buf)
					if err := dec.Decode(&input); err != nil {
						qr.logger.Error("failed to decode input for workflow", "workflow_id", workflow.id, "error", err)
						continue
					}
				}

				_, err := registeredWorkflow.wrappedFunction(ctx, input, WithWorkflowID(workflow.id))
				if err != nil {
					qr.logger.Error("Error running queued workflow", "error", err)
				}
			}
		}

		// Adjust polling interval based on errors
		if hasBackoffError {
			// Increase polling interval using exponential backoff
			pollingInterval = math.Min(pollingInterval*qr.backoffFactor, qr.maxInterval)
		} else {
			// Scale back polling interval on successful iteration
			pollingInterval = math.Max(qr.minInterval, pollingInterval*qr.scalebackFactor)
		}

		// Apply jitter to the polling interval
		jitter := qr.jitterMin + rand.Float64()*(qr.jitterMax-qr.jitterMin) // #nosec G404 -- non-crypto jitter; acceptable
		sleepDuration := time.Duration(pollingInterval * jitter * float64(time.Second))

		// Sleep with jittered interval, but allow early exit on context cancellation
		select {
		case <-ctx.Done():
			qr.logger.Info("Queue runner stopping due to context cancellation", "cause", context.Cause(ctx))
			qr.completionChan <- struct{}{}
			return
		case <-time.After(sleepDuration):
			// Continue to next iteration
		}
	}
}
