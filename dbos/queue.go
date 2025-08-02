package dbos

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"math"
	"math/rand"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	workflowQueueRegistry = make(map[string]WorkflowQueue)
	_                     = NewWorkflowQueue(_DBOS_INTERNAL_QUEUE_NAME)
)

const (
	_DBOS_INTERNAL_QUEUE_NAME        = "_dbos_internal_queue"
	_DEFAULT_MAX_TASKS_PER_ITERATION = 100
)

// RateLimiter represents a rate limiting configuration
type RateLimiter struct {
	Limit  int
	Period float64
}

type WorkflowQueue struct {
	name                 string
	workerConcurrency    *int
	globalConcurrency    *int
	priorityEnabled      bool
	limiter              *RateLimiter
	maxTasksPerIteration int
}

// queueOption is a functional option for configuring a workflow queue
type queueOption func(*WorkflowQueue)

func WithWorkerConcurrency(concurrency int) queueOption {
	return func(q *WorkflowQueue) {
		q.workerConcurrency = &concurrency
	}
}

func WithGlobalConcurrency(concurrency int) queueOption {
	return func(q *WorkflowQueue) {
		q.globalConcurrency = &concurrency
	}
}

func WithPriorityEnabled(enabled bool) queueOption {
	return func(q *WorkflowQueue) {
		q.priorityEnabled = enabled
	}
}

func WithRateLimiter(limiter *RateLimiter) queueOption {
	return func(q *WorkflowQueue) {
		q.limiter = limiter
	}
}

func WithMaxTasksPerIteration(maxTasks int) queueOption {
	return func(q *WorkflowQueue) {
		q.maxTasksPerIteration = maxTasks
	}
}

// NewWorkflowQueue creates a new workflow queue with optional configuration
func NewWorkflowQueue(name string, options ...queueOption) WorkflowQueue {
	// TODO: Add runtime check for post-initialization registration if needed
	if _, exists := workflowQueueRegistry[name]; exists {
		panic(newConflictingRegistrationError(name))
	}

	// Create queue with default settings
	q := WorkflowQueue{
		name:                 name,
		workerConcurrency:    nil,
		globalConcurrency:    nil,
		priorityEnabled:      false,
		limiter:              nil,
		maxTasksPerIteration: _DEFAULT_MAX_TASKS_PER_ITERATION,
	}

	// Apply functional options
	for _, option := range options {
		option(&q)
	}

	// Register the queue in the global registry
	workflowQueueRegistry[name] = q

	return q
}

func queueRunner(ctx *dbosContext) {
	const (
		baseInterval    = 1.0   // Base interval in seconds
		minInterval     = 1.0   // Minimum polling interval in seconds
		maxInterval     = 120.0 // Maximum polling interval in seconds
		backoffFactor   = 2.0   // Exponential backoff multiplier
		scalebackFactor = 0.9   // Scale back factor for successful iterations
		jitterMin       = 0.95  // Minimum jitter multiplier
		jitterMax       = 1.05  // Maximum jitter multiplier
	)

	pollingInterval := baseInterval

	for {
		hasBackoffError := false

		// Iterate through all queues in the registry
		for queueName, queue := range workflowQueueRegistry {
			getLogger().Debug("Processing queue", "queue_name", queueName)
			// Call DequeueWorkflows for each queue
			dequeuedWorkflows, err := ctx.systemDB.DequeueWorkflows(ctx.queueRunnerCtx, queue, ctx.executorID, ctx.applicationVersion)
			if err != nil {
				if pgErr, ok := err.(*pgconn.PgError); ok {
					switch pgErr.Code {
					case pgerrcode.SerializationFailure:
						hasBackoffError = true
					case pgerrcode.LockNotAvailable:
						hasBackoffError = true
					}
				} else {
					getLogger().Error("Error dequeuing workflows from queue", "queue_name", queueName, "error", err)
				}
				continue
			}

			// Print what was dequeued
			if len(dequeuedWorkflows) > 0 {
				getLogger().Debug("Dequeued workflows from queue", "queue_name", queueName, "workflows", dequeuedWorkflows)
			}
			for _, workflow := range dequeuedWorkflows {
				// Find the workflow in the registry
				registeredWorkflow, exists := ctx.workflowRegistry[workflow.name]
				if !exists {
					getLogger().Error("workflow function not found in registry", "workflow_name", workflow.name)
					continue
				}

				// Deserialize input
				var input any
				if len(workflow.input) > 0 {
					inputBytes, err := base64.StdEncoding.DecodeString(workflow.input)
					if err != nil {
						getLogger().Error("failed to decode input for workflow", "workflow_id", workflow.id, "error", err)
						continue
					}
					buf := bytes.NewBuffer(inputBytes)
					dec := gob.NewDecoder(buf)
					if err := dec.Decode(&input); err != nil {
						getLogger().Error("failed to decode input for workflow", "workflow_id", workflow.id, "error", err)
						continue
					}
				}

				// XXX this demonstrate why contexts cannot be used globally -- the task does not inherit the context used in the program that enqueued it
				_, err := registeredWorkflow.wrappedFunction(ctx, input, WithWorkflowID(workflow.id))
				if err != nil {
					getLogger().Error("Error running queued workflow", "error", err)
				}
			}
		}

		// Adjust polling interval based on errors
		if hasBackoffError {
			// Increase polling interval using exponential backoff
			pollingInterval = math.Min(pollingInterval*backoffFactor, maxInterval)
		} else {
			// Scale back polling interval on successful iteration
			pollingInterval = math.Max(minInterval, pollingInterval*scalebackFactor)
		}

		// Apply jitter to the polling interval
		jitter := jitterMin + rand.Float64()*(jitterMax-jitterMin)
		sleepDuration := time.Duration(pollingInterval * jitter * float64(time.Second))

		// Sleep with jittered interval, but allow early exit on context cancellation
		select {
		case <-ctx.queueRunnerCtx.Done():
			getLogger().Info("Queue runner stopping due to context cancellation")
			return
		case <-time.After(sleepDuration):
			// Continue to next iteration
		}
	}
}
