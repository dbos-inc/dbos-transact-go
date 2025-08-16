package dbos

import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

/*******************************/
/******* WORKFLOW STATUS *******/
/*******************************/

// WorkflowStatusType represents the current execution state of a workflow.
type WorkflowStatusType string

const (
	WorkflowStatusPending         WorkflowStatusType = "PENDING"                        // Workflow is running or ready to run
	WorkflowStatusEnqueued        WorkflowStatusType = "ENQUEUED"                       // Workflow is queued and waiting for execution
	WorkflowStatusSuccess         WorkflowStatusType = "SUCCESS"                        // Workflow completed successfully
	WorkflowStatusError           WorkflowStatusType = "ERROR"                          // Workflow completed with an error
	WorkflowStatusCancelled       WorkflowStatusType = "CANCELLED"                      // Workflow was cancelled (manually or due to timeout)
	WorkflowStatusRetriesExceeded WorkflowStatusType = "MAX_RECOVERY_ATTEMPTS_EXCEEDED" // Workflow exceeded maximum retry attempts
)

// WorkflowStatus contains comprehensive information about a workflow's current state and execution history.
// This struct is returned by workflow status queries and contains both metadata and execution details.
type WorkflowStatus struct {
	ID                 string             `json:"workflow_uuid"`       // Unique identifier for the workflow
	Status             WorkflowStatusType `json:"status"`              // Current execution status
	Name               string             `json:"name"`                // Function name of the workflow
	AuthenticatedUser  *string            `json:"authenticated_user"`  // User who initiated the workflow (if applicable)
	AssumedRole        *string            `json:"assumed_role"`        // Role assumed during execution (if applicable)
	AuthenticatedRoles *string            `json:"authenticated_roles"` // Roles available to the user (if applicable)
	Output             any                `json:"output"`              // Workflow output (available after completion)
	Error              error              `json:"error"`               // Error information (if status is ERROR)
	ExecutorID         string             `json:"executor_id"`         // ID of the executor running this workflow
	CreatedAt          time.Time          `json:"created_at"`          // When the workflow was created
	UpdatedAt          time.Time          `json:"updated_at"`          // When the workflow status was last updated
	ApplicationVersion string             `json:"application_version"` // Version of the application that created this workflow
	ApplicationID      string             `json:"application_id"`      // Application identifier
	Attempts           int                `json:"attempts"`            // Number of execution attempts
	QueueName          string             `json:"queue_name"`          // Queue name (if workflow was enqueued)
	Timeout            time.Duration      `json:"timeout"`             // Workflow timeout duration
	Deadline           time.Time          `json:"deadline"`            // Absolute deadline for workflow completion
	StartedAt          time.Time          `json:"started_at"`          // When the workflow execution actually started
	DeduplicationID    string             `json:"deduplication_id"`    // Deduplication identifier (if applicable)
	Input              any                `json:"input"`               // Input parameters passed to the workflow
	Priority           int                `json:"priority"`            // Execution priority (lower numbers have higher priority)
}

// workflowState holds the runtime state for a workflow execution
type workflowState struct {
	workflowID   string
	stepID       int
	isWithinStep bool
}

// NextStepID returns the next step ID and increments the counter
func (ws *workflowState) NextStepID() int {
	ws.stepID++
	return ws.stepID
}

/********************************/
/******* WORKFLOW HANDLES ********/
/********************************/

// workflowOutcome holds the result and error from workflow execution
type workflowOutcome[R any] struct {
	result R
	err    error
}

// WorkflowHandle provides methods to interact with a running or completed workflow.
// The type parameter R represents the expected return type of the workflow.
// Handles can be used to wait for workflow completion, check status, and retrieve results.
type WorkflowHandle[R any] interface {
	GetResult() (R, error)              // Wait for workflow completion and return the result
	GetStatus() (WorkflowStatus, error) // Get current workflow status without waiting
	GetWorkflowID() string              // Get the unique workflow identifier
}

// baseWorkflowHandle contains common fields and methods for workflow handles
type baseWorkflowHandle struct {
	workflowID  string
	dbosContext DBOSContext
}

// GetStatus returns the current status of the workflow from the database
func (h *baseWorkflowHandle) GetStatus() (WorkflowStatus, error) {
	workflowStatuses, err := h.dbosContext.(*dbosContext).systemDB.listWorkflows(h.dbosContext, listWorkflowsDBInput{
		workflowIDs: []string{h.workflowID},
		loadInput:   true,
		loadOutput:  true,
	})
	if err != nil {
		return WorkflowStatus{}, fmt.Errorf("failed to get workflow status: %w", err)
	}
	if len(workflowStatuses) == 0 {
		return WorkflowStatus{}, newNonExistentWorkflowError(h.workflowID)
	}
	return workflowStatuses[0], nil
}

func (h *baseWorkflowHandle) GetWorkflowID() string {
	return h.workflowID
}

// newWorkflowHandle creates a new workflowHandle with the given parameters
func newWorkflowHandle[R any](ctx DBOSContext, workflowID string, outcomeChan chan workflowOutcome[R]) *workflowHandle[R] {
	return &workflowHandle[R]{
		baseWorkflowHandle: baseWorkflowHandle{
			workflowID:  workflowID,
			dbosContext: ctx,
		},
		outcomeChan: outcomeChan,
	}
}

// newWorkflowPollingHandle creates a new workflowPollingHandle with the given parameters
func newWorkflowPollingHandle[R any](ctx DBOSContext, workflowID string) *workflowPollingHandle[R] {
	return &workflowPollingHandle[R]{
		baseWorkflowHandle: baseWorkflowHandle{
			workflowID:  workflowID,
			dbosContext: ctx,
		},
	}
}

// workflowHandle is a concrete implementation of WorkflowHandle
type workflowHandle[R any] struct {
	baseWorkflowHandle
	outcomeChan chan workflowOutcome[R]
}

// GetResult waits for the workflow to complete and returns the result
func (h *workflowHandle[R]) GetResult() (R, error) {
	outcome, ok := <-h.outcomeChan // Blocking read
	if !ok {
		// Return an error if the channel was closed. In normal operations this would happen if GetResul() is called twice on a handler. The first call should get the buffered result, the second call find zero values (channel is empty and closed).
		return *new(R), errors.New("workflow result channel is already closed. Did you call GetResult() twice on the same workflow handle?")
	}
	// If we are calling GetResult inside a workflow, record the result as a step result
	workflowState, ok := h.dbosContext.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	if isWithinWorkflow {
		encodedOutput, encErr := serialize(outcome.result)
		if encErr != nil {
			return *new(R), newWorkflowExecutionError(workflowState.workflowID, fmt.Sprintf("serializing child workflow result: %v", encErr))
		}
		recordGetResultInput := recordChildGetResultDBInput{
			parentWorkflowID: workflowState.workflowID,
			childWorkflowID:  h.workflowID,
			stepID:           workflowState.NextStepID(),
			output:           encodedOutput,
			err:              outcome.err,
		}
		recordResultErr := h.dbosContext.(*dbosContext).systemDB.recordChildGetResult(h.dbosContext, recordGetResultInput)
		if recordResultErr != nil {
			h.dbosContext.(*dbosContext).logger.Error("failed to record get result", "error", recordResultErr)
			return *new(R), newWorkflowExecutionError(workflowState.workflowID, fmt.Sprintf("recording child workflow result: %v", recordResultErr))
		}
	}
	return outcome.result, outcome.err
}

type workflowPollingHandle[R any] struct {
	baseWorkflowHandle
}

func (h *workflowPollingHandle[R]) GetResult() (R, error) {
	result, err := h.dbosContext.(*dbosContext).systemDB.awaitWorkflowResult(h.dbosContext, h.workflowID)
	if result != nil {
		typedResult, ok := result.(R)
		if !ok {
			return *new(R), newWorkflowUnexpectedResultType(h.workflowID, fmt.Sprintf("%T", new(R)), fmt.Sprintf("%T", result))
		}
		// If we are calling GetResult inside a workflow, record the result as a step result
		workflowState, ok := h.dbosContext.Value(workflowStateKey).(*workflowState)
		isWithinWorkflow := ok && workflowState != nil
		if isWithinWorkflow {
			encodedOutput, encErr := serialize(typedResult)
			if encErr != nil {
				return *new(R), newWorkflowExecutionError(workflowState.workflowID, fmt.Sprintf("serializing child workflow result: %v", encErr))
			}
			recordGetResultInput := recordChildGetResultDBInput{
				parentWorkflowID: workflowState.workflowID,
				childWorkflowID:  h.workflowID,
				stepID:           workflowState.NextStepID(),
				output:           encodedOutput,
				err:              err,
			}
			recordResultErr := h.dbosContext.(*dbosContext).systemDB.recordChildGetResult(h.dbosContext, recordGetResultInput)
			if recordResultErr != nil {
				h.dbosContext.(*dbosContext).logger.Error("failed to record get result", "error", recordResultErr)
				return *new(R), newWorkflowExecutionError(workflowState.workflowID, fmt.Sprintf("recording child workflow result: %v", recordResultErr))
			}
		}
		return typedResult, err
	}
	return *new(R), err
}

/**********************************/
/******* WORKFLOW REGISTRY *******/
/**********************************/
type GenericWrappedWorkflowFunc[P any, R any] func(ctx DBOSContext, input P, opts ...WorkflowOption) (WorkflowHandle[R], error)
type WrappedWorkflowFunc func(ctx DBOSContext, input any, opts ...WorkflowOption) (WorkflowHandle[any], error)

type workflowRegistryEntry struct {
	wrappedFunction WrappedWorkflowFunc
	maxRetries      int
	name            string
}

// Register adds a workflow function to the registry (thread-safe, only once per name)
func registerWorkflow(ctx DBOSContext, workflowFQN string, fn WrappedWorkflowFunc, maxRetries int, customName string) {
	// Skip if we don't have a concrete dbosContext
	c, ok := ctx.(*dbosContext)
	if !ok {
		return
	}

	if c.launched.Load() {
		panic("Cannot register workflow after DBOS has launched")
	}

	// Check if workflow already exists and store atomically using LoadOrStore
	entry := workflowRegistryEntry{
		wrappedFunction: fn,
		maxRetries:      maxRetries,
		name:            customName,
	}

	if _, exists := c.workflowRegistry.LoadOrStore(workflowFQN, entry); exists {
		c.logger.Error("workflow function already registered", "fqn", workflowFQN)
		panic(newConflictingRegistrationError(workflowFQN))
	}

	// We need to get a mapping from custom name to FQN for registry lookups that might not know the FQN (queue, recovery)
	// We also panic if we found the name was already registered (this could happen if registering two different workflows under the same custom name)
	if len(customName) > 0 {
		if _, exists := c.workflowCustomNametoFQN.LoadOrStore(customName, workflowFQN); exists {
			c.logger.Error("workflow function already registered", "custom_name", customName)
			panic(newConflictingRegistrationError(customName))
		}
	} else {
		c.workflowCustomNametoFQN.Store(workflowFQN, workflowFQN) // Store the FQN as the custom name if none was provided
	}
}

func registerScheduledWorkflow(ctx DBOSContext, workflowName string, fn WorkflowFunc, cronSchedule string) {
	// Skip if we don't have a concrete dbosContext
	c, ok := ctx.(*dbosContext)
	if !ok {
		return
	}

	if c.launched.Load() {
		panic("Cannot register scheduled workflow after DBOS has launched")
	}

	c.getWorkflowScheduler().Start()
	var entryID cron.EntryID
	entryID, err := c.getWorkflowScheduler().AddFunc(cronSchedule, func() {
		// Execute the workflow on the cron schedule once DBOS is launched
		if !c.launched.Load() {
			return
		}
		// Get the scheduled time from the cron entry
		entry := c.getWorkflowScheduler().Entry(entryID)
		scheduledTime := entry.Prev
		if scheduledTime.IsZero() {
			// Use Next if Prev is not set, which will only happen for the first run
			scheduledTime = entry.Next
		}
		wfID := fmt.Sprintf("sched-%s-%s", workflowName, scheduledTime)
		opts := []WorkflowOption{
			WithWorkflowID(wfID),
			WithQueue(_DBOS_INTERNAL_QUEUE_NAME),
			withWorkflowName(workflowName),
		}
		_, err := ctx.RunAsWorkflow(ctx, fn, scheduledTime, opts...)
		if err != nil {
			c.logger.Error("failed to run scheduled workflow", "fqn", workflowName, "error", err)
		}
	})
	if err != nil {
		panic(fmt.Sprintf("failed to register scheduled workflow: %v", err))
	}
	c.logger.Info("Registered scheduled workflow", "fqn", workflowName, "cron_schedule", cronSchedule)
}

type workflowRegistrationParams struct {
	cronSchedule string
	maxRetries   int
	name         string
}

type workflowRegistrationOption func(*workflowRegistrationParams)

const (
	_DEFAULT_MAX_RECOVERY_ATTEMPTS = 100

	// Step retry defaults
	_DEFAULT_STEP_BASE_INTERVAL  = 100 * time.Millisecond
	_DEFAULT_STEP_MAX_INTERVAL   = 5 * time.Second
	_DEFAULT_STEP_BACKOFF_FACTOR = 2.0
)

// WithMaxRetries sets the maximum number of retry attempts for workflow recovery.
// If a workflow fails or is interrupted, it will be retried up to this many times.
// After exceeding max retries, the workflow status becomes RETRIES_EXCEEDED.
func WithMaxRetries(maxRetries int) workflowRegistrationOption {
	return func(p *workflowRegistrationParams) {
		p.maxRetries = maxRetries
	}
}

// WithSchedule registers the workflow as a scheduled workflow using cron syntax.
// The schedule string follows standard cron format with second precision.
// Scheduled workflows automatically receive a time.Time input parameter.
func WithSchedule(schedule string) workflowRegistrationOption {
	return func(p *workflowRegistrationParams) {
		p.cronSchedule = schedule
	}
}

func WithWorkflowName(name string) workflowRegistrationOption {
	return func(p *workflowRegistrationParams) {
		p.name = name
	}
}

// RegisterWorkflow registers a function as a durable workflow that can be executed and recovered.
// The function is registered with type safety - P represents the input type and R the return type.
// Types are automatically registered with gob encoding for serialization.
//
// Registration options include:
//   - WithMaxRetries: Set maximum retry attempts for workflow recovery
//   - WithSchedule: Register as a scheduled workflow with cron syntax
//   - WithWorkflowName:: Set a custom name for the workflow
//
// Scheduled workflows receive a time.Time as input representing the scheduled execution time.
//
// Example:
//
//	func MyWorkflow(ctx dbos.DBOSContext, input string) (int, error) {
//	    // workflow implementation
//	    return len(input), nil
//	}
//
//	dbos.RegisterWorkflow(ctx, MyWorkflow)
//
//	// With options:
//	dbos.RegisterWorkflow(ctx, MyWorkflow,
//	    dbos.WithMaxRetries(5),
//	    dbos.WithSchedule("0 0 * * *")) // daily at midnight
//		dbos.WithWorkflowName("MyCustomWorkflowName") // Custom name for the workflow
func RegisterWorkflow[P any, R any](ctx DBOSContext, fn GenericWorkflowFunc[P, R], opts ...workflowRegistrationOption) {
	if ctx == nil {
		panic("ctx cannot be nil")
	}

	if fn == nil {
		panic("workflow function cannot be nil")
	}

	registrationParams := workflowRegistrationParams{
		maxRetries: _DEFAULT_MAX_RECOVERY_ATTEMPTS,
	}

	for _, opt := range opts {
		opt(&registrationParams)
	}

	fqn := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()

	// Registry the input/output types for gob encoding
	var p P
	var r R
	gob.Register(p)
	gob.Register(r)

	// Register a type-erased version of the durable workflow for recovery
	typedErasedWorkflow := WorkflowFunc(func(ctx DBOSContext, input any) (any, error) {
		typedInput, ok := input.(P)
		if !ok {
			wfID, err := ctx.GetWorkflowID()
			if err != nil {
				return nil, fmt.Errorf("failed to get workflow ID: %w", err)
			}
			err = ctx.(*dbosContext).systemDB.updateWorkflowOutcome(WithoutCancel(ctx), updateWorkflowOutcomeDBInput{
				workflowID: wfID,
				status:     WorkflowStatusError,
				err:        newWorkflowUnexpectedInputType(fqn, fmt.Sprintf("%T", typedInput), fmt.Sprintf("%T", input)),
			})
			if err != nil {
				return nil, fmt.Errorf("failed to record unexpected input type error: %w", err)
			}
			return nil, newWorkflowUnexpectedInputType(fqn, fmt.Sprintf("%T", typedInput), fmt.Sprintf("%T", input))
		}
		return fn(ctx, typedInput)
	})

	typeErasedWrapper := WrappedWorkflowFunc(func(ctx DBOSContext, input any, opts ...WorkflowOption) (WorkflowHandle[any], error) {
		opts = append(opts, withWorkflowName(fqn)) // Append the name so ctx.RunAsWorkflow can look it up from the registry to apply registration-time options
		handle, err := ctx.RunAsWorkflow(ctx, typedErasedWorkflow, input, opts...)
		if err != nil {
			return nil, err
		}
		return newWorkflowPollingHandle[any](ctx, handle.GetWorkflowID()), nil // this is only used by recovery and queue runner so far -- queue runner dismisses it
	})
	registerWorkflow(ctx, fqn, typeErasedWrapper, registrationParams.maxRetries, registrationParams.name)

	// If this is a scheduled workflow, register a cron job
	if registrationParams.cronSchedule != "" {
		if reflect.TypeOf(p) != reflect.TypeOf(time.Time{}) {
			panic(fmt.Sprintf("scheduled workflow function must accept a time.Time as input, got %T", p))
		}
		registerScheduledWorkflow(ctx, fqn, typedErasedWorkflow, registrationParams.cronSchedule)
	}
}

/**********************************/
/******* WORKFLOW FUNCTIONS *******/
/**********************************/

type DBOSContextKey string

const workflowStateKey DBOSContextKey = "workflowState"

// GenericWorkflowFunc represents a type-safe workflow function with specific input and output types.
// P is the input parameter type and R is the return type.
// All workflow functions must accept a DBOSContext as their first parameter.
type GenericWorkflowFunc[P any, R any] func(ctx DBOSContext, input P) (R, error)

// WorkflowFunc represents a type-erased workflow function used internally.
type WorkflowFunc func(ctx DBOSContext, input any) (any, error)

type workflowParams struct {
	workflowName       string
	workflowID         string
	queueName          string
	applicationVersion string
	maxRetries         int
	deduplicationID    string
	priority           uint
}

// WorkflowOption is a functional option for configuring workflow execution parameters.
type WorkflowOption func(*workflowParams)

// WithWorkflowID sets a custom workflow ID instead of generating one automatically.
// This is useful for idempotent workflow execution and workflow retrieval.
func WithWorkflowID(id string) WorkflowOption {
	return func(p *workflowParams) {
		p.workflowID = id
	}
}

// WithQueue enqueues the workflow to the specified queue instead of executing immediately.
// Queued workflows will be processed by the queue runner according to the queue's configuration.
func WithQueue(queueName string) WorkflowOption {
	return func(p *workflowParams) {
		p.queueName = queueName
	}
}

// WithApplicationVersion overrides the DBOS Context application version for this workflow.
// This affects workflow recovery.
func WithApplicationVersion(version string) WorkflowOption {
	return func(p *workflowParams) {
		p.applicationVersion = version
	}
}

// WithDeduplicationID sets a deduplication ID for the workflow.
func WithDeduplicationID(id string) WorkflowOption {
	return func(p *workflowParams) {
		p.deduplicationID = id
	}
}

// WithPriority sets the execution priority for the workflow.
func WithPriority(priority uint) WorkflowOption {
	return func(p *workflowParams) {
		p.priority = priority
	}
}

// An internal option we use to map the reflection function name to the registration options.
func withWorkflowName(name string) WorkflowOption {
	return func(p *workflowParams) {
		p.workflowName = name
	}
}

// RunAsWorkflow executes a workflow function with type safety and durability guarantees.
// The workflow can be executed immediately or enqueued for later execution based on options.
// Returns a typed handle that can be used to wait for completion and retrieve results.
//
// The workflow will be automatically recovered if the process crashes or is interrupted.
// All workflow state is persisted to ensure exactly-once execution semantics.
//
// Example:
//
//	handle, err := dbos.RunAsWorkflow(ctx, MyWorkflow, "input string", dbos.WithWorkflowID("my-custom-id"))
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	result, err := handle.GetResult() // blocks until completion
//	if err != nil {
//	    log.Printf("Workflow failed: %v", err)
//	} else {
//	    log.Printf("Result: %v", result)
//	}
func RunAsWorkflow[P any, R any](ctx DBOSContext, fn GenericWorkflowFunc[P, R], input P, opts ...WorkflowOption) (WorkflowHandle[R], error) {
	if ctx == nil {
		return nil, fmt.Errorf("ctx cannot be nil")
	}

	// Add the fn name to the options so we can communicate it with DBOSContext.RunAsWorkflow
	opts = append(opts, withWorkflowName(runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()))

	typedErasedWorkflow := WorkflowFunc(func(ctx DBOSContext, input any) (any, error) {
		return fn(ctx, input.(P))
	})

	handle, err := ctx.(*dbosContext).RunAsWorkflow(ctx, typedErasedWorkflow, input, opts...)
	if err != nil {
		return nil, err
	}

	// If we got a polling handle, return its typed version
	if pollingHandle, ok := handle.(*workflowPollingHandle[any]); ok {
		// We need to convert the polling handle to a typed handle
		typedPollingHandle := newWorkflowPollingHandle[R](pollingHandle.dbosContext, pollingHandle.workflowID)
		return typedPollingHandle, nil
	}

	// Create a typed channel for the user to get a typed handle
	if handle, ok := handle.(*workflowHandle[any]); ok {
		typedOutcomeChan := make(chan workflowOutcome[R], 1)

		go func() {
			defer close(typedOutcomeChan)
			outcome := <-handle.outcomeChan

			resultErr := outcome.err
			var typedResult R
			if typedRes, ok := outcome.result.(R); ok {
				typedResult = typedRes
			} else { // This should never happen
				typedResult = *new(R)
				typeErr := fmt.Errorf("unexpected result type: expected %T, got %T", *new(R), outcome.result)
				resultErr = errors.Join(resultErr, typeErr)
			}

			typedOutcomeChan <- workflowOutcome[R]{
				result: typedResult,
				err:    resultErr,
			}
		}()

		typedHandle := newWorkflowHandle[R](handle.dbosContext, handle.workflowID, typedOutcomeChan)

		return typedHandle, nil
	}

	// Should never happen
	return nil, fmt.Errorf("unexpected workflow handle type: %T", handle)
}

func (c *dbosContext) RunAsWorkflow(_ DBOSContext, fn WorkflowFunc, input any, opts ...WorkflowOption) (WorkflowHandle[any], error) {
	// Apply options to build params
	params := workflowParams{
		applicationVersion: c.GetApplicationVersion(),
	}
	for _, opt := range opts {
		opt(&params)
	}

	// Lookup the registry for registration-time options
	registeredWorkflowAny, exists := c.workflowRegistry.Load(params.workflowName)
	if !exists {
		return nil, newNonExistentWorkflowError(params.workflowName)
	}
	registeredWorkflow, ok := registeredWorkflowAny.(workflowRegistryEntry)
	if !ok {
		return nil, fmt.Errorf("invalid workflow registry entry type for workflow %s", params.workflowName)
	}
	if registeredWorkflow.maxRetries > 0 {
		params.maxRetries = registeredWorkflow.maxRetries
	}
	if len(registeredWorkflow.name) > 0 {
		params.workflowName = registeredWorkflow.name
	}

	// Check if we are within a workflow (and thus a child workflow)
	parentWorkflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isChildWorkflow := ok && parentWorkflowState != nil

	// Prevent spawning child workflows from within a step
	if isChildWorkflow && parentWorkflowState.isWithinStep {
		return nil, newStepExecutionError(parentWorkflowState.workflowID, params.workflowName, "cannot spawn child workflow from within a step")
	}

	if isChildWorkflow {
		// Advance step ID if we are a child workflow
		parentWorkflowState.NextStepID()
	}

	// Generate an ID for the workflow if not provided
	var workflowID string
	if params.workflowID == "" {
		if isChildWorkflow {
			stepID := parentWorkflowState.stepID
			workflowID = fmt.Sprintf("%s-%d", parentWorkflowState.workflowID, stepID)
		} else {
			workflowID = uuid.New().String()
		}
	} else {
		workflowID = params.workflowID
	}

	// Create an uncancellable context for the DBOS operations
	// This detaches it from any deadline or cancellation signal set by the user
	uncancellableCtx := WithoutCancel(c)

	// If this is a child workflow that has already been recorded in operations_output, return directly a polling handle
	if isChildWorkflow {
		childWorkflowID, err := c.systemDB.checkChildWorkflow(uncancellableCtx, parentWorkflowState.workflowID, parentWorkflowState.stepID)
		if err != nil {
			return nil, newWorkflowExecutionError(parentWorkflowState.workflowID, fmt.Sprintf("checking child workflow: %v", err))
		}
		if childWorkflowID != nil {
			return newWorkflowPollingHandle[any](uncancellableCtx, *childWorkflowID), nil
		}
	}

	var status WorkflowStatusType
	if params.queueName != "" {
		status = WorkflowStatusEnqueued
	} else {
		status = WorkflowStatusPending
	}

	// Compute the timeout based on the context deadline, if any
	deadline, ok := c.Deadline()
	if !ok {
		deadline = time.Time{} // No deadline set
	}
	var timeout time.Duration
	if !deadline.IsZero() {
		timeout = time.Until(deadline)
		// The timeout could be in the past, for small deadlines, to propagation delays. If so set it to a minimal value
		if timeout < 0 {
			timeout = 1 * time.Millisecond
		}
	}
	// When enqueuing, we do not set a deadline. It'll be computed with the timeout during dequeue.
	if status == WorkflowStatusEnqueued {
		deadline = time.Time{}
	}

	if params.priority > uint(math.MaxInt) {
		return nil, fmt.Errorf("priority %d exceeds maximum allowed value %d", params.priority, math.MaxInt)
	}
	workflowStatus := WorkflowStatus{
		Name:               params.workflowName,
		ApplicationVersion: params.applicationVersion,
		ExecutorID:         c.GetExecutorID(),
		Status:             status,
		ID:                 workflowID,
		CreatedAt:          time.Now(),
		Deadline:           deadline,
		Timeout:            timeout,
		Input:              input,
		ApplicationID:      c.GetApplicationID(),
		QueueName:          params.queueName,
		DeduplicationID:    params.deduplicationID,
		Priority:           int(params.priority),
	}

	// Init status and record child workflow relationship in a single transaction
	tx, err := c.systemDB.(*sysDB).pool.Begin(uncancellableCtx)
	if err != nil {
		return nil, newWorkflowExecutionError(workflowID, fmt.Sprintf("failed to begin transaction: %v", err))
	}
	defer tx.Rollback(uncancellableCtx) // Rollback if not committed

	// Insert workflow status with transaction
	insertInput := insertWorkflowStatusDBInput{
		status:     workflowStatus,
		maxRetries: params.maxRetries,
		tx:         tx,
	}
	insertStatusResult, err := c.systemDB.insertWorkflowStatus(uncancellableCtx, insertInput)
	if err != nil {
		c.logger.Error("failed to insert workflow status", "error", err, "workflow_id", workflowID)
		return nil, err
	}

	// Return a polling handle if: we are enqueueing, the workflow is already in a terminal state (success or error),
	if len(params.queueName) > 0 || insertStatusResult.status == WorkflowStatusSuccess || insertStatusResult.status == WorkflowStatusError {
		// Commit the transaction to update the number of attempts and/or enact the enqueue
		if err := tx.Commit(uncancellableCtx); err != nil {
			return nil, newWorkflowExecutionError(workflowID, fmt.Sprintf("failed to commit transaction: %v", err))
		}
		return newWorkflowPollingHandle[any](uncancellableCtx, workflowStatus.ID), nil
	}

	// Record child workflow relationship if this is a child workflow
	if isChildWorkflow {
		// Get the step ID that was used for generating the child workflow ID
		childInput := recordChildWorkflowDBInput{
			parentWorkflowID: parentWorkflowState.workflowID,
			childWorkflowID:  workflowID,
			stepName:         params.workflowName,
			stepID:           parentWorkflowState.stepID,
			tx:               tx,
		}
		err = c.systemDB.recordChildWorkflow(uncancellableCtx, childInput)
		if err != nil {
			c.logger.Error("failed to record child workflow", "error", err, "parent_workflow_id", parentWorkflowState.workflowID, "child_workflow_id", workflowID)
			return nil, newWorkflowExecutionError(parentWorkflowState.workflowID, fmt.Sprintf("recording child workflow: %v", err))
		}
	}

	// Channel to receive the outcome from the goroutine
	// The buffer size of 1 allows the goroutine to send the outcome without blocking
	// In addition it allows the channel to be garbage collected
	outcomeChan := make(chan workflowOutcome[any], 1)

	// Create workflow state to track step execution
	wfState := &workflowState{
		workflowID: workflowID,
		stepID:     -1, // Steps are O-indexed
	}

	workflowCtx := WithValue(c, workflowStateKey, wfState)

	// If the workflow has a timeout but no deadline, compute the deadline from the timeout.
	// Else use the durable deadline.
	durableDeadline := time.Time{}
	if insertStatusResult.timeout > 0 && insertStatusResult.workflowDeadline.IsZero() {
		durableDeadline = time.Now().Add(insertStatusResult.timeout)
	} else if !insertStatusResult.workflowDeadline.IsZero() {
		durableDeadline = insertStatusResult.workflowDeadline
	}

	var stopFunc func() bool
	cancelFuncCompleted := make(chan struct{})
	if !durableDeadline.IsZero() {
		workflowCtx, _ = WithTimeout(workflowCtx, time.Until(durableDeadline))
		// Register a cancel function that cancels the workflow in the DB as soon as the context is cancelled
		dbosCancelFunction := func() {
			c.logger.Info("Cancelling workflow", "workflow_id", workflowID)
			err = c.systemDB.cancelWorkflow(uncancellableCtx, workflowID)
			if err != nil {
				c.logger.Error("Failed to cancel workflow", "error", err)
			}
			close(cancelFuncCompleted)
		}
		stopFunc = context.AfterFunc(workflowCtx, dbosCancelFunction)
	}

	// Commit the transaction. This must happen before we start the goroutine to ensure the workflow is found by steps in the database
	if err := tx.Commit(uncancellableCtx); err != nil {
		return nil, newWorkflowExecutionError(workflowID, fmt.Sprintf("failed to commit transaction: %v", err))
	}

	// Run the function in a goroutine
	c.workflowsWg.Add(1)
	go func() {
		defer c.workflowsWg.Done()
		result, err := fn(workflowCtx, input)
		status := WorkflowStatusSuccess

		// If an error occurred, set the status to error
		if err != nil {
			status = WorkflowStatusError
		}

		// If the afterFunc has started, the workflow was cancelled and the status should be set to cancelled
		if stopFunc != nil && !stopFunc() {
			// Wait for the cancel function to complete
			// Note this must happen before we write on the outcome channel (and signal the handler's GetResult)
			<-cancelFuncCompleted
			// Set the status to cancelled and move on so we still record the outcome in the DB
			status = WorkflowStatusCancelled
		}

		recordErr := c.systemDB.updateWorkflowOutcome(uncancellableCtx, updateWorkflowOutcomeDBInput{
			workflowID: workflowID,
			status:     status,
			err:        err,
			output:     result,
		})
		if recordErr != nil {
			c.logger.Error("Error recording workflow outcome", "workflow_id", workflowID, "error", recordErr)
			outcomeChan <- workflowOutcome[any]{result: nil, err: recordErr}
			close(outcomeChan)
			return
		}
		outcomeChan <- workflowOutcome[any]{result: result, err: err}
		close(outcomeChan)
	}()

	return newWorkflowHandle[any](uncancellableCtx, workflowID, outcomeChan), nil
}

/******************************/
/******* STEP FUNCTIONS *******/
/******************************/

// StepFunc represents a type-erased step function used internally.
type StepFunc func(ctx context.Context) (any, error)

// GenericStepFunc represents a type-safe step function with a specific output type R.
type GenericStepFunc[R any] func(ctx context.Context) (R, error)

// stepOptions holds the configuration for step execution using functional options pattern.
type stepOptions struct {
	MaxRetries    int           // Maximum number of retry attempts (0 = no retries)
	BackoffFactor float64       // Exponential backoff multiplier between retries (default: 2.0)
	BaseInterval  time.Duration // Initial delay between retries (default: 100ms)
	MaxInterval   time.Duration // Maximum delay between retries (default: 5s)
	StepName      string        // Custom name for the step (defaults to function name)
}

// setDefaults applies default values to stepOptions
func (opts *stepOptions) setDefaults() {
	if opts.BackoffFactor == 0 {
		opts.BackoffFactor = _DEFAULT_STEP_BACKOFF_FACTOR
	}
	if opts.BaseInterval == 0 {
		opts.BaseInterval = _DEFAULT_STEP_BASE_INTERVAL
	}
	if opts.MaxInterval == 0 {
		opts.MaxInterval = _DEFAULT_STEP_MAX_INTERVAL
	}
}

// StepOption is a functional option for configuring step execution parameters.
type StepOption func(*stepOptions)

// WithStepName sets a custom name for the step. If the step name has already been set
// by a previous call to WithStepName, this option will be ignored to allow
// multiple WithStepName calls without overriding the first one.
func WithStepName(name string) StepOption {
	return func(opts *stepOptions) {
		if opts.StepName == "" {
			opts.StepName = name
		}
	}
}

// WithStepMaxRetries sets the maximum number of retry attempts for the step.
// A value of 0 means no retries (default behavior).
func WithStepMaxRetries(maxRetries int) StepOption {
	return func(opts *stepOptions) {
		opts.MaxRetries = maxRetries
	}
}

// WithBackoffFactor sets the exponential backoff multiplier between retries.
// The delay between retries is calculated as: BaseInterval * (BackoffFactor^(retry-1))
// Default value is 2.0.
func WithBackoffFactor(factor float64) StepOption {
	return func(opts *stepOptions) {
		opts.BackoffFactor = factor
	}
}

// WithBaseInterval sets the initial delay between retries.
// Default value is 100ms.
func WithBaseInterval(interval time.Duration) StepOption {
	return func(opts *stepOptions) {
		opts.BaseInterval = interval
	}
}

// WithMaxInterval sets the maximum delay between retries.
// Default value is 5s.
func WithMaxInterval(interval time.Duration) StepOption {
	return func(opts *stepOptions) {
		opts.MaxInterval = interval
	}
}

// RunAsStep executes a function as a durable step within a workflow.
// Steps provide at-least-once execution guarantees and automatic retry capabilities.
// If a step has already been executed (e.g., during workflow recovery), its recorded
// result is returned instead of re-executing the function.
//
// Steps can be configured with functional options:
//
//	data, err := dbos.RunAsStep(ctx, func(ctx context.Context) ([]byte, error) {
//	    return MyStep(ctx, "https://api.example.com/data")
//	}, dbos.WithStepMaxRetries(3), dbos.WithBaseInterval(500*time.Millisecond))
//
// Available options:
//   - WithStepName: Custom name for the step (only sets if not already set)
//   - WithStepMaxRetries: Maximum retry attempts (default: 0)
//   - WithBackoffFactor: Exponential backoff multiplier (default: 2.0)
//   - WithBaseInterval: Initial delay between retries (default: 100ms)
//   - WithMaxInterval: Maximum delay between retries (default: 5s)
//
// Example:
//
//	func MyStep(ctx context.Context, url string) ([]byte, error) {
//	    resp, err := http.Get(url)
//	    if err != nil {
//	        return nil, err
//	    }
//	    defer resp.Body.Close()
//	    return io.ReadAll(resp.Body)
//	}
//
//	// Within a workflow:
//	data, err := dbos.RunAsStep(ctx, func(ctx context.Context) ([]byte, error) {
//	    return MyStep(ctx, "https://api.example.com/data")
//	}, dbos.WithStepName("FetchData"), dbos.WithStepMaxRetries(3))
//	if err != nil {
//	    return nil, err
//	}
//
// Note that the function passed to RunAsStep must accept a context.Context as its first parameter
// and this context *must* be the one specified in the function's signature (not the context passed to RunAsStep).
// Under the hood, DBOS will augment the step's context and pass it to the function when executing it durably.
func RunAsStep[R any](ctx DBOSContext, fn GenericStepFunc[R], opts ...StepOption) (R, error) {
	if ctx == nil {
		return *new(R), newStepExecutionError("", "", "ctx cannot be nil")
	}

	if fn == nil {
		return *new(R), newStepExecutionError("", "", "step function cannot be nil")
	}

	// Append WithStepName option to ensure the step name is set. This will not erase a user-provided step name
	stepName := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
	opts = append(opts, WithStepName(stepName))

	// Type-erase the function
	typeErasedFn := StepFunc(func(ctx context.Context) (any, error) { return fn(ctx) })

	result, err := ctx.RunAsStep(ctx, typeErasedFn, opts...)
	// Step function could return a nil result
	if result == nil {
		return *new(R), err
	}
	// Otherwise type-check and cast the result
	typedResult, ok := result.(R)
	if !ok {
		return *new(R), fmt.Errorf("unexpected result type: expected %T, got %T", *new(R), result)
	}
	return typedResult, err
}

func (c *dbosContext) RunAsStep(_ DBOSContext, fn StepFunc, opts ...StepOption) (any, error) {
	// Process functional options
	stepOpts := &stepOptions{}
	for _, opt := range opts {
		opt(stepOpts)
	}
	stepOpts.setDefaults()

	// Get workflow state from context
	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return nil, newStepExecutionError("", stepOpts.StepName, "workflow state not found in context: are you running this step within a workflow?")
	}

	// This should not happen when called from the package-level RunAsStep
	if fn == nil {
		return nil, newStepExecutionError(wfState.workflowID, stepOpts.StepName, "step function cannot be nil")
	}

	// If within a step, just run the function directly
	if wfState.isWithinStep {
		return fn(c)
	}

	// Setup step state
	stepState := workflowState{
		workflowID:   wfState.workflowID,
		stepID:       wfState.NextStepID(), // crucially, this increments the step ID on the *workflow* state
		isWithinStep: true,
	}

	// Uncancellable context for DBOS operations
	uncancellableCtx := WithoutCancel(c)

	// Check the step is cancelled, has already completed, or is called with a different name
	recordedOutput, err := c.systemDB.checkOperationExecution(uncancellableCtx, checkOperationExecutionDBInput{
		workflowID: stepState.workflowID,
		stepID:     stepState.stepID,
		stepName:   stepOpts.StepName,
	})
	if err != nil {
		return nil, newStepExecutionError(stepState.workflowID, stepOpts.StepName, fmt.Sprintf("checking operation execution: %v", err))
	}
	if recordedOutput != nil {
		return recordedOutput.output, recordedOutput.err
	}

	// Spawn a child DBOSContext with the step state
	stepCtx := WithValue(c, workflowStateKey, &stepState)

	stepOutput, stepError := fn(stepCtx)

	// Retry if MaxRetries > 0 and the first execution failed
	var joinedErrors error
	if stepError != nil && stepOpts.MaxRetries > 0 {
		joinedErrors = errors.Join(joinedErrors, stepError)

		for retry := 1; retry <= stepOpts.MaxRetries; retry++ {
			// Calculate delay for exponential backoff
			delay := stepOpts.BaseInterval
			if retry > 1 {
				exponentialDelay := float64(stepOpts.BaseInterval) * math.Pow(stepOpts.BackoffFactor, float64(retry-1))
				delay = time.Duration(math.Min(exponentialDelay, float64(stepOpts.MaxInterval)))
			}

			c.logger.Error("step failed, retrying", "step_name", stepOpts.StepName, "retry", retry, "max_retries", stepOpts.MaxRetries, "delay", delay, "error", stepError)

			// Wait before retry
			select {
			case <-c.Done():
				return nil, newStepExecutionError(stepState.workflowID, stepOpts.StepName, fmt.Sprintf("context cancelled during retry: %v", c.Err()))
			case <-time.After(delay):
				// Continue to retry
			}

			// Execute the retry
			stepOutput, stepError = fn(stepCtx)

			// If successful, break
			if stepError == nil {
				break
			}

			// Join the error with existing errors
			joinedErrors = errors.Join(joinedErrors, stepError)

			// If max retries reached, create MaxStepRetriesExceeded error
			if retry == stepOpts.MaxRetries {
				stepError = newMaxStepRetriesExceededError(stepState.workflowID, stepOpts.StepName, stepOpts.MaxRetries, joinedErrors)
				break
			}
		}
	}

	// Record the final result
	dbInput := recordOperationResultDBInput{
		workflowID: stepState.workflowID,
		stepName:   stepOpts.StepName,
		stepID:     stepState.stepID,
		err:        stepError,
		output:     stepOutput,
	}
	recErr := c.systemDB.recordOperationResult(uncancellableCtx, dbInput)
	if recErr != nil {
		return nil, newStepExecutionError(stepState.workflowID, stepOpts.StepName, fmt.Sprintf("recording step outcome: %v", recErr))
	}

	return stepOutput, stepError
}

/****************************************/
/******* WORKFLOW COMMUNICATIONS ********/
/****************************************/

// GenericWorkflowSendInput defines the parameters for sending a message to another workflow.
type GenericWorkflowSendInput[P any] struct {
	DestinationID string // Workflow ID to send the message to
	Message       P      // Message payload (must be gob-encodable)
	Topic         string // Optional topic for message filtering
}

func (c *dbosContext) Send(_ DBOSContext, input WorkflowSendInput) error {
	return c.systemDB.send(c, input)
}

// Send sends a message to another workflow with type safety.
// The message type R is automatically registered for gob encoding.
//
// Send can be called from within a workflow (as a durable step) or from outside workflows.
// When called within a workflow, the send operation becomes part of the workflow's durable state.
//
// Example:
//
//	err := dbos.Send(ctx, dbos.GenericWorkflowSendInput[string]{
//	    DestinationID: "target-workflow-id",
//	    Message:       "Hello from sender",
//	    Topic:         "notifications",
//	})
func Send[P any](ctx DBOSContext, input GenericWorkflowSendInput[P]) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	var typedMessage P
	gob.Register(typedMessage)
	return ctx.Send(ctx, WorkflowSendInput{
		DestinationID: input.DestinationID,
		Message:       input.Message,
		Topic:         input.Topic,
	})
}

// WorkflowRecvInput defines the parameters for receiving messages sent to this workflow.
type WorkflowRecvInput struct {
	Topic   string        // Topic to listen for (empty string receives from default topic)
	Timeout time.Duration // Maximum time to wait for a message
}

func (c *dbosContext) Recv(_ DBOSContext, input WorkflowRecvInput) (any, error) {
	return c.systemDB.recv(c, input)
}

// Recv receives a message sent to this workflow with type safety.
// This function blocks until a message is received or the timeout is reached.
// Messages are consumed in FIFO order and each message is delivered exactly once.
//
// Recv can only be called from within a workflow and becomes part of the workflow's durable state.
// If the workflow is recovered, previously received messages are not re-delivered.
//
// Example:
//
//	message, err := dbos.Recv[string](ctx, dbos.WorkflowRecvInput{
//	    Topic:   "notifications",
//	    Timeout: 30 * time.Second,
//	})
//	if err != nil {
//	    // Handle timeout or error
//	    return err
//	}
//	log.Printf("Received: %s", message)
func Recv[R any](ctx DBOSContext, input WorkflowRecvInput) (R, error) {
	if ctx == nil {
		return *new(R), errors.New("ctx cannot be nil")
	}
	msg, err := ctx.Recv(ctx, input)
	if err != nil {
		return *new(R), err
	}
	// Type check
	var typedMessage R
	if msg != nil {
		var ok bool
		typedMessage, ok = msg.(R)
		if !ok {
			return *new(R), newWorkflowUnexpectedResultType("", fmt.Sprintf("%T", new(R)), fmt.Sprintf("%T", msg))
		}
	}
	return typedMessage, nil
}

// GenericWorkflowSetEventInput defines the parameters for setting a workflow event.
type GenericWorkflowSetEventInput[P any] struct {
	Key     string // Event key identifier
	Message P      // Event value (must be gob-encodable)
}

func (c *dbosContext) SetEvent(_ DBOSContext, input WorkflowSetEventInput) error {
	return c.systemDB.setEvent(c, input)
}

// SetEvent sets a key-value event for the current workflow with type safety.
// Events are persistent and can be retrieved by other workflows using GetEvent.
// The event type R is automatically registered for gob encoding.
//
// SetEvent can only be called from within a workflow and becomes part of the workflow's durable state.
// Setting an event with the same key will overwrite the previous value.
//
// Example:
//
//	err := dbos.SetEvent(ctx, dbos.GenericWorkflowSetEventInput[string]{
//	    Key:     "status",
//	    Message: "processing-complete",
//	})
func SetEvent[P any](ctx DBOSContext, input GenericWorkflowSetEventInput[P]) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	var typedMessage P
	gob.Register(typedMessage)
	return ctx.SetEvent(ctx, WorkflowSetEventInput{
		Key:     input.Key,
		Message: input.Message,
	})
}

// WorkflowGetEventInput defines the parameters for retrieving an event from a workflow.
type WorkflowGetEventInput struct {
	TargetWorkflowID string        // Workflow ID to get the event from
	Key              string        // Event key to retrieve
	Timeout          time.Duration // Maximum time to wait for the event to be set
}

func (c *dbosContext) GetEvent(_ DBOSContext, input WorkflowGetEventInput) (any, error) {
	return c.systemDB.getEvent(c, input)
}

// GetEvent retrieves a key-value event from a target workflow with type safety.
// This function blocks until the event is set or the timeout is reached.
//
// When called within a workflow, the get operation becomes part of the workflow's durable state.
// The returned value is of type R and will be type-checked at runtime.
//
// Example:
//
//	status, err := dbos.GetEvent[string](ctx, dbos.WorkflowGetEventInput{
//	    TargetWorkflowID: "target-workflow-id",
//	    Key:              "status",
//	    Timeout:          30 * time.Second,
//	})
//	if err != nil {
//	    // Handle timeout or error
//	    return err
//	}
//	log.Printf("Status: %s", status)
func GetEvent[R any](ctx DBOSContext, input WorkflowGetEventInput) (R, error) {
	if ctx == nil {
		return *new(R), errors.New("ctx cannot be nil")
	}
	value, err := ctx.GetEvent(ctx, input)
	if err != nil {
		return *new(R), err
	}
	if value == nil {
		return *new(R), nil
	}
	// Type check
	typedValue, ok := value.(R)
	if !ok {
		return *new(R), newWorkflowUnexpectedResultType("", fmt.Sprintf("%T", new(R)), fmt.Sprintf("%T", value))
	}
	return typedValue, nil
}

func (c *dbosContext) Sleep(duration time.Duration) (time.Duration, error) {
	return c.systemDB.sleep(c, duration)
}

// Sleep pauses workflow execution for the specified duration.
// This is a durable sleep - if the workflow is recovered during the sleep period,
// it will continue sleeping for the remaining time.
// Returns the actual duration slept.
//
// Example:
//
//	actualDuration, err := dbos.Sleep(ctx, 5*time.Second)
//	if err != nil {
//	    return err
//	}
func Sleep(ctx DBOSContext, duration time.Duration) (time.Duration, error) {
	if ctx == nil {
		return 0, errors.New("ctx cannot be nil")
	}
	return ctx.Sleep(duration)
}

/***********************************/
/******* WORKFLOW MANAGEMENT *******/
/***********************************/

// GetWorkflowID retrieves the workflow ID from the context if called within a DBOS workflow
func (c *dbosContext) GetWorkflowID() (string, error) {
	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return "", errors.New("not within a DBOS workflow context")
	}
	return wfState.workflowID, nil
}

// GetStepID retrieves the current step ID from the context if called within a DBOS workflow
func (c *dbosContext) GetStepID() (int, error) {
	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return -1, errors.New("not within a DBOS workflow context")
	}
	return wfState.stepID, nil
}

// GetWorkflowID retrieves the workflow ID from the context if called within a DBOS workflow.
// Returns an error if not called from within a workflow context.
//
// Example:
//
//	workflowID, err := dbos.GetWorkflowID(ctx)
//	if err != nil {
//	    log.Printf("Not in a workflow context: %v", err)
//	} else {
//	    log.Printf("Current workflow ID: %s", workflowID)
//	}
func GetWorkflowID(ctx DBOSContext) (string, error) {
	if ctx == nil {
		return "", errors.New("ctx cannot be nil")
	}
	return ctx.GetWorkflowID()
}

// GetStepID retrieves the current step ID from the context if called within a DBOS workflow.
// Returns -1 and an error if not called from within a workflow context.
//
// Example:
//
//	stepID, err := dbos.GetStepID(ctx)
//	if err != nil {
//	    log.Printf("Not in a workflow context: %v", err)
//	} else {
//	    log.Printf("Current step ID: %d", stepID)
//	}
func GetStepID(ctx DBOSContext) (int, error) {
	if ctx == nil {
		return -1, errors.New("ctx cannot be nil")
	}
	return ctx.GetStepID()
}

func (c *dbosContext) RetrieveWorkflow(_ DBOSContext, workflowID string) (WorkflowHandle[any], error) {
	workflowStatus, err := c.systemDB.listWorkflows(c, listWorkflowsDBInput{
		workflowIDs: []string{workflowID},
		loadInput:   true,
		loadOutput:  true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve workflow status: %w", err)
	}
	if len(workflowStatus) == 0 {
		return nil, newNonExistentWorkflowError(workflowID)
	}
	return newWorkflowPollingHandle[any](c, workflowID), nil
}

// RetrieveWorkflow returns a typed handle to an existing workflow.
// The handle can be used to check status and wait for results.
// The type parameter R must match the workflow's actual return type.
//
// Example:
//
//	handle, err := dbos.RetrieveWorkflow[int](ctx, "workflow-id")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	result, err := handle.GetResult() // blocks until completion
//	if err != nil {
//	    log.Printf("Workflow failed: %v", err)
//	} else {
//	    log.Printf("Result: %d", result)
//	}
func RetrieveWorkflow[R any](ctx DBOSContext, workflowID string) (*workflowPollingHandle[R], error) {
	if ctx == nil {
		return nil, errors.New("dbosCtx cannot be nil")
	}

	// Register the output for gob encoding
	var r R
	gob.Register(r)

	workflowStatus, err := ctx.(*dbosContext).systemDB.listWorkflows(ctx, listWorkflowsDBInput{
		workflowIDs: []string{workflowID},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve workflow status: %w", err)
	}
	if len(workflowStatus) == 0 {
		return nil, newNonExistentWorkflowError(workflowID)
	}
	return newWorkflowPollingHandle[R](ctx, workflowID), nil
}

type EnqueueOptions struct {
	WorkflowName       string
	QueueName          string
	WorkflowID         string
	ApplicationVersion string
	DeduplicationID    string
	Priority           uint
	WorkflowTimeout    time.Duration
	WorkflowInput      any
}

func (c *dbosContext) Enqueue(_ DBOSContext, params EnqueueOptions) (WorkflowHandle[any], error) {
	workflowID := params.WorkflowID
	if workflowID == "" {
		workflowID = uuid.New().String()
	}

	var deadline time.Time
	if params.WorkflowTimeout > 0 {
		deadline = time.Now().Add(params.WorkflowTimeout)
	}

	if params.Priority > uint(math.MaxInt) {
		return nil, fmt.Errorf("priority %d exceeds maximum allowed value %d", params.Priority, math.MaxInt)
	}
	status := WorkflowStatus{
		Name:               params.WorkflowName,
		ApplicationVersion: params.ApplicationVersion,
		Status:             WorkflowStatusEnqueued,
		ID:                 workflowID,
		CreatedAt:          time.Now(),
		Deadline:           deadline,
		Timeout:            params.WorkflowTimeout,
		Input:              params.WorkflowInput,
		QueueName:          params.QueueName,
		DeduplicationID:    params.DeduplicationID,
		Priority:           int(params.Priority),
	}

	uncancellableCtx := WithoutCancel(c)

	tx, err := c.systemDB.(*sysDB).pool.Begin(uncancellableCtx)
	if err != nil {
		return nil, newWorkflowExecutionError(workflowID, fmt.Sprintf("failed to begin transaction: %v", err))
	}
	defer tx.Rollback(uncancellableCtx) // Rollback if not committed

	// Insert workflow status with transaction
	insertInput := insertWorkflowStatusDBInput{
		status: status,
		tx:     tx,
	}
	_, err = c.systemDB.insertWorkflowStatus(uncancellableCtx, insertInput)
	if err != nil {
		c.logger.Error("failed to insert workflow status", "error", err, "workflow_id", workflowID)
		return nil, err
	}

	if err := tx.Commit(uncancellableCtx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return newWorkflowPollingHandle[any](uncancellableCtx, workflowID), nil
}

type GenericEnqueueOptions[P any] struct {
	WorkflowName       string
	QueueName          string
	WorkflowID         string
	ApplicationVersion string
	DeduplicationID    string
	Priority           uint
	WorkflowTimeout    time.Duration
	WorkflowInput      P
}

// Enqueue adds a workflow to a named queue for later execution with type safety.
// The workflow will be persisted with ENQUEUED status until picked up by a DBOS process.
// This provides asynchronous workflow execution with durability guarantees.
//
// Parameters:
//   - ctx: DBOS context for the operation
//   - params: Configuration parameters including workflow name, queue name, input, and options
//
// The params struct contains:
//   - WorkflowName: Name of the registered workflow function to execute (required)
//   - QueueName: Name of the queue to enqueue the workflow to (required)
//   - WorkflowID: Custom workflow ID (optional, auto-generated if empty)
//   - ApplicationVersion: Application version override (optional)
//   - DeduplicationID: Deduplication identifier for idempotent enqueuing (optional)
//   - WorkflowTimeout: Maximum execution time for the workflow (optional)
//   - WorkflowInput: Input parameters to pass to the workflow (type P)
//
// Returns a typed workflow handle that can be used to check status and retrieve results.
// The handle uses polling to check workflow completion since the execution is asynchronous.
//
// Example usage:
//
//	// Enqueue a workflow with string input and int output
//	handle, err := dbos.Enqueue[string, int](ctx, dbos.GenericEnqueueOptions[string]{
//	    WorkflowName:    "ProcessDataWorkflow",
//	    QueueName:       "data-processing",
//	    WorkflowInput:   "input data",
//	    WorkflowTimeout: 30 * time.Minute,
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Check status
//	status, err := handle.GetStatus()
//	if err != nil {
//	    log.Printf("Failed to get status: %v", err)
//	}
//
//	// Wait for completion and get result
//	result, err := handle.GetResult() // blocks until completion
//	if err != nil {
//	    log.Printf("Workflow failed: %v", err)
//	} else {
//	    log.Printf("Result: %d", result)
//	}
//
//	// Enqueue with deduplication and custom workflow ID
//	handle, err := dbos.Enqueue[MyInputType, MyOutputType](ctx, dbos.GenericEnqueueOptions[MyInputType]{
//	    WorkflowName:    "MyWorkflow",
//	    QueueName:       "my-queue",
//	    WorkflowID:      "custom-workflow-id",
//	    DeduplicationID: "unique-operation-id",
//	    WorkflowInput:   MyInputType{Field: "value"},
//	})
func Enqueue[P any, R any](ctx DBOSContext, params GenericEnqueueOptions[P]) (WorkflowHandle[R], error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}

	// Register the input and outputs for gob encoding
	var typedInput P
	gob.Register(typedInput)
	var typedOutput R
	gob.Register(typedOutput)

	// Call typed erased enqueue
	handle, err := ctx.Enqueue(ctx, EnqueueOptions{
		WorkflowName:       params.WorkflowName,
		QueueName:          params.QueueName,
		WorkflowID:         params.WorkflowID,
		ApplicationVersion: params.ApplicationVersion,
		DeduplicationID:    params.DeduplicationID,
		Priority:           params.Priority,
		WorkflowInput:      params.WorkflowInput,
		WorkflowTimeout:    params.WorkflowTimeout,
	})
	if err != nil {
		return nil, err
	}

	return newWorkflowPollingHandle[R](ctx, handle.GetWorkflowID()), nil
}

// CancelWorkflow cancels a running or enqueued workflow by setting its status to CANCELLED.
// Once cancelled, the workflow will stop executing. Currently executing steps will not be interrupted.
//
// Parameters:
//   - workflowID: The unique identifier of the workflow to cancel
//
// Returns an error if the workflow does not exist or if the cancellation operation fails.
func (c *dbosContext) CancelWorkflow(workflowID string) error {
	return c.systemDB.cancelWorkflow(c, workflowID)
}

// CancelWorkflow cancels a running or enqueued workflow by setting its status to CANCELLED.
// Once cancelled, the workflow will stop executing. Currently executing steps will not be interrupted.
//
// Parameters:
//   - ctx: DBOS context for the operation
//   - workflowID: The unique identifier of the workflow to cancel
//
// Returns an error if the workflow does not exist or if the cancellation operation fails.
//
// Example:
//
//	err := dbos.CancelWorkflow(ctx, "workflow-to-cancel")
//	if err != nil {
//	    log.Printf("Failed to cancel workflow: %v", err)
//	}
func CancelWorkflow(ctx DBOSContext, workflowID string) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.CancelWorkflow(workflowID)
}

func (c *dbosContext) ResumeWorkflow(_ DBOSContext, workflowID string) (WorkflowHandle[any], error) {
	err := c.systemDB.resumeWorkflow(c, workflowID)
	if err != nil {
		return nil, err
	}
	return newWorkflowPollingHandle[any](c, workflowID), nil
}

// ResumeWorkflow resumes a cancelled workflow by setting its status back to ENQUEUED.
// The workflow will be picked up by a DBOS queue processor and execution will continue
// from where it left off. If the workflow is already completed, this is a no-op.
// Returns a handle that can be used to wait for completion and retrieve results.
// Returns an error if the workflow does not exist or if the cancellation operation fails.
//
// Example:
//
//	handle, err := dbos.ResumeWorkflow[int](ctx, "workflow-id")
//	if err != nil {
//	    log.Printf("Failed to resume workflow: %v", err)
//	} else {
//	    result, err := handle.GetResult() // blocks until completion
//	    if err != nil {
//	        log.Printf("Workflow failed: %v", err)
//	    } else {
//	        log.Printf("Result: %d", result)
//	    }
//	}
func ResumeWorkflow[R any](ctx DBOSContext, workflowID string) (WorkflowHandle[R], error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}

	// Register the output for gob encoding
	var r R
	gob.Register(r)

	_, err := ctx.ResumeWorkflow(ctx, workflowID)
	if err != nil {
		return nil, err
	}
	return newWorkflowPollingHandle[R](ctx, workflowID), nil
}

// ForkWorkflowInput holds configuration parameters for forking workflows.
type ForkWorkflowInput struct {
	OriginalWorkflowID string // Required: The UUID of the original workflow to fork from
	ForkedWorkflowID   string // Optional: Custom workflow ID for the forked workflow (auto-generated if empty)
	StartStep          uint   // Optional: Step to start the forked workflow from (default: 0)
	ApplicationVersion string // Optional: Application version for the forked workflow (inherits from original if empty)
}

func (c *dbosContext) ForkWorkflow(_ DBOSContext, input ForkWorkflowInput) (WorkflowHandle[any], error) {
	if input.OriginalWorkflowID == "" {
		return nil, errors.New("original workflow ID cannot be empty")
	}

	// Generate new workflow ID if not provided
	forkedWorkflowID := input.ForkedWorkflowID
	if forkedWorkflowID == "" {
		forkedWorkflowID = uuid.New().String()
	}

	// Create input for system database
	if input.StartStep > uint(math.MaxInt) {
		return nil, fmt.Errorf("start step too large: %d", input.StartStep)
	}
	dbInput := forkWorkflowDBInput{
		originalWorkflowID: input.OriginalWorkflowID,
		forkedWorkflowID:   forkedWorkflowID,
		startStep:          int(input.StartStep),
		applicationVersion: input.ApplicationVersion,
	}

	// Call system database method
	err := c.systemDB.forkWorkflow(c, dbInput)
	if err != nil {
		return nil, err
	}

	return newWorkflowPollingHandle[any](c, forkedWorkflowID), nil
}

// ForkWorkflow creates a new workflow instance by copying an existing workflow from a specific step.
// The forked workflow will have a new UUID and will execute from the specified StartStep.
// If StartStep > 0, the forked workflow will reuse the operation outputs from steps 0 to StartStep-1
// copied from the original workflow.
//
// Parameters:
//   - ctx: DBOS context for the operation
//   - input: Configuration parameters for the forked workflow
//
// The input struct contains:
//   - OriginalWorkflowID: The UUID of the original workflow to fork from (required)
//   - ForkedWorkflowID: Custom workflow ID for the forked workflow (optional, auto-generated if empty)
//   - StartStep: Step to start the forked workflow from (optional, default: 0)
//   - ApplicationVersion: Application version for the forked workflow (optional, inherits from original if empty)
//
// Returns a typed workflow handle for the newly created forked workflow.
//
// Example usage:
//
//	// Basic fork from step 5
//	handle, err := dbos.ForkWorkflow[MyResultType](ctx, dbos.ForkWorkflowInput{
//	    OriginalWorkflowID: "original-workflow-id",
//	    StartStep:          5,
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Fork with custom workflow ID and application version
//	handle, err := dbos.ForkWorkflow[MyResultType](ctx, dbos.ForkWorkflowInput{
//	    OriginalWorkflowID: "original-workflow-id",
//	    ForkedWorkflowID:   "my-custom-fork-id",
//	    StartStep:          3,
//	    ApplicationVersion: "v2.0.0",
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
func ForkWorkflow[R any](ctx DBOSContext, input ForkWorkflowInput) (WorkflowHandle[R], error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}

	// Register the output for gob encoding
	var r R
	gob.Register(r)

	handle, err := ctx.ForkWorkflow(ctx, input)
	if err != nil {
		return nil, err
	}
	return newWorkflowPollingHandle[R](ctx, handle.GetWorkflowID()), nil
}

// listWorkflowsParams holds configuration parameters for listing workflows
type listWorkflowsParams struct {
	workflowIDs      []string
	status           []WorkflowStatusType
	startTime        time.Time
	endTime          time.Time
	name             string
	appVersion       string
	user             string
	limit            *int
	offset           *int
	sortDesc         bool
	workflowIDPrefix string
	loadInput        bool
	loadOutput       bool
	queueName        string
}

// ListWorkflowsOption is a functional option for configuring workflow listing parameters.
type ListWorkflowsOption func(*listWorkflowsParams)

// WithWorkflowIDs filters workflows by the specified workflow IDs.
//
// Example:
//
//	workflows, err := dbos.ListWorkflows(ctx,
//	    dbos.WithWorkflowIDs([]string{"workflow1", "workflow2"}))
func WithWorkflowIDs(workflowIDs []string) ListWorkflowsOption {
	return func(p *listWorkflowsParams) {
		p.workflowIDs = workflowIDs
	}
}

// WithStatus filters workflows by the specified status(es).
// Can accept a single status or a list of statuses.
//
// Example:
//
//	workflows, err := dbos.ListWorkflows(ctx,
//	    dbos.WithStatus([]dbos.WorkflowStatusType{dbos.WorkflowStatusSuccess, dbos.WorkflowStatusError}))
func WithStatus(status []WorkflowStatusType) ListWorkflowsOption {
	return func(p *listWorkflowsParams) {
		p.status = status
	}
}

// WithStartTime filters workflows created after the specified time.
//
// Example:
//
//	workflows, err := dbos.ListWorkflows(ctx,
//	    dbos.WithStartTime(time.Now().Add(-24*time.Hour)))
func WithStartTime(startTime time.Time) ListWorkflowsOption {
	return func(p *listWorkflowsParams) {
		p.startTime = startTime
	}
}

// WithEndTime filters workflows created before the specified time.
//
// Example:
//
//	workflows, err := dbos.ListWorkflows(ctx,
//	    dbos.WithEndTime(time.Now()))
func WithEndTime(endTime time.Time) ListWorkflowsOption {
	return func(p *listWorkflowsParams) {
		p.endTime = endTime
	}
}

// WithName filters workflows by the specified workflow function name.
//
// Example:
//
//	workflows, err := dbos.ListWorkflows(ctx,
//	    dbos.WithName("MyWorkflowFunction"))
func WithName(name string) ListWorkflowsOption {
	return func(p *listWorkflowsParams) {
		p.name = name
	}
}

// WithAppVersion filters workflows by the specified application version.
//
// Example:
//
//	workflows, err := dbos.ListWorkflows(ctx,
//	    dbos.WithAppVersion("v1.0.0"))
func WithAppVersion(appVersion string) ListWorkflowsOption {
	return func(p *listWorkflowsParams) {
		p.appVersion = appVersion
	}
}

// WithUser filters workflows by the specified authenticated user.
//
// Example:
//
//	workflows, err := dbos.ListWorkflows(ctx,
//	    dbos.WithUser("john.doe"))
func WithUser(user string) ListWorkflowsOption {
	return func(p *listWorkflowsParams) {
		p.user = user
	}
}

// WithLimit limits the number of workflows returned.
//
// Example:
//
//	workflows, err := dbos.ListWorkflows(ctx,
//	    dbos.WithLimit(100))
func WithLimit(limit int) ListWorkflowsOption {
	return func(p *listWorkflowsParams) {
		p.limit = &limit
	}
}

// WithOffset sets the offset for pagination.
//
// Example:
//
//	workflows, err := dbos.ListWorkflows(ctx,
//	    dbos.WithOffset(50), dbos.WithLimit(25))
func WithOffset(offset int) ListWorkflowsOption {
	return func(p *listWorkflowsParams) {
		p.offset = &offset
	}
}

// WithSortDesc enables descending sort by creation time (default is ascending).
//
// Example:
//
//	workflows, err := dbos.ListWorkflows(ctx,
//	    dbos.WithSortDesc(true))
func WithSortDesc(sortDesc bool) ListWorkflowsOption {
	return func(p *listWorkflowsParams) {
		p.sortDesc = sortDesc
	}
}

// WithWorkflowIDPrefix filters workflows by workflow ID prefix.
//
// Example:
//
//	workflows, err := dbos.ListWorkflows(ctx,
//	    dbos.WithWorkflowIDPrefix("batch-"))
func WithWorkflowIDPrefix(prefix string) ListWorkflowsOption {
	return func(p *listWorkflowsParams) {
		p.workflowIDPrefix = prefix
	}
}

// WithLoadInput controls whether to load workflow input data (default: true).
//
// Example:
//
//	workflows, err := dbos.ListWorkflows(ctx,
//	    dbos.WithLoadInput(false))
func WithLoadInput(loadInput bool) ListWorkflowsOption {
	return func(p *listWorkflowsParams) {
		p.loadInput = loadInput
	}
}

// WithLoadOutput controls whether to load workflow output data (default: true).
//
// Example:
//
//	workflows, err := dbos.ListWorkflows(ctx,
//	    dbos.WithLoadOutput(false))
func WithLoadOutput(loadOutput bool) ListWorkflowsOption {
	return func(p *listWorkflowsParams) {
		p.loadOutput = loadOutput
	}
}

// WithQueueName filters workflows by the specified queue name.
// This is typically used when listing queued workflows.
//
// Example:
//
//	workflows, err := dbos.ListWorkflows(ctx,
//	    dbos.WithQueueName("data-processing"))
func WithQueueName(queueName string) ListWorkflowsOption {
	return func(p *listWorkflowsParams) {
		p.queueName = queueName
	}
}

// ListWorkflows retrieves a list of workflows based on the provided filters.
//
// The function supports filtering by workflow IDs, status, time ranges, names, application versions,
// authenticated users, workflow ID prefixes, and more. It also supports pagination through
// limit/offset parameters and sorting control (ascending by default, or descending with WithSortDesc).
//
// By default, both input and output data are loaded for each workflow. This can be controlled
// using WithLoadInput(false) and WithLoadOutput(false) options for better performance when
// the data is not needed.
//
// Parameters:
//   - opts: Functional options to configure the query filters and parameters
//
// Returns a slice of WorkflowStatus structs containing the workflow information.
//
// Example usage:
//
//	// List all successful workflows from the last 24 hours
//	workflows, err := ctx.ListWorkflows(
//	    dbos.WithStatus([]dbos.WorkflowStatusType{dbos.WorkflowStatusSuccess}),
//	    dbos.WithStartTime(time.Now().Add(-24*time.Hour)),
//	    dbos.WithLimit(100))
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// List workflows by specific IDs without loading input/output data
//	workflows, err := ctx.ListWorkflows(
//	    dbos.WithWorkflowIDs([]string{"workflow1", "workflow2"}),
//	    dbos.WithLoadInput(false),
//	    dbos.WithLoadOutput(false))
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// List workflows with pagination
//	workflows, err := ctx.ListWorkflows(
//	    dbos.WithUser("john.doe"),
//	    dbos.WithOffset(50),
//	    dbos.WithLimit(25),
//	    dbos.WithSortDesc(true))
//	if err != nil {
//	    log.Fatal(err)
//	}
func (c *dbosContext) ListWorkflows(opts ...ListWorkflowsOption) ([]WorkflowStatus, error) {

	// Initialize parameters with defaults
	params := &listWorkflowsParams{
		loadInput:  true, // Default to loading input
		loadOutput: true, // Default to loading output
	}

	// Apply all provided options
	for _, opt := range opts {
		opt(params)
	}

	// Convert to system database input structure
	dbInput := listWorkflowsDBInput{
		workflowIDs:        params.workflowIDs,
		status:             params.status,
		startTime:          params.startTime,
		endTime:            params.endTime,
		workflowName:       params.name,
		applicationVersion: params.appVersion,
		authenticatedUser:  params.user,
		limit:              params.limit,
		offset:             params.offset,
		sortDesc:           params.sortDesc,
		workflowIDPrefix:   params.workflowIDPrefix,
		loadInput:          params.loadInput,
		loadOutput:         params.loadOutput,
		queueName:          params.queueName,
	}

	// Call the context method to list workflows
	workflows, err := c.systemDB.listWorkflows(c, dbInput)
	if err != nil {
		return nil, fmt.Errorf("failed to list workflows: %w", err)
	}

	return workflows, nil
}

// ListWorkflows retrieves a list of workflows based on the provided filters.
//
// The function supports filtering by workflow IDs, status, time ranges, names, application versions,
// authenticated users, workflow ID prefixes, and more. It also supports pagination through
// limit/offset parameters and sorting control (ascending by default, or descending with WithSortDesc).
//
// By default, both input and output data are loaded for each workflow. This can be controlled
// using WithLoadInput(false) and WithLoadOutput(false) options for better performance when
// the data is not needed.
//
// Parameters:
//   - ctx: DBOS context for the operation
//   - opts: Functional options to configure the query filters and parameters
//
// Returns a slice of WorkflowStatus structs containing the workflow information.
//
// Example usage:
//
//	// List all successful workflows from the last 24 hours
//	workflows, err := dbos.ListWorkflows(ctx,
//	    dbos.WithStatus([]dbos.WorkflowStatusType{dbos.WorkflowStatusSuccess}),
//	    dbos.WithStartTime(time.Now().Add(-24*time.Hour)),
//	    dbos.WithLimit(100))
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// List workflows by specific IDs without loading input/output data
//	workflows, err := dbos.ListWorkflows(ctx,
//	    dbos.WithWorkflowIDs([]string{"workflow1", "workflow2"}),
//	    dbos.WithLoadInput(false),
//	    dbos.WithLoadOutput(false))
//	if err != nil {
//	    log.Fatal(err)
//	}
func ListWorkflows(ctx DBOSContext, opts ...ListWorkflowsOption) ([]WorkflowStatus, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.ListWorkflows(opts...)
}
