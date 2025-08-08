package dbos

import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"log/slog"
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

type WorkflowStatusType string

const (
	WorkflowStatusPending         WorkflowStatusType = "PENDING"
	WorkflowStatusEnqueued        WorkflowStatusType = "ENQUEUED"
	WorkflowStatusSuccess         WorkflowStatusType = "SUCCESS"
	WorkflowStatusError           WorkflowStatusType = "ERROR"
	WorkflowStatusCancelled       WorkflowStatusType = "CANCELLED"
	WorkflowStatusRetriesExceeded WorkflowStatusType = "RETRIES_EXCEEDED"
)

type WorkflowStatus struct {
	ID                 string             `json:"workflow_uuid"`
	Status             WorkflowStatusType `json:"status"`
	Name               string             `json:"name"`
	AuthenticatedUser  *string            `json:"authenticated_user"`
	AssumedRole        *string            `json:"assumed_role"`
	AuthenticatedRoles *string            `json:"authenticated_roles"`
	Output             any                `json:"output"`
	Error              error              `json:"error"`
	ExecutorID         string             `json:"executor_id"`
	CreatedAt          time.Time          `json:"created_at"`
	UpdatedAt          time.Time          `json:"updated_at"`
	ApplicationVersion string             `json:"application_version"`
	ApplicationID      string             `json:"application_id"`
	Attempts           int                `json:"attempts"`
	QueueName          string             `json:"queue_name"`
	Timeout            time.Duration      `json:"timeout"`
	Deadline           time.Time          `json:"deadline"`
	StartedAt          time.Time          `json:"started_at"`
	DeduplicationID    *string            `json:"deduplication_id"`
	Input              any                `json:"input"`
	Priority           int                `json:"priority"`
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

type WorkflowHandle[R any] interface {
	GetResult() (R, error)
	GetStatus() (WorkflowStatus, error)
	GetWorkflowID() string
}

// workflowHandle is a concrete implementation of WorkflowHandle
type workflowHandle[R any] struct {
	workflowID  string
	outcomeChan chan workflowOutcome[R]
	dbosContext DBOSContext
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
		recordResultErr := h.dbosContext.(*dbosContext).systemDB.RecordChildGetResult(h.dbosContext, recordGetResultInput)
		if recordResultErr != nil {
			h.dbosContext.(*dbosContext).logger.Error("failed to record get result", "error", recordResultErr)
			return *new(R), newWorkflowExecutionError(workflowState.workflowID, fmt.Sprintf("recording child workflow result: %v", recordResultErr))
		}
	}
	return outcome.result, outcome.err
}

// GetStatus returns the current status of the workflow from the database
func (h *workflowHandle[R]) GetStatus() (WorkflowStatus, error) {
	workflowStatuses, err := h.dbosContext.(*dbosContext).systemDB.ListWorkflows(h.dbosContext, listWorkflowsDBInput{
		workflowIDs: []string{h.workflowID},
	})
	if err != nil {
		return WorkflowStatus{}, fmt.Errorf("failed to get workflow status: %w", err)
	}
	if len(workflowStatuses) == 0 {
		return WorkflowStatus{}, newNonExistentWorkflowError(h.workflowID)
	}
	return workflowStatuses[0], nil
}

func (h *workflowHandle[R]) GetWorkflowID() string {
	return h.workflowID
}

type workflowPollingHandle[R any] struct {
	workflowID  string
	dbosContext DBOSContext
}

func (h *workflowPollingHandle[R]) GetResult() (R, error) {
	result, err := h.dbosContext.(*dbosContext).systemDB.AwaitWorkflowResult(h.dbosContext, h.workflowID)
	if result != nil {
		typedResult, ok := result.(R)
		if !ok {
			// TODO check what this looks like in practice
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
			recordResultErr := h.dbosContext.(*dbosContext).systemDB.RecordChildGetResult(h.dbosContext, recordGetResultInput)
			if recordResultErr != nil {
				// XXX do we want to fail this?
				h.dbosContext.(*dbosContext).logger.Error("failed to record get result", "error", recordResultErr)
			}
		}
		return typedResult, err
	}
	return *new(R), err
}

// GetStatus returns the current status of the workflow from the database
func (h *workflowPollingHandle[R]) GetStatus() (WorkflowStatus, error) {
	workflowStatuses, err := h.dbosContext.(*dbosContext).systemDB.ListWorkflows(h.dbosContext, listWorkflowsDBInput{
		workflowIDs: []string{h.workflowID},
	})
	if err != nil {
		return WorkflowStatus{}, fmt.Errorf("failed to get workflow status: %w", err)
	}
	if len(workflowStatuses) == 0 {
		return WorkflowStatus{}, newNonExistentWorkflowError(h.workflowID)
	}
	return workflowStatuses[0], nil
}

func (h *workflowPollingHandle[R]) GetWorkflowID() string {
	return h.workflowID
}

/**********************************/
/******* WORKFLOW REGISTRY *******/
/**********************************/
type GenericWrappedWorkflowFunc[P any, R any] func(ctx DBOSContext, input P, opts ...WorkflowOption) (WorkflowHandle[R], error)
type WrappedWorkflowFunc func(ctx DBOSContext, input any, opts ...WorkflowOption) (WorkflowHandle[any], error)

type workflowRegistryEntry struct {
	wrappedFunction WrappedWorkflowFunc
	maxRetries      int
}

// Register adds a workflow function to the registry (thread-safe, only once per name)
func registerWorkflow(ctx DBOSContext, workflowName string, fn WrappedWorkflowFunc, maxRetries int) {
	// Skip if we don't have a concrete dbosContext
	c, ok := ctx.(*dbosContext)
	if !ok {
		return
	}

	if c.launched.Load() {
		panic("Cannot register workflow after DBOS has launched")
	}

	c.workflowRegMutex.Lock()
	defer c.workflowRegMutex.Unlock()

	if _, exists := c.workflowRegistry[workflowName]; exists {
		c.logger.Error("workflow function already registered", "fqn", workflowName)
		panic(newConflictingRegistrationError(workflowName))
	}

	c.workflowRegistry[workflowName] = workflowRegistryEntry{
		wrappedFunction: fn,
		maxRetries:      maxRetries,
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
		wfID := fmt.Sprintf("sched-%s-%s", workflowName, scheduledTime) // XXX we can rethink the format
		opts := []WorkflowOption{
			WithWorkflowID(wfID),
			WithQueue(_DBOS_INTERNAL_QUEUE_NAME),
			withWorkflowName(workflowName),
		}
		ctx.RunAsWorkflow(ctx, fn, scheduledTime, opts...)
	})
	if err != nil {
		panic(fmt.Sprintf("failed to register scheduled workflow: %v", err))
	}
	c.logger.Info("Registered scheduled workflow", "fqn", workflowName, "cron_schedule", cronSchedule)
}

type workflowRegistrationParams struct {
	cronSchedule string
	maxRetries   int
}

type workflowRegistrationOption func(*workflowRegistrationParams)

const (
	_DEFAULT_MAX_RECOVERY_ATTEMPTS = 100
)

func WithMaxRetries(maxRetries int) workflowRegistrationOption {
	return func(p *workflowRegistrationParams) {
		p.maxRetries = maxRetries
	}
}

func WithSchedule(schedule string) workflowRegistrationOption {
	return func(p *workflowRegistrationParams) {
		p.cronSchedule = schedule
	}
}

// RegisterWorkflow registers the provided function as a durable workflow with the provided DBOSContext workflow registry
// If the workflow is a scheduled workflow (determined by the presence of a cron schedule), it will also register a cron job to execute it
// RegisterWorkflow is generically typed, providing compile-time type checking and allowing us to register the workflow input and output types for gob encoding
// The registered workflow is wrapped in a typed-erased wrapper which performs runtime type checks and conversions
// To execute the workflow, use DBOSContext.RunAsWorkflow
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
		// This type check is redundant with the one in the wrapper, but I'd better be safe than sorry
		typedInput, ok := input.(P)
		if !ok {
			return nil, newWorkflowUnexpectedInputType(fqn, fmt.Sprintf("%T", typedInput), fmt.Sprintf("%T", input))
		}
		return fn(ctx, typedInput)
	})

	typeErasedWrapper := WrappedWorkflowFunc(func(ctx DBOSContext, input any, opts ...WorkflowOption) (WorkflowHandle[any], error) {
		typedInput, ok := input.(P)
		if !ok {
			return nil, newWorkflowUnexpectedInputType(fqn, fmt.Sprintf("%T", typedInput), fmt.Sprintf("%T", input))
		}

		opts = append(opts, withWorkflowName(fqn)) // Append the name so ctx.RunAsWorkflow can look it up from the registry to apply registration-time options
		handle, err := ctx.RunAsWorkflow(ctx, typedErasedWorkflow, typedInput, opts...)
		if err != nil {
			return nil, err
		}
		return &workflowPollingHandle[any]{workflowID: handle.GetWorkflowID(), dbosContext: ctx}, nil // this is only used by recovery and queue runner so far -- queue runner dismisses it
	})
	registerWorkflow(ctx, fqn, typeErasedWrapper, registrationParams.maxRetries)

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

type GenericWorkflowFunc[P any, R any] func(ctx DBOSContext, input P) (R, error)
type WorkflowFunc func(ctx DBOSContext, input any) (any, error)

type workflowParams struct {
	workflowName       string
	workflowID         string
	queueName          string
	applicationVersion string
	maxRetries         int
}

type WorkflowOption func(*workflowParams)

func WithWorkflowID(id string) WorkflowOption {
	return func(p *workflowParams) {
		p.workflowID = id
	}
}

func WithQueue(queueName string) WorkflowOption {
	return func(p *workflowParams) {
		p.queueName = queueName
	}
}

func WithApplicationVersion(version string) WorkflowOption {
	return func(p *workflowParams) {
		p.applicationVersion = version
	}
}

func withWorkflowName(name string) WorkflowOption {
	return func(p *workflowParams) {
		p.workflowName = name
	}
}

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
		typedPollingHandle := &workflowPollingHandle[R]{
			workflowID:  pollingHandle.workflowID,
			dbosContext: pollingHandle.dbosContext,
		}
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

		typedHandle := &workflowHandle[R]{
			workflowID:  handle.workflowID,
			outcomeChan: typedOutcomeChan,
			dbosContext: handle.dbosContext,
		}

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
	registeredWorkflow, exists := c.workflowRegistry[params.workflowName]
	if !exists {
		return nil, newNonExistentWorkflowError(params.workflowName)
	}
	if registeredWorkflow.maxRetries > 0 {
		params.maxRetries = registeredWorkflow.maxRetries
	}

	// Check if we are within a workflow (and thus a child workflow)
	parentWorkflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isChildWorkflow := ok && parentWorkflowState != nil

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
		childWorkflowID, err := c.systemDB.CheckChildWorkflow(uncancellableCtx, parentWorkflowState.workflowID, parentWorkflowState.stepID)
		if err != nil {
			return nil, newWorkflowExecutionError(parentWorkflowState.workflowID, fmt.Sprintf("checking child workflow: %v", err))
		}
		if childWorkflowID != nil {
			return &workflowPollingHandle[any]{workflowID: *childWorkflowID, dbosContext: uncancellableCtx}, nil
		}
	}

	var status WorkflowStatusType
	if params.queueName != "" {
		status = WorkflowStatusEnqueued
	} else {
		status = WorkflowStatusPending
	}

	// Check if the user-provided context has a deadline
	deadline, ok := c.Deadline()
	if !ok {
		deadline = time.Time{} // No deadline set
	}

	// Compute the timeout based on the deadline
	var timeout time.Duration
	if !deadline.IsZero() {
		timeout = time.Until(deadline)
		/* unclear to me if this is a real use case:
		if timeout < 0 {
			return nil, newWorkflowExecutionError(workflowID, "deadline is in the past")
		}
		*/
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
	}

	// Init status and record child workflow relationship in a single transaction
	tx, err := c.systemDB.(*systemDatabase).pool.Begin(uncancellableCtx)
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
	insertStatusResult, err := c.systemDB.InsertWorkflowStatus(uncancellableCtx, insertInput)
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
		return &workflowPollingHandle[any]{workflowID: workflowStatus.ID, dbosContext: uncancellableCtx}, nil
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
		err = c.systemDB.RecordChildWorkflow(uncancellableCtx, childInput)
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

	// If the workflow has a durable deadline, set it in the context.
	var stopFunc func() bool
	cancelFuncCompleted := make(chan struct{})
	if !insertStatusResult.workflowDeadline.IsZero() {
		workflowCtx, _ = WithTimeout(workflowCtx, time.Until(insertStatusResult.workflowDeadline))
		// Register a cancel function that cancels the workflow in the DB as soon as the context is cancelled
		dbosCancelFunction := func() {
			c.logger.Info("Cancelling workflow", "workflow_id", workflowID)
			err = c.systemDB.CancelWorkflow(uncancellableCtx, workflowID)
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

		recordErr := c.systemDB.UpdateWorkflowOutcome(uncancellableCtx, updateWorkflowOutcomeDBInput{
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

	return &workflowHandle[any]{workflowID: workflowID, outcomeChan: outcomeChan, dbosContext: uncancellableCtx}, nil
}

/******************************/
/******* STEP FUNCTIONS *******/
/******************************/

// safeExecute wraps an operation that might panic and converts panics to errors
func safeExecute(operation func() (any, error), workflowID, stepName string, logger *slog.Logger) (result any, err error) {
	defer func() {
		if r := recover(); r != nil {
			result = nil
			err = newStepExecutionError(workflowID, stepName, fmt.Sprintf("panic recovered: %v", r))
			// Log the panic if we have a logger
			if logger != nil {
				logger.Error("panic recovered in step execution", "workflowID", workflowID, "stepName", stepName, "panic", r)
			}
		}
	}()
	return operation()
}

const StepParamsKey DBOSContextKey = "stepParams"

type StepParams struct {
	MaxRetries    int
	BackoffFactor float64
	BaseInterval  time.Duration
	MaxInterval   time.Duration
	StepName      string
}

// setStepParamDefaults returns a StepParams struct with all defaults properly set
func setStepParamDefaults(params *StepParams, stepName string) *StepParams {
	if params == nil {
		return &StepParams{
			MaxRetries:    0, // Default to no retries
			BackoffFactor: 2.0,
			BaseInterval:  100 * time.Millisecond, // Default base interval
			MaxInterval:   5 * time.Second,        // Default max interval
			StepName:      typeErasedStepNameToStepName[stepName],
		}
	}

	// Set defaults for zero values
	if params.BackoffFactor == 0 {
		params.BackoffFactor = 2.0 // Default backoff factor
	}
	if params.BaseInterval == 0 {
		params.BaseInterval = 100 * time.Millisecond // Default base interval
	}
	if params.MaxInterval == 0 {
		params.MaxInterval = 5 * time.Second // Default max interval
	}
	if params.StepName == "" {
		// If the step name is not provided, use the function name
		params.StepName = typeErasedStepNameToStepName[stepName]
	}

	return params
}

var typeErasedStepNameToStepName = make(map[string]string)

func RunAsStep[R any](ctx DBOSContext, fn any, input ...any) (R, error) {

	if ctx == nil {
		return *new(R), newStepExecutionError("", "", "ctx cannot be nil")
	}

	if fn == nil {
		return *new(R), newStepExecutionError("", "", "step function cannot be nil")
	}

	wfState, ok := ctx.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return *new(R), newStepExecutionError("", "", "workflow state not found in context: are you running this step within a workflow?")
	}
	workflowID := wfState.workflowID

	// Resolve step name in safeExecute
	stepNameResult, err := safeExecute(func() (any, error) {
		return runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name(), nil
	}, workflowID, "", nil)

	if err != nil {
		return *new(R), err
	}

	stepName, ok := stepNameResult.(string)
	if !ok { // This should never happen, but we check for safety
		return *new(R), newStepExecutionError(workflowID, "", fmt.Sprintf("unexpected step function type: expected string, got %T", stepNameResult))
	}

	// Type-erase the function based on its actual type
	typeErasedFn := func(ctx context.Context) (any, error) {
		return safeExecute(func() (any, error) {
			// Prepare the arguments
			fnType := reflect.TypeOf(fn)
			typedInput := make([]reflect.Value, fnType.NumIn())
			typedInput[0] = reflect.ValueOf(ctx) // First argument is always context.Context
			for i := 1; i < fnType.NumIn(); i++ {
				if i-1 < len(input) {
					typedInput[i] = reflect.ValueOf(input[i-1])
				}
			}

			fnValue := reflect.ValueOf(fn)
			results := fnValue.Call(typedInput)

			// Convert the results to the expected types (R and error)
			var result any
			if results[0].IsValid() && results[0].CanInterface() {
				// If the result is a pointer or interface, check if it's nil before casting
				if results[0].Kind() == reflect.Ptr || results[0].Kind() == reflect.Interface {
					if !results[0].IsNil() {
						result = results[0].Interface().(R) // Cast the result to the expected type R
					}
				} else {
					result = results[0].Interface().(R) // Cast the result to the expected type R
				}
			}
			var err error
			if results[1].IsValid() && results[1].CanInterface() {
				// Handle the case were the return value is an interface or pointer that implements the error interface
				if results[1].Kind() == reflect.Ptr || results[1].Kind() == reflect.Interface {
					if !results[1].IsNil() {
						err = results[1].Interface().(error)
					}
				} else {
					err = results[1].Interface().(error)
				}
			}

			return result, err
		}, workflowID, stepName, nil)
	}

	// Map type-erased function name to original step name
	typeErasedStepNameToStepName[runtime.FuncForPC(reflect.ValueOf(typeErasedFn).Pointer()).Name()] = stepName

	// Call the executor method
	result, err := ctx.RunAsStep(ctx, typeErasedFn)

	// Type-check and cast the result with panic recovery
	// The result should already be of type R (casted in the inner function), but ctx.RunAsStep returns an `any` type, so we need to cast it back to R
	// err is the step error, if any, and already of type error
	typedResult, castErr := safeExecute(func() (any, error) {
		if casted, ok := result.(R); ok {
			return casted, nil
		}
		return nil, fmt.Errorf("unexpected result type: expected %T, got %T", *new(R), result)
	}, workflowID, stepName, nil)
	if castErr != nil {
		return *new(R), castErr
	}

	return typedResult.(R), err
}

func (c *dbosContext) RunAsStep(_ DBOSContext, fn func(ctx context.Context) (any, error)) (any, error) {

	// Get workflow state from context
	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		// TODO: try to print step name
		return nil, newStepExecutionError("", "", "workflow state not found in context: are you running this step within a workflow?")
	}

	if fn == nil {
		// TODO: try to print step name
		return nil, newStepExecutionError(wfState.workflowID, "", "step function cannot be nil")
	}

	// Look up for step parameters in the context and set defaults
	params, ok := c.Value(StepParamsKey).(*StepParams)
	if !ok {
		params = nil
	}
	params = setStepParamDefaults(params, runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name())

	// If within a step, just run the function directly with panic recovery
	if wfState.isWithinStep {
		return safeExecute(func() (any, error) { return fn(c) }, wfState.workflowID, params.StepName, c.logger)
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
	recordedOutput, err := c.systemDB.CheckOperationExecution(uncancellableCtx, checkOperationExecutionDBInput{
		workflowID: stepState.workflowID,
		stepID:     stepState.stepID,
		stepName:   params.StepName,
	})
	if err != nil {
		return nil, newStepExecutionError(stepState.workflowID, params.StepName, fmt.Sprintf("checking operation execution: %v", err))
	}
	if recordedOutput != nil {
		return recordedOutput.output, recordedOutput.err
	}

	// Spawn a child DBOSContext with the step state
	stepCtx := WithValue(c, workflowStateKey, &stepState)

	stepOutput, stepError := safeExecute(func() (any, error) { return fn(stepCtx) }, stepState.workflowID, params.StepName, c.logger)

	// Retry if MaxRetries > 0 and the first execution failed
	var joinedErrors error
	if stepError != nil && params.MaxRetries > 0 {
		joinedErrors = errors.Join(joinedErrors, stepError)

		for retry := 1; retry <= params.MaxRetries; retry++ {
			// Calculate delay for exponential backoff
			delay := params.BaseInterval
			if retry > 1 {
				exponentialDelay := float64(params.BaseInterval) * math.Pow(params.BackoffFactor, float64(retry-1))
				delay = time.Duration(math.Min(exponentialDelay, float64(params.MaxInterval)))
			}

			c.logger.Error("step failed, retrying", "step_name", params.StepName, "retry", retry, "max_retries", params.MaxRetries, "delay", delay, "error", stepError)

			// Wait before retry
			select {
			case <-c.Done():
				return nil, newStepExecutionError(stepState.workflowID, params.StepName, fmt.Sprintf("context cancelled during retry: %v", c.Err()))
			case <-time.After(delay):
				// Continue to retry
			}

			// Execute the retry with panic recovery
			stepOutput, stepError = safeExecute(func() (any, error) { return fn(stepCtx) }, stepState.workflowID, params.StepName, c.logger)

			// If successful, break
			if stepError == nil {
				break
			}

			// Join the error with existing errors
			joinedErrors = errors.Join(joinedErrors, stepError)

			// If max retries reached, create MaxStepRetriesExceeded error
			if retry == params.MaxRetries {
				stepError = newMaxStepRetriesExceededError(stepState.workflowID, params.StepName, params.MaxRetries, joinedErrors)
				break
			}
		}
	}

	// Record the final result
	dbInput := recordOperationResultDBInput{
		workflowID: stepState.workflowID,
		stepName:   params.StepName,
		stepID:     stepState.stepID,
		err:        stepError,
		output:     stepOutput,
	}
	recErr := c.systemDB.RecordOperationResult(uncancellableCtx, dbInput)
	if recErr != nil {
		return nil, newStepExecutionError(stepState.workflowID, params.StepName, fmt.Sprintf("recording step outcome: %v", recErr))
	}

	return stepOutput, stepError
}

/****************************************/
/******* WORKFLOW COMMUNICATIONS ********/
/****************************************/

type WorkflowSendInput[R any] struct {
	DestinationID string
	Message       R
	Topic         string
}

func (c *dbosContext) Send(_ DBOSContext, input WorkflowSendInputInternal) error {
	return c.systemDB.Send(c, input)
}

// Send sends a message to another workflow.
// Send automatically registers the type of R for gob encoding
func Send[R any](ctx DBOSContext, input WorkflowSendInput[R]) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	var typedMessage R
	gob.Register(typedMessage)
	return ctx.Send(ctx, WorkflowSendInputInternal{
		DestinationID: input.DestinationID,
		Message:       input.Message,
		Topic:         input.Topic,
	})
}

type WorkflowRecvInput struct {
	Topic   string
	Timeout time.Duration
}

func (c *dbosContext) Recv(_ DBOSContext, input WorkflowRecvInput) (any, error) {
	return c.systemDB.Recv(c, input)
}

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

type WorkflowSetEventInputGeneric[R any] struct {
	Key     string
	Message R
}

func (c *dbosContext) SetEvent(_ DBOSContext, input WorkflowSetEventInput) error {
	return c.systemDB.SetEvent(c, input)
}

// Sets an event from a workflow.
// The event is a key value pair
// SetEvent automatically registers the type of R for gob encoding
func SetEvent[R any](ctx DBOSContext, input WorkflowSetEventInputGeneric[R]) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	var typedMessage R
	gob.Register(typedMessage)
	return ctx.SetEvent(ctx, WorkflowSetEventInput{
		Key:     input.Key,
		Message: input.Message,
	})
}

type WorkflowGetEventInput struct {
	TargetWorkflowID string
	Key              string
	Timeout          time.Duration
}

func (c *dbosContext) GetEvent(_ DBOSContext, input WorkflowGetEventInput) (any, error) {
	return c.systemDB.GetEvent(c, input)
}

func GetEvent[R any](ctx DBOSContext, input WorkflowGetEventInput) (R, error) {
	if ctx == nil {
		return *new(R), errors.New("dbosCtx cannot be nil")
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
	return c.systemDB.Sleep(c, duration)
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

func (c *dbosContext) RetrieveWorkflow(_ DBOSContext, workflowID string) (WorkflowHandle[any], error) {
	workflowStatus, err := c.systemDB.ListWorkflows(c, listWorkflowsDBInput{
		workflowIDs: []string{workflowID},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve workflow status: %w", err)
	}
	if len(workflowStatus) == 0 {
		return nil, newNonExistentWorkflowError(workflowID)
	}
	return &workflowPollingHandle[any]{workflowID: workflowID, dbosContext: c}, nil
}

func RetrieveWorkflow[R any](ctx DBOSContext, workflowID string) (workflowPollingHandle[R], error) {
	if ctx == nil {
		return workflowPollingHandle[R]{}, errors.New("dbosCtx cannot be nil")
	}
	workflowStatus, err := ctx.(*dbosContext).systemDB.ListWorkflows(ctx, listWorkflowsDBInput{
		workflowIDs: []string{workflowID},
	})
	if err != nil {
		return workflowPollingHandle[R]{}, fmt.Errorf("failed to retrieve workflow status: %w", err)
	}
	if len(workflowStatus) == 0 {
		return workflowPollingHandle[R]{}, newNonExistentWorkflowError(workflowID)
	}
	return workflowPollingHandle[R]{workflowID: workflowID, dbosContext: ctx}, nil
}
