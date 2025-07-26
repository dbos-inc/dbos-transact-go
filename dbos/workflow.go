package dbos

import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"sync"
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
// TODO: this should be an internal type. Workflows should have aptly named getters to access the state
type workflowState struct {
	workflowID   string
	stepCounter  int
	isWithinStep bool
}

// NextStepID returns the next step ID and increments the counter
func (ws *workflowState) NextStepID() int {
	ws.stepCounter++
	return ws.stepCounter
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
	GetResult(ctx context.Context) (R, error)
	GetStatus() (WorkflowStatus, error)
	GetWorkflowID() string // XXX we could have a base struct with GetWorkflowID and then embed it in the implementations
}

// workflowHandle is a concrete implementation of WorkflowHandle
type workflowHandle[R any] struct {
	workflowID  string
	outcomeChan chan workflowOutcome[R]
}

// GetResult waits for the workflow to complete and returns the result
func (h *workflowHandle[R]) GetResult(ctx context.Context) (R, error) {
	outcome, ok := <-h.outcomeChan // Blocking read
	if !ok {
		// Return an error if the channel was closed. In normal operations this would happen if GetResul() is called twice on a handler. The first call should get the buffered result, the second call find zero values (channel is empty and closed).
		return *new(R), errors.New("workflow result channel is already closed. Did you call GetResult() twice on the same workflow handle?")
	}
	// If we are calling GetResult inside a workflow, record the result as a step result
	parentWorkflowState, ok := ctx.Value(workflowStateKey).(*workflowState)
	isChildWorkflow := ok && parentWorkflowState != nil
	if isChildWorkflow {
		encodedOutput, encErr := serialize(outcome.result)
		if encErr != nil {
			return *new(R), newWorkflowExecutionError(parentWorkflowState.workflowID, fmt.Sprintf("serializing child workflow result: %v", encErr))
		}
		recordGetResultInput := recordChildGetResultDBInput{
			parentWorkflowID: parentWorkflowState.workflowID,
			childWorkflowID:  h.workflowID,
			stepID:           parentWorkflowState.NextStepID(),
			output:           encodedOutput,
			err:              outcome.err,
		}
		recordResultErr := dbos.systemDB.RecordChildGetResult(ctx, recordGetResultInput)
		if recordResultErr != nil {
			// XXX do we want to fail this?
			getLogger().Error("failed to record get result", "error", recordResultErr)
		}
	}
	return outcome.result, outcome.err
}

// GetStatus returns the current status of the workflow from the database
func (h *workflowHandle[R]) GetStatus() (WorkflowStatus, error) {
	ctx := context.Background()
	workflowStatuses, err := dbos.systemDB.ListWorkflows(ctx, listWorkflowsDBInput{
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
	workflowID string
}

func (h *workflowPollingHandle[R]) GetResult(ctx context.Context) (R, error) {
	result, err := dbos.systemDB.AwaitWorkflowResult(ctx, h.workflowID)
	if result != nil {
		typedResult, ok := result.(R)
		if !ok {
			// TODO check what this looks like in practice
			return *new(R), newWorkflowUnexpectedResultType(h.workflowID, fmt.Sprintf("%T", new(R)), fmt.Sprintf("%T", result))
		}
		// If we are calling GetResult inside a workflow, record the result as a step result
		parentWorkflowState, ok := ctx.Value(workflowStateKey).(*workflowState)
		isChildWorkflow := ok && parentWorkflowState != nil
		if isChildWorkflow {
			encodedOutput, encErr := serialize(typedResult)
			if encErr != nil {
				return *new(R), newWorkflowExecutionError(parentWorkflowState.workflowID, fmt.Sprintf("serializing child workflow result: %v", encErr))
			}
			recordGetResultInput := recordChildGetResultDBInput{
				parentWorkflowID: parentWorkflowState.workflowID,
				childWorkflowID:  h.workflowID,
				stepID:           parentWorkflowState.NextStepID(),
				output:           encodedOutput,
				err:              err,
			}
			recordResultErr := dbos.systemDB.RecordChildGetResult(ctx, recordGetResultInput)
			if recordResultErr != nil {
				// XXX do we want to fail this?
				getLogger().Error("failed to record get result", "error", recordResultErr)
			}
		}
		return typedResult, err
	}
	return *new(R), err
}

// GetStatus returns the current status of the workflow from the database
func (h *workflowPollingHandle[R]) GetStatus() (WorkflowStatus, error) {
	ctx := context.Background()
	workflowStatuses, err := dbos.systemDB.ListWorkflows(ctx, listWorkflowsDBInput{
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
type typedErasedWorkflowWrapperFunc func(ctx context.Context, input any, opts ...workflowOption) (WorkflowHandle[any], error)

type workflowRegistryEntry struct {
	wrappedFunction typedErasedWorkflowWrapperFunc
	maxRetries      int
}

var registry = make(map[string]workflowRegistryEntry)
var regMutex sync.RWMutex

// Register adds a workflow function to the registry (thread-safe, only once per name)
func registerWorkflow(fqn string, fn typedErasedWorkflowWrapperFunc, maxRetries int) {
	regMutex.Lock()
	defer regMutex.Unlock()

	if _, exists := registry[fqn]; exists {
		getLogger().Error("workflow function already registered", "fqn", fqn)
		panic(newConflictingRegistrationError(fqn))
	}

	registry[fqn] = workflowRegistryEntry{
		wrappedFunction: fn,
		maxRetries:      maxRetries,
	}
}

type workflowRegistrationParams struct {
	cronSchedule string
	maxRetries   int
	// Likely we will allow a name here
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

func WithWorkflow[P any, R any](fn WorkflowFunc[P, R], opts ...workflowRegistrationOption) WorkflowWrapperFunc[P, R] {
	if dbos != nil {
		getLogger().Warn("WithWorkflow called after DBOS initialization, dynamic registration is not supported")
		return nil
	}

	registrationParams := workflowRegistrationParams{
		maxRetries: _DEFAULT_MAX_RECOVERY_ATTEMPTS,
	}
	for _, opt := range opts {
		opt(&registrationParams)
	}

	if fn == nil {
		panic("workflow function cannot be nil")
	}
	fqn := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()

	// Registry the input/output types for gob encoding
	var p P
	var r R
	gob.Register(p)
	gob.Register(r)

	// Wrap the function in a durable workflow
	wrappedFunction := WorkflowWrapperFunc[P, R](func(ctx context.Context, workflowInput P, opts ...workflowOption) (WorkflowHandle[R], error) {
		opts = append(opts, WithWorkflowMaxRetries(registrationParams.maxRetries))
		return runAsWorkflow(ctx, fn, workflowInput, opts...)
	})

	// If this is a scheduled workflow, register a cron job
	if registrationParams.cronSchedule != "" {
		if reflect.TypeOf(p) != reflect.TypeOf(time.Time{}) {
			panic(fmt.Sprintf("scheduled workflow function must accept ScheduledWorkflowInput as input, got %T", p))
		}
		if workflowScheduler == nil {
			workflowScheduler = cron.New(cron.WithSeconds())
		}
		var entryID cron.EntryID
		entryID, err := workflowScheduler.AddFunc(registrationParams.cronSchedule, func() {
			// Execute the workflow on the cron schedule once DBOS is launched
			if dbos == nil {
				return
			}
			// Get the scheduled time from the cron entry
			entry := workflowScheduler.Entry(entryID)
			scheduledTime := entry.Prev
			if scheduledTime.IsZero() {
				// Use Next if Prev is not set, which will only happen for the first run
				scheduledTime = entry.Next
			}
			wfID := fmt.Sprintf("sched-%s-%s", fqn, scheduledTime) // XXX we can rethink the format
			wrappedFunction(context.Background(), any(scheduledTime).(P), WithWorkflowID(wfID), WithQueue(_DBOS_INTERNAL_QUEUE_NAME))
		})
		if err != nil {
			panic(fmt.Sprintf("failed to register scheduled workflow: %v", err))
		}
	}

	// Register a type-erased version of the durable workflow for recovery
	typeErasedWrapper := func(ctx context.Context, input any, opts ...workflowOption) (WorkflowHandle[any], error) {
		typedInput, ok := input.(P)
		if !ok {
			return nil, newWorkflowUnexpectedInputType(fqn, fmt.Sprintf("%T", typedInput), fmt.Sprintf("%T", input))
		}

		handle, err := wrappedFunction(ctx, typedInput, opts...)
		if err != nil {
			return nil, err
		}
		return &workflowPollingHandle[any]{workflowID: handle.GetWorkflowID()}, nil
	}
	registerWorkflow(fqn, typeErasedWrapper, registrationParams.maxRetries)

	return wrappedFunction
}

/**********************************/
/******* WORKFLOW FUNCTIONS *******/
/**********************************/

type contextKey string

// TODO this should be a private type, once we have proper getter for a workflow state
const workflowStateKey contextKey = "workflowState"

type WorkflowFunc[P any, R any] func(ctx context.Context, input P) (R, error)
type WorkflowWrapperFunc[P any, R any] func(ctx context.Context, input P, opts ...workflowOption) (WorkflowHandle[R], error)

type workflowParams struct {
	workflowID         string
	timeout            time.Duration
	deadline           time.Time
	queueName          string
	applicationVersion string
	maxRetries         int
}

type workflowOption func(*workflowParams)

func WithWorkflowID(id string) workflowOption {
	return func(p *workflowParams) {
		p.workflowID = id
	}
}

func WithTimeout(timeout time.Duration) workflowOption {
	return func(p *workflowParams) {
		p.timeout = timeout
	}
}

func WithDeadline(deadline time.Time) workflowOption {
	return func(p *workflowParams) {
		p.deadline = deadline
	}
}

func WithQueue(queueName string) workflowOption {
	return func(p *workflowParams) {
		p.queueName = queueName
	}
}

func WithApplicationVersion(version string) workflowOption {
	return func(p *workflowParams) {
		p.applicationVersion = version
	}
}

func WithWorkflowMaxRetries(maxRetries int) workflowOption {
	return func(p *workflowParams) {
		p.maxRetries = maxRetries
	}
}

func runAsWorkflow[P any, R any](ctx context.Context, fn WorkflowFunc[P, R], input P, opts ...workflowOption) (WorkflowHandle[R], error) {
	// Apply options to build params
	params := workflowParams{
		applicationVersion: _APP_VERSION,
	}
	for _, opt := range opts {
		opt(&params)
	}

	// First, create a context for the workflow
	dbosWorkflowContext := context.Background()

	// Check if we are within a workflow (and thus a child workflow)
	parentWorkflowState, ok := ctx.Value(workflowStateKey).(*workflowState)
	isChildWorkflow := ok && parentWorkflowState != nil

	// TODO Check if cancelled

	// Generate an ID for the workflow if not provided
	var workflowID string
	if params.workflowID == "" {
		if isChildWorkflow {
			stepID := parentWorkflowState.NextStepID()
			workflowID = fmt.Sprintf("%s-%d", parentWorkflowState.workflowID, stepID)
		} else {
			workflowID = uuid.New().String()
		}
	} else {
		workflowID = params.workflowID
	}

	// If this is a child workflow that has already been recorded in operations_output, return directly a polling handle
	if isChildWorkflow {
		childWorkflowID, err := dbos.systemDB.CheckChildWorkflow(dbosWorkflowContext, parentWorkflowState.workflowID, parentWorkflowState.stepCounter)
		if err != nil {
			return nil, newWorkflowExecutionError(parentWorkflowState.workflowID, fmt.Sprintf("checking child workflow: %v", err))
		}
		if childWorkflowID != nil {
			return &workflowPollingHandle[R]{workflowID: *childWorkflowID}, nil
		}
	}

	var status WorkflowStatusType
	if params.queueName != "" {
		status = WorkflowStatusEnqueued
	} else {
		status = WorkflowStatusPending
	}

	workflowStatus := WorkflowStatus{
		Name:               runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name(), // TODO factor out somewhere else so we dont' have to reflect here
		ApplicationVersion: params.applicationVersion,
		ExecutorID:         _EXECUTOR_ID,
		Status:             status,
		ID:                 workflowID,
		CreatedAt:          time.Now(),
		Deadline:           params.deadline, // TODO compute the deadline based on the timeout
		Timeout:            params.timeout,
		Input:              input,
		ApplicationID:      _APP_ID,
		QueueName:          params.queueName,
	}

	// Init status and record child workflow relationship in a single transaction
	tx, err := dbos.systemDB.(*systemDatabase).pool.Begin(dbosWorkflowContext)
	if err != nil {
		return nil, newWorkflowExecutionError(workflowID, fmt.Sprintf("failed to begin transaction: %v", err))
	}
	defer tx.Rollback(dbosWorkflowContext) // Rollback if not committed

	// Insert workflow status with transaction
	insertInput := insertWorkflowStatusDBInput{
		status:     workflowStatus,
		maxRetries: params.maxRetries,
		tx:         tx,
	}
	insertStatusResult, err := dbos.systemDB.InsertWorkflowStatus(dbosWorkflowContext, insertInput)
	if err != nil {
		return nil, err
	}

	// Return a polling handle if: we are enqueueing, the workflow is already in a terminal state (success or error),
	if len(params.queueName) > 0 || insertStatusResult.status == WorkflowStatusSuccess || insertStatusResult.status == WorkflowStatusError {
		// Commit the transaction to update the number of attempts and/or enact the enqueue
		if err := tx.Commit(dbosWorkflowContext); err != nil {
			return nil, newWorkflowExecutionError(workflowID, fmt.Sprintf("failed to commit transaction: %v", err))
		}
		return &workflowPollingHandle[R]{workflowID: workflowStatus.ID}, nil
	}

	// Record child workflow relationship if this is a child workflow
	if isChildWorkflow {
		// Get the step ID that was used for generating the child workflow ID
		stepID := parentWorkflowState.stepCounter
		childInput := recordChildWorkflowDBInput{
			parentWorkflowID: parentWorkflowState.workflowID,
			childWorkflowID:  workflowStatus.ID,
			stepName:         runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name(), // Will need to test this
			stepID:           stepID,
			tx:               tx,
		}
		err = dbos.systemDB.RecordChildWorkflow(dbosWorkflowContext, childInput)
		if err != nil {
			return nil, newWorkflowExecutionError(parentWorkflowState.workflowID, fmt.Sprintf("recording child workflow: %v", err))
		}
	}

	// Commit the transaction
	if err := tx.Commit(dbosWorkflowContext); err != nil {
		return nil, newWorkflowExecutionError(workflowID, fmt.Sprintf("failed to commit transaction: %v", err))
	}

	// Channel to receive the outcome from the goroutine
	// The buffer size of 1 allows the goroutine to send the outcome without blocking
	// In addition it allows the channel to be garbage collected
	outcomeChan := make(chan workflowOutcome[R], 1)

	// Create the handle
	handle := &workflowHandle[R]{
		workflowID:  workflowStatus.ID,
		outcomeChan: outcomeChan,
	}

	// Create workflow state to track step execution
	wfState := &workflowState{
		workflowID:  workflowStatus.ID,
		stepCounter: -1,
	}

	// Run the function in a goroutine
	augmentUserContext := context.WithValue(ctx, workflowStateKey, wfState)
	go func() {
		result, err := fn(augmentUserContext, input)
		status := WorkflowStatusSuccess
		if err != nil {
			status = WorkflowStatusError
		}
		recordErr := dbos.systemDB.UpdateWorkflowOutcome(dbosWorkflowContext, updateWorkflowOutcomeDBInput{workflowID: workflowStatus.ID, status: status, err: err, output: result})
		if recordErr != nil {
			outcomeChan <- workflowOutcome[R]{result: *new(R), err: recordErr}
			close(outcomeChan) // Close the channel to signal completion
			return
		}
		outcomeChan <- workflowOutcome[R]{result: result, err: err}
		close(outcomeChan) // Close the channel to signal completion
	}()

	// Run the peer goroutine to handle cancellation and timeout
	/*
		if dbosWorkflowContext.Done() != nil {
			getLogger().Debug("starting goroutine to handle workflow context cancellation or timeout")
			go func() {
				select {
				case <-dbosWorkflowContext.Done():
					// The context was cancelled or timed out: record timeout or cancellation
					// CANCEL WORKFLOW HERE
					return
				}
			}()
		}
	*/

	return handle, nil
}

/******************************/
/******* STEP FUNCTIONS *******/
/******************************/

type StepFunc[P any, R any] func(ctx context.Context, input P) (R, error)

type StepParams struct {
	MaxRetries    int
	BackoffFactor float64
	BaseInterval  time.Duration
	MaxInterval   time.Duration
}

// stepOption is a functional option for configuring step parameters
type stepOption func(*StepParams)

// WithStepMaxRetries sets the maximum number of retries for a step
func WithStepMaxRetries(maxRetries int) stepOption {
	return func(p *StepParams) {
		p.MaxRetries = maxRetries
	}
}

// WithBackoffFactor sets the backoff factor for retries (multiplier for exponential backoff)
func WithBackoffFactor(backoffFactor float64) stepOption {
	return func(p *StepParams) {
		p.BackoffFactor = backoffFactor
	}
}

// WithBaseInterval sets the base delay for the first retry
func WithBaseInterval(baseInterval time.Duration) stepOption {
	return func(p *StepParams) {
		p.BaseInterval = baseInterval
	}
}

// WithMaxInterval sets the maximum delay for retries
func WithMaxInterval(maxInterval time.Duration) stepOption {
	return func(p *StepParams) {
		p.MaxInterval = maxInterval
	}
}

func RunAsStep[P any, R any](ctx context.Context, fn StepFunc[P, R], input P, opts ...stepOption) (R, error) {
	if fn == nil {
		return *new(R), newStepExecutionError("", "", "step function cannot be nil")
	}

	stepName := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()

	// Apply options to build params with defaults
	params := StepParams{
		MaxRetries:    0,
		BackoffFactor: 2.0,
		BaseInterval:  500 * time.Millisecond,
		MaxInterval:   1 * time.Hour,
	}
	for _, opt := range opts {
		opt(&params)
	}

	// Get workflow state from context
	wfState, ok := ctx.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return *new(R), newStepExecutionError("", stepName, "workflow state not found in context: are you running this step within a workflow?")
	}

	// If within a step, just run the function directly
	if wfState.isWithinStep {
		return fn(ctx, input)
	}

	// Get next step ID
	stepID := wfState.NextStepID()

	// Check the step is cancelled, has already completed, or is called with a different name
	recordedOutput, err := dbos.systemDB.CheckOperationExecution(ctx, checkOperationExecutionDBInput{
		workflowID: wfState.workflowID,
		stepID:     stepID,
		stepName:   runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name(),
	})
	if err != nil {
		return *new(R), newStepExecutionError(wfState.workflowID, stepName, fmt.Sprintf("checking operation execution: %v", err))
	}
	if recordedOutput != nil {
		return recordedOutput.output.(R), recordedOutput.err
	}

	// Execute step with retry logic if MaxRetries > 0
	stepState := workflowState{
		workflowID:   wfState.workflowID,
		stepCounter:  wfState.stepCounter,
		isWithinStep: true,
	}
	stepCtx := context.WithValue(ctx, workflowStateKey, &stepState)

	stepOutput, stepError := fn(stepCtx, input)

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

			getLogger().Error("step failed, retrying", "step_name", stepName, "retry", retry, "max_retries", params.MaxRetries, "delay", delay, "error", stepError)

			// Wait before retry
			select {
			case <-ctx.Done():
				return *new(R), newStepExecutionError(wfState.workflowID, stepName, fmt.Sprintf("context cancelled during retry: %v", ctx.Err()))
			case <-time.After(delay):
				// Continue to retry
			}

			// Execute the retry
			stepOutput, stepError = fn(stepCtx, input)

			// If successful, break
			if stepError == nil {
				break
			}

			// Join the error with existing errors
			joinedErrors = errors.Join(joinedErrors, stepError)

			// If max retries reached, create MaxStepRetriesExceeded error
			if retry == params.MaxRetries {
				stepError = newMaxStepRetriesExceededError(wfState.workflowID, stepName, params.MaxRetries, joinedErrors)
				break
			}
		}
	}

	// Record the final result
	dbInput := recordOperationResultDBInput{
		workflowID: wfState.workflowID,
		stepName:   stepName,
		stepID:     stepID,
		err:        stepError,
		output:     stepOutput,
	}
	recErr := dbos.systemDB.RecordOperationResult(ctx, dbInput)
	if recErr != nil {
		return *new(R), newStepExecutionError(wfState.workflowID, stepName, fmt.Sprintf("recording step outcome: %v", recErr))
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

// Send sends a message to another workflow.
// Send automatically registers the type of R for gob encoding
func Send[R any](ctx context.Context, input WorkflowSendInput[R]) error {
	var typedMessage R
	gob.Register(typedMessage)
	return dbos.systemDB.Send(ctx, workflowSendInputInternal{
		destinationID: input.DestinationID,
		message:       input.Message,
		topic:         input.Topic,
	})
}

type WorkflowRecvInput struct {
	Topic   string
	Timeout time.Duration
}

func Recv[R any](ctx context.Context, input WorkflowRecvInput) (R, error) {
	msg, err := dbos.systemDB.Recv(ctx, input)
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

type WorkflowSetEventInput[R any] struct {
	Key     string
	Message R
}

// Sets an event from a workflow.
// The event is a key value pair
// SetEvent automatically registers the type of R for gob encoding
func SetEvent[R any](ctx context.Context, input WorkflowSetEventInput[R]) error {
	var typedMessage R
	gob.Register(typedMessage)
	return dbos.systemDB.SetEvent(ctx, workflowSetEventInputInternal{
		key:     input.Key,
		message: input.Message,
	})
}

type WorkflowGetEventInput struct {
	TargetWorkflowID string
	Key              string
	Timeout          time.Duration
}

func GetEvent[R any](ctx context.Context, input WorkflowGetEventInput) (R, error) {
	value, err := dbos.systemDB.GetEvent(ctx, input)
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

func Sleep(ctx context.Context, duration time.Duration) (time.Duration, error) {
	return dbos.systemDB.Sleep(ctx, duration)
}

/***********************************/
/******* WORKFLOW MANAGEMENT *******/
/***********************************/

// GetWorkflowID retrieves the workflow ID from the context if called within a DBOS workflow
func GetWorkflowID(ctx context.Context) (string, error) {
	wfState, ok := ctx.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return "", errors.New("not within a DBOS workflow context")
	}
	return wfState.workflowID, nil
}

func RetrieveWorkflow[R any](workflowID string) (workflowPollingHandle[R], error) {
	ctx := context.Background()
	workflowStatus, err := dbos.systemDB.ListWorkflows(ctx, listWorkflowsDBInput{
		workflowIDs: []string{workflowID},
	})
	if err != nil {
		return workflowPollingHandle[R]{}, fmt.Errorf("failed to retrieve workflow status: %w", err)
	}
	if len(workflowStatus) == 0 {
		return workflowPollingHandle[R]{}, newNonExistentWorkflowError(workflowID)
	}
	return workflowPollingHandle[R]{workflowID: workflowID}, nil
}
