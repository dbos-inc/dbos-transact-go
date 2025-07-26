package dbos

/**
Test workflow and steps features
[x] Wrapping various golang methods in DBOS workflows
[x] workflow idempotency
[x] workflow DLQ
[] workflow conflicting name
[] workflow timeout
[] workflow deadlines
*/

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Global counter for idempotency testing
var idempotencyCounter int64

var (
	simpleWf              = WithWorkflow(simpleWorkflow)
	simpleWfError         = WithWorkflow(simpleWorkflowError)
	simpleWfWithStep      = WithWorkflow(simpleWorkflowWithStep)
	simpleWfWithStepError = WithWorkflow(simpleWorkflowWithStepError)
	// struct methods
	s              = workflowStruct{}
	simpleWfStruct = WithWorkflow(s.simpleWorkflow)
	simpleWfValue  = WithWorkflow(s.simpleWorkflowValue)
	// interface method workflow
	workflowIface TestWorkflowInterface = &workflowImplementation{
		field: "example",
	}
	simpleWfIface = WithWorkflow(workflowIface.Execute)
	// Generic workflow
	wfInt = WithWorkflow(Identity[string]) // FIXME make this an int eventually
	// Closure with captured state
	prefix  = "hello-"
	wfClose = WithWorkflow(func(ctx context.Context, in string) (string, error) {
		return prefix + in, nil
	})
)

func simpleWorkflow(ctxt context.Context, input string) (string, error) {
	return input, nil
}

func simpleWorkflowError(ctx context.Context, input string) (int, error) {
	return 0, fmt.Errorf("failure")
}

func simpleWorkflowWithStep(ctx context.Context, input string) (string, error) {
	return RunAsStep(ctx, simpleStep, input)
}

func simpleStep(ctx context.Context, input string) (string, error) {
	return "from step", nil
}

func simpleStepError(ctx context.Context, input string) (string, error) {
	return "", fmt.Errorf("step failure")
}

func simpleWorkflowWithStepError(ctx context.Context, input string) (string, error) {
	return RunAsStep(ctx, simpleStepError, input)
}

// idempotencyWorkflow increments a global counter and returns the input
func incrementCounter(_ context.Context, value int64) (int64, error) {
	idempotencyCounter += value
	return idempotencyCounter, nil
}

// Unified struct that demonstrates both pointer and value receiver methods
type workflowStruct struct{}

// Pointer receiver method
func (w *workflowStruct) simpleWorkflow(ctx context.Context, input string) (string, error) {
	return simpleWorkflow(ctx, input)
}

// Value receiver method on the same struct
func (w workflowStruct) simpleWorkflowValue(ctx context.Context, input string) (string, error) {
	return input + "-value", nil
}

// interface for workflow methods
type TestWorkflowInterface interface {
	Execute(ctx context.Context, input string) (string, error)
}

type workflowImplementation struct {
	field string
}

func (w *workflowImplementation) Execute(ctx context.Context, input string) (string, error) {
	return input + "-" + w.field + "-interface", nil
}

// Generic workflow function
func Identity[T any](ctx context.Context, in T) (T, error) {
	return in, nil
}

var (
	anonymousWf = WithWorkflow(func(ctx context.Context, in string) (string, error) {
		return "anonymous-" + in, nil
	})
)

func TestWorkflowsWrapping(t *testing.T) {
	setupDBOS(t)

	type testCase struct {
		name           string
		workflowFunc   func(context.Context, string, ...workflowOption) (any, error)
		input          string
		expectedResult any
		expectError    bool
		expectedError  string
	}

	tests := []testCase{
		{
			name: "SimpleWorkflow",
			workflowFunc: func(ctx context.Context, input string, opts ...workflowOption) (any, error) {
				handle, err := simpleWf(ctx, input, opts...)
				if err != nil {
					return nil, err
				}
				result, err := handle.GetResult(ctx)
				_, err2 := handle.GetResult(ctx)
				if err2 == nil {
					t.Fatal("Second call to GetResult should return an error")
				}
				expectedErrorMsg := "workflow result channel is already closed. Did you call GetResult() twice on the same workflow handle?"
				if err2.Error() != expectedErrorMsg {
					t.Fatal("Unexpected error message:", err2, "expected:", expectedErrorMsg)
				}
				return result, err
			},
			input:          "echo",
			expectedResult: "echo",
			expectError:    false,
		},
		{
			name: "SimpleWorkflowError",
			workflowFunc: func(ctx context.Context, input string, opts ...workflowOption) (any, error) {
				handle, err := simpleWfError(ctx, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult(ctx)
			},
			input:         "echo",
			expectError:   true,
			expectedError: "failure",
		},
		{
			name: "SimpleWorkflowWithStep",
			workflowFunc: func(ctx context.Context, input string, opts ...workflowOption) (any, error) {
				handle, err := simpleWfWithStep(ctx, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult(ctx)
			},
			input:          "echo",
			expectedResult: "from step",
			expectError:    false,
		},
		{
			name: "SimpleWorkflowStruct",
			workflowFunc: func(ctx context.Context, input string, opts ...workflowOption) (any, error) {
				handle, err := simpleWfStruct(ctx, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult(ctx)
			},
			input:          "echo",
			expectedResult: "echo",
			expectError:    false,
		},
		{
			name: "ValueReceiverWorkflow",
			workflowFunc: func(ctx context.Context, input string, opts ...workflowOption) (any, error) {
				handle, err := simpleWfValue(ctx, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult(ctx)
			},
			input:          "echo",
			expectedResult: "echo-value",
			expectError:    false,
		},
		{
			name: "interfaceMethodWorkflow",
			workflowFunc: func(ctx context.Context, input string, opts ...workflowOption) (any, error) {
				handle, err := simpleWfIface(ctx, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult(ctx)
			},
			input:          "echo",
			expectedResult: "echo-example-interface",
			expectError:    false,
		},
		{
			name: "GenericWorkflow",
			workflowFunc: func(ctx context.Context, input string, opts ...workflowOption) (any, error) {
				// For generic workflow, we need to convert string to int for testing
				handle, err := wfInt(ctx, "42", opts...) // FIXME for now this returns a string because sys db accepts this
				if err != nil {
					return nil, err
				}
				return handle.GetResult(ctx)
			},
			input:          "42", // input not used in this case
			expectedResult: "42", // FIXME make this an int eventually
			expectError:    false,
		},
		{
			name: "ClosureWithCapturedState",
			workflowFunc: func(ctx context.Context, input string, opts ...workflowOption) (any, error) {
				handle, err := wfClose(ctx, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult(ctx)
			},
			input:          "world",
			expectedResult: "hello-world",
			expectError:    false,
		},
		{
			name: "AnonymousClosure",
			workflowFunc: func(ctx context.Context, input string, opts ...workflowOption) (any, error) {
				handle, err := anonymousWf(ctx, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult(ctx)
			},
			input:          "test",
			expectedResult: "anonymous-test",
			expectError:    false,
		},
		{
			name: "SimpleWorkflowWithStepError",
			workflowFunc: func(ctx context.Context, input string, opts ...workflowOption) (any, error) {
				handle, err := simpleWfWithStepError(ctx, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult(ctx)
			},
			input:         "echo",
			expectError:   true,
			expectedError: "step failure",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := tc.workflowFunc(context.Background(), tc.input, WithWorkflowID(uuid.NewString()))

			if tc.expectError {
				if err == nil {
					t.Fatal("expected error but got none")
				}
				if tc.expectedError != "" && err.Error() != tc.expectedError {
					t.Fatalf("expected error %q but got %q", tc.expectedError, err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error but got: %v", err)
				}
				if result != tc.expectedResult {
					t.Fatalf("expected result %v but got %v", tc.expectedResult, result)
				}
			}
		})
	}
}

func stepWithinAStep(ctx context.Context, input string) (string, error) {
	return RunAsStep(ctx, simpleStep, input)
}

func stepWithinAStepWorkflow(ctx context.Context, input string) (string, error) {
	return RunAsStep(ctx, stepWithinAStep, input)
}

// Global counter for retry testing
var stepRetryAttemptCount int

func stepRetryAlwaysFailsStep(ctx context.Context, input string) (string, error) {
	stepRetryAttemptCount++
	return "", fmt.Errorf("always fails - attempt %d", stepRetryAttemptCount)
}

var stepIdempotencyCounter int

func stepIdempotencyTest(ctx context.Context, input string) (string, error) {
	stepIdempotencyCounter++
	return input, nil
}

func stepRetryWorkflow(ctx context.Context, input string) (string, error) {
	RunAsStep(ctx, stepIdempotencyTest, input)
	return RunAsStep(ctx, stepRetryAlwaysFailsStep, input,
		WithStepMaxRetries(5),
		WithBackoffFactor(2.0),
		WithBaseInterval(1*time.Millisecond),
		WithMaxInterval(10*time.Millisecond))
}

var (
	stepWithinAStepWf = WithWorkflow(stepWithinAStepWorkflow)
	stepRetryWf       = WithWorkflow(stepRetryWorkflow)
)

func TestSteps(t *testing.T) {
	setupDBOS(t)

	t.Run("StepsMustRunInsideWorkflows", func(t *testing.T) {
		ctx := context.Background()

		// Attempt to run a step outside of a workflow context
		_, err := RunAsStep(ctx, simpleStep, "test")
		if err == nil {
			t.Fatal("expected error when running step outside of workflow context, but got none")
		}

		// Check the error type
		dbosErr, ok := err.(*DBOSError)
		if !ok {
			t.Fatalf("expected error to be of type *DBOSError, got %T", err)
		}

		if dbosErr.Code != StepExecutionError {
			t.Fatalf("expected error code to be StepExecutionError, got %v", dbosErr.Code)
		}

		// Test the specific message from the 3rd argument
		expectedMessagePart := "workflow state not found in context: are you running this step within a workflow?"
		if !strings.Contains(err.Error(), expectedMessagePart) {
			t.Fatalf("expected error message to contain %q, but got %q", expectedMessagePart, err.Error())
		}
	})

	t.Run("StepWithinAStepAreJustFunctions", func(t *testing.T) {
		handle, err := stepWithinAStepWf(context.Background(), "test")
		if err != nil {
			t.Fatal("failed to run step within a step:", err)
		}
		result, err := handle.GetResult(context.Background())
		if err != nil {
			t.Fatal("failed to get result from step within a step:", err)
		}
		if result != "from step" {
			t.Fatalf("expected result 'from step', got '%s'", result)
		}

		steps, err := dbos.systemDB.GetWorkflowSteps(context.Background(), handle.GetWorkflowID())
		if err != nil {
			t.Fatal("failed to list steps:", err)
		}
		if len(steps) != 1 {
			t.Fatalf("expected 1 step, got %d", len(steps))
		}
	})

	t.Run("StepRetryWithExponentialBackoff", func(t *testing.T) {
		// Reset the global counters before test
		stepRetryAttemptCount = 0
		stepIdempotencyCounter = 0

		// Execute the workflow
		handle, err := stepRetryWf(context.Background(), "test")
		if err != nil {
			t.Fatal("failed to start retry workflow:", err)
		}

		_, err = handle.GetResult(context.Background())
		if err == nil {
			t.Fatal("expected error from failing workflow but got none")
		}

		// Verify the step was called exactly 6 times (max attempts + 1 initial attempt)
		if stepRetryAttemptCount != 6 {
			t.Fatalf("expected 6 attempts, got %d", stepRetryAttemptCount)
		}

		// Verify the error is a MaxStepRetriesExceeded error
		dbosErr, ok := err.(*DBOSError)
		if !ok {
			t.Fatalf("expected error to be of type *DBOSError, got %T", err)
		}

		if dbosErr.Code != MaxStepRetriesExceeded {
			t.Fatalf("expected error code to be MaxStepRetriesExceeded, got %v", dbosErr.Code)
		}

		// Verify the error contains the step name and max retries
		expectedErrorMessage := "dbos.stepRetryAlwaysFailsStep has exceeded its maximum of 5 retries"
		if !strings.Contains(dbosErr.Message, expectedErrorMessage) {
			t.Fatalf("expected error message to contain '%s', got '%s'", expectedErrorMessage, dbosErr.Message)
		}

		// Verify each error message is present in the joined error
		for i := 1; i <= 5; i++ {
			expectedMsg := fmt.Sprintf("always fails - attempt %d", i)
			if !strings.Contains(dbosErr.Error(), expectedMsg) {
				t.Fatalf("expected joined error to contain '%s', but got '%s'", expectedMsg, dbosErr.Error())
			}
		}

		// Verify that the failed step was still recorded in the database
		steps, err := dbos.systemDB.GetWorkflowSteps(context.Background(), handle.GetWorkflowID())
		if err != nil {
			t.Fatal("failed to get workflow steps:", err)
		}

		if len(steps) != 2 {
			t.Fatalf("expected 2 recorded step, got %d", len(steps))
		}

		// Verify the second step has the error
		step := steps[1]
		if step.Error == nil {
			t.Fatal("expected error in recorded step, got none")
		}

		if step.Error.Error() != dbosErr.Error() {
			t.Fatalf("expected recorded step error to match joined error, got '%s', expected '%s'", step.Error.Error(), dbosErr.Error())
		}

		// Verify the idempotency step was executed only once
		if stepIdempotencyCounter != 1 {
			t.Fatalf("expected idempotency step to be executed only once, got %d", stepIdempotencyCounter)
		}
	})
}

var (
	childWf = WithWorkflow(func(ctx context.Context, i int) (string, error) {
		workflowID, err := GetWorkflowID(ctx)
		if err != nil {
			return "", fmt.Errorf("failed to get workflow ID: %v", err)
		}
		expectedCurrentID := fmt.Sprintf("%s-%d", workflowID, i)
		if workflowID != expectedCurrentID {
			return "", fmt.Errorf("expected parentWf workflow ID to be %s, got %s", expectedCurrentID, workflowID)
		}
		// XXX right now the steps of a child workflow start with an incremented step ID, because the first step ID is allocated to the child workflow
		return RunAsStep(ctx, simpleStep, "")
	})
	parentWf = WithWorkflow(func(ctx context.Context, i int) (string, error) {
		workflowID, err := GetWorkflowID(ctx)
		if err != nil {
			return "", fmt.Errorf("failed to get workflow ID: %v", err)
		}

		childHandle, err := childWf(ctx, i)
		if err != nil {
			return "", err
		}

		// Check this wf ID is built correctly
		expectedParentID := fmt.Sprintf("%s-%d", workflowID, i)
		if workflowID != expectedParentID {
			return "", fmt.Errorf("expected parentWf workflow ID to be %s, got %s", expectedParentID, workflowID)
		}

		// Verify child workflow ID follows the pattern: parentID-functionID
		childWorkflowID := childHandle.GetWorkflowID()
		expectedChildID := fmt.Sprintf("%s-%d", workflowID, i)
		if childWorkflowID != expectedChildID {
			return "", fmt.Errorf("expected childWf ID to be %s, got %s", expectedChildID, childWorkflowID)
		}
		return childHandle.GetResult(ctx)
	})
	grandParentWf = WithWorkflow(func(ctx context.Context, _ string) (string, error) {
		for i := range 3 {
			workflowID, err := GetWorkflowID(ctx)
			if err != nil {
				return "", fmt.Errorf("failed to get workflow ID: %v", err)
			}

			childHandle, err := parentWf(ctx, i)
			if err != nil {
				return "", err
			}

			// The handle should a direct handle
			_, ok := childHandle.(*workflowHandle[string])
			if !ok {
				return "", fmt.Errorf("expected childHandle to be of type *workflowHandle[string], got %T", childHandle)
			}

			// Verify child workflow ID follows the pattern: parentID-functionID
			childWorkflowID := childHandle.GetWorkflowID()
			expectedPrefix := fmt.Sprintf("%s-%d", workflowID, i)
			if childWorkflowID != expectedPrefix {
				return "", fmt.Errorf("expected parentWf workflow ID to be %s, got %s", expectedPrefix, childWorkflowID)
			}

			// Calling the child a second time should return a polling handle
			childHandle, err = parentWf(ctx, i, WithWorkflowID(childHandle.GetWorkflowID()))
			if err != nil {
				return "", err
			}
			_, ok = childHandle.(*workflowPollingHandle[string])
			if !ok {
				return "", fmt.Errorf("expected childHandle to be of type *workflowPollingHandle[string], got %T", childHandle)
			}

		}

		return "", nil
	})
)

// TODO Check timeouts behaviors for parents and children (e.g. awaited cancelled, etc)
func TestChildWorkflow(t *testing.T) {
	setupDBOS(t)

	t.Run("ChildWorkflowIDPattern", func(t *testing.T) {
		h, err := grandParentWf(context.Background(), "")
		if err != nil {
			t.Fatalf("failed to execute grand parent workflow: %v", err)
		}
		_, err = h.GetResult(context.Background())
		if err != nil {
			t.Fatalf("failed to get result from grand parent workflow: %v", err)
		}
	})
}

var (
	idempotencyWf         = WithWorkflow(idempotencyWorkflow)
	idempotencyWfWithStep = WithWorkflow(idempotencyWorkflowWithStep)
)

func idempotencyWorkflow(ctx context.Context, input string) (string, error) {
	incrementCounter(ctx, 1)
	return input, nil
}

var blockingStepStopEvent *Event

func blockingStep(ctx context.Context, input string) (string, error) {
	blockingStepStopEvent.Wait()
	return "", nil
}

var idempotencyWorkflowWithStepEvent *Event

func idempotencyWorkflowWithStep(ctx context.Context, input string) (int64, error) {
	RunAsStep(ctx, incrementCounter, 1)
	idempotencyWorkflowWithStepEvent.Set()
	RunAsStep(ctx, blockingStep, input)
	return idempotencyCounter, nil
}

func TestWorkflowIdempotency(t *testing.T) {
	setupDBOS(t)

	t.Run("WorkflowExecutedOnlyOnce", func(t *testing.T) {
		idempotencyCounter = 0

		workflowID := uuid.NewString()
		input := "idempotency-test"

		// Execute the same workflow twice with the same ID
		// First execution
		handle1, err := idempotencyWf(context.Background(), input, WithWorkflowID(workflowID))
		if err != nil {
			t.Fatalf("failed to execute workflow first time: %v", err)
		}
		result1, err := handle1.GetResult(context.Background())
		if err != nil {
			t.Fatalf("failed to get result from first execution: %v", err)
		}

		// Second execution with the same workflow ID
		handle2, err := idempotencyWf(context.Background(), input, WithWorkflowID(workflowID))
		if err != nil {
			t.Fatalf("failed to execute workflow second time: %v", err)
		}
		result2, err := handle2.GetResult(context.Background())
		if err != nil {
			t.Fatalf("failed to get result from second execution: %v", err)
		}

		// Verify the second handle is a polling handle
		_, ok := handle2.(*workflowPollingHandle[string])
		if !ok {
			t.Fatalf("expected handle2 to be of type workflowPollingHandle, got %T", handle2)
		}

		// Verify both executions return the same result
		if result1 != result2 {
			t.Fatalf("expected same result from both executions, got %v and %v", result1, result2)
		}

		// Verify the counter was only incremented once (idempotency)
		if idempotencyCounter != 1 {
			t.Fatalf("expected counter to be 1 (workflow executed only once), but got %d", idempotencyCounter)
		}
	})
}

func TestWorkflowRecovery(t *testing.T) {
	setupDBOS(t)
	t.Run("RecoveryResumeWhereItLeftOff", func(t *testing.T) {
		// Reset the global counter
		idempotencyCounter = 0

		// First execution - run the workflow once
		input := "recovery-test"
		idempotencyWorkflowWithStepEvent = NewEvent()
		blockingStepStopEvent = NewEvent()
		handle1, err := idempotencyWfWithStep(context.Background(), input)
		if err != nil {
			t.Fatalf("failed to execute workflow first time: %v", err)
		}

		idempotencyWorkflowWithStepEvent.Wait() // Wait for the first step to complete. The second spins forever.

		// Run recovery for pending workflows with "local" executor
		recoveredHandles, err := recoverPendingWorkflows(context.Background(), []string{"local"})
		if err != nil {
			t.Fatalf("failed to recover pending workflows: %v", err)
		}

		// Check that we have a single handle in the return list
		if len(recoveredHandles) != 1 {
			t.Fatalf("expected 1 recovered handle, got %d", len(recoveredHandles))
		}

		// Check that the workflow ID from the handle is the same as the first handle
		recoveredHandle := recoveredHandles[0]
		_, ok := recoveredHandle.(*workflowPollingHandle[any])
		if !ok {
			t.Fatalf("expected handle to be of type workflowPollingHandle, got %T", recoveredHandle)
		}
		if recoveredHandle.GetWorkflowID() != handle1.GetWorkflowID() {
			t.Fatalf("expected recovered workflow ID %s, got %s", handle1.GetWorkflowID(), recoveredHandle.GetWorkflowID())
		}

		idempotencyWorkflowWithStepEvent.Clear()
		idempotencyWorkflowWithStepEvent.Wait()

		// Check that the first step was *not* re-executed (idempotency counter is still 1)
		if idempotencyCounter != 1 {
			t.Fatalf("expected counter to remain 1 after recovery (idempotent), but got %d", idempotencyCounter)
		}

		// Using ListWorkflows, retrieve the status of the workflow
		workflows, err := dbos.systemDB.ListWorkflows(context.Background(), listWorkflowsDBInput{
			workflowIDs: []string{handle1.GetWorkflowID()},
		})
		if err != nil {
			t.Fatalf("failed to list workflows: %v", err)
		}

		if len(workflows) != 1 {
			t.Fatalf("expected 1 workflow, got %d", len(workflows))
		}

		workflow := workflows[0]

		// Ensure its number of attempts is 2
		if workflow.Attempts != 2 {
			t.Fatalf("expected workflow attempts to be 2, got %d", workflow.Attempts)
		}

		// unlock the workflow & wait for result
		blockingStepStopEvent.Set() // This will allow the blocking step to complete
		result, err := recoveredHandle.GetResult(context.Background())
		if err != nil {
			t.Fatalf("failed to get result from recovered handle: %v", err)
		}
		if result != idempotencyCounter {
			t.Fatalf("expected result to be %s, got %s", input, result)
		}
	})
}

var (
	maxRecoveryAttempts       = 20
	deadLetterQueueWf         = WithWorkflow(deadLetterQueueWorkflow, WithMaxRetries(maxRecoveryAttempts))
	infiniteDeadLetterQueueWf = WithWorkflow(infiniteDeadLetterQueueWorkflow, WithMaxRetries(-1)) // A negative value means infinite retries
	deadLetterQueueStartEvent *Event
	deadLetterQueueEvent      *Event
	recoveryCount             int64
)

func deadLetterQueueWorkflow(ctx context.Context, input string) (int, error) {
	recoveryCount++
	fmt.Printf("Dead letter queue workflow started, recovery count: %d\n", recoveryCount)
	deadLetterQueueStartEvent.Set()
	deadLetterQueueEvent.Wait()
	return 0, nil
}

func infiniteDeadLetterQueueWorkflow(ctx context.Context, input string) (int, error) {
	deadLetterQueueStartEvent.Set()
	deadLetterQueueEvent.Wait()
	return 0, nil
}
func TestWorkflowDeadLetterQueue(t *testing.T) {
	setupDBOS(t)

	t.Run("DeadLetterQueueBehavior", func(t *testing.T) {
		deadLetterQueueEvent = NewEvent()
		deadLetterQueueStartEvent = NewEvent()
		recoveryCount = 0

		// Start a workflow that blocks forever
		wfID := uuid.NewString()
		handle, err := deadLetterQueueWf(context.Background(), "test", WithWorkflowID(wfID))
		if err != nil {
			t.Fatalf("failed to start dead letter queue workflow: %v", err)
		}
		deadLetterQueueStartEvent.Wait()
		deadLetterQueueStartEvent.Clear()

		// Attempt to recover the blocked workflow the maximum number of times
		for i := range maxRecoveryAttempts {
			_, err := recoverPendingWorkflows(context.Background(), []string{"local"})
			if err != nil {
				t.Fatalf("failed to recover pending workflows on attempt %d: %v", i+1, err)
			}
			deadLetterQueueStartEvent.Wait()
			deadLetterQueueStartEvent.Clear()
			expectedCount := int64(i + 2) // +1 for initial execution, +1 for each recovery
			if recoveryCount != expectedCount {
				t.Fatalf("expected recovery count to be %d, got %d", expectedCount, recoveryCount)
			}
		}

		// Verify an additional attempt throws a DLQ error and puts the workflow in the DLQ status
		_, err = recoverPendingWorkflows(context.Background(), []string{"local"})
		if err == nil {
			t.Fatal("expected dead letter queue error but got none")
		}

		dbosErr, ok := err.(*DBOSError)
		if !ok {
			t.Fatalf("expected DBOSError, got %T", err)
		}
		if dbosErr.Code != DeadLetterQueueError {
			t.Fatalf("expected DeadLetterQueueError, got %v", dbosErr.Code)
		}

		// Verify workflow status is RETRIES_EXCEEDED
		status, err := handle.GetStatus()
		if err != nil {
			t.Fatalf("failed to get workflow status: %v", err)
		}
		if status.Status != WorkflowStatusRetriesExceeded {
			t.Fatalf("expected workflow status to be RETRIES_EXCEEDED, got %v", status.Status)
		}

		// Verify that attempting to start a workflow with the same ID throws a DLQ error
		_, err = deadLetterQueueWf(context.Background(), "test", WithWorkflowID(wfID))
		if err == nil {
			t.Fatal("expected dead letter queue error when restarting workflow with same ID but got none")
		}

		dbosErr, ok = err.(*DBOSError)
		if !ok {
			t.Fatalf("expected DBOSError, got %T", err)
		}
		if dbosErr.Code != DeadLetterQueueError {
			t.Fatalf("expected DeadLetterQueueError, got %v", dbosErr.Code)
		}

		/*
				// TODO: test resume when implemented
				resumedHandle, err := ...

				// Recover pending workflows again - should work without error
				_, err = recoverPendingWorkflows(context.Background(), []string{"local"})
				if err != nil {
					t.Fatalf("failed to recover pending workflows after resume: %v", err)
				}

				// Complete the blocked workflow
				deadLetterQueueEvent.Set()

				// Wait for both handles to complete
				result1, err = handle.GetResult(context.Background())
				if err != nil {
					t.Fatalf("failed to get result from original handle: %v", err)
				}

				result2, err := resumedHandle.GetResult(context.Background())
				if err != nil {
					t.Fatalf("failed to get result from resumed handle: %v", err)
				}

				if result1 != result2 {
					t.Fatalf("expected both handles to return same result, got %v and %v", result1, result2)
				}

				// Verify workflow status is SUCCESS
				status, err = handle.GetStatus()
				if err != nil {
					t.Fatalf("failed to get final workflow status: %v", err)
				}
				if status.Status != WorkflowStatusSuccess {
					t.Fatalf("expected workflow status to be SUCCESS, got %v", status.Status)
				}

			// Verify that retries of a completed workflow do not raise the DLQ exception
			for i := 0; i < maxRecoveryAttempts*2; i++ {
				_, err = deadLetterQueueWf(context.Background(), "test", WithWorkflowID(wfID))
				if err != nil {
					t.Fatalf("unexpected error when retrying completed workflow: %v", err)
				}
			}
		*/
	})

	t.Run("InfiniteRetriesWorkflow", func(t *testing.T) {
		deadLetterQueueEvent = NewEvent()
		deadLetterQueueStartEvent = NewEvent()

		// Verify that a workflow with MaxRetries=0 (infinite retries) is retried infinitely
		wfID := uuid.NewString()

		handle, err := infiniteDeadLetterQueueWf(context.Background(), "test", WithWorkflowID(wfID))
		if err != nil {
			t.Fatalf("failed to start infinite dead letter queue workflow: %v", err)
		}

		deadLetterQueueStartEvent.Wait()
		deadLetterQueueStartEvent.Clear()
		// Attempt to recover the blocked workflow many times (should never fail)
		handles := []WorkflowHandle[any]{}
		for i := range _DEFAULT_MAX_RECOVERY_ATTEMPTS * 2 {
			recoveredHandles, err := recoverPendingWorkflows(context.Background(), []string{"local"})
			if err != nil {
				t.Fatalf("failed to recover pending workflows on attempt %d: %v", i+1, err)
			}
			handles = append(handles, recoveredHandles...)
			deadLetterQueueStartEvent.Wait()
			deadLetterQueueStartEvent.Clear()
		}

		// Complete the workflow
		deadLetterQueueEvent.Set()

		result, err := handle.GetResult(context.Background())
		if err != nil {
			t.Fatalf("failed to get result from infinite dead letter queue workflow: %v", err)
		}
		if result != 0 {
			t.Fatalf("expected result to be 0, got %v", result)
		}

		// Wait for all handles to complete
		for i, h := range handles {
			result, err := h.GetResult(context.Background())
			if err != nil {
				t.Fatalf("failed to get result from handle %d: %v", i, err)
			}
			if result != 0 {
				t.Fatalf("expected 0 result, got %v", result)
			}
		}
	})
}

var (
	counter    = 0
	counter1Ch = make(chan time.Time, 100)
	_          = WithWorkflow(func(ctx context.Context, scheduledTime time.Time) (string, error) {
		startTime := time.Now()
		counter++
		if counter == 10 {
			return "", fmt.Errorf("counter reached 100, stopping workflow")
		}
		select {
		case counter1Ch <- startTime:
		default:
		}
		return fmt.Sprintf("Scheduled workflow scheduled at time %v and executed at time %v", scheduledTime, startTime), nil
	}, WithSchedule("* * * * * *")) // Every second
)

func TestScheduledWorkflows(t *testing.T) {
	setupDBOS(t)

	// Helper function to collect execution times
	collectExecutionTimes := func(ch chan time.Time, target int, timeout time.Duration) ([]time.Time, error) {
		var executionTimes []time.Time
		for len(executionTimes) < target {
			select {
			case execTime := <-ch:
				executionTimes = append(executionTimes, execTime)
			case <-time.After(timeout):
				return nil, fmt.Errorf("timeout waiting for %d executions, got %d", target, len(executionTimes))
			}
		}
		return executionTimes, nil
	}

	t.Run("ScheduledWorkflowExecution", func(t *testing.T) {
		// Wait for workflow to execute at least 10 times (should take ~9-10 seconds)
		executionTimes, err := collectExecutionTimes(counter1Ch, 10, 10*time.Second)
		if err != nil {
			t.Fatalf("Failed to collect scheduled workflow execution times: %v", err)
		}
		if len(executionTimes) < 10 {
			t.Fatalf("Expected at least 10 executions, got %d", len(executionTimes))
		}

		// Verify timing - each execution should be approximately 1 second apart
		scheduleInterval := 1 * time.Second
		allowedSlack := 2 * time.Second

		for i, execTime := range executionTimes {
			// Calculate expected execution time based on schedule interval
			expectedTime := executionTimes[0].Add(time.Duration(i+1) * scheduleInterval)

			// Calculate the delta between actual and expected execution time
			delta := execTime.Sub(expectedTime)
			if delta < 0 {
				delta = -delta // Get absolute value
			}

			// Check if delta is within acceptable slack
			if delta > allowedSlack {
				t.Fatalf("Execution %d timing deviation too large: expected around %v, got %v (delta: %v, allowed slack: %v)",
					i+1, expectedTime, execTime, delta, allowedSlack)
			}

			t.Logf("Execution %d: expected %v, actual %v, delta %v", i+1, expectedTime, execTime, delta)
		}

		// Stop the workflowScheduler and check if it stops executing
		currentCounter := counter
		workflowScheduler.Stop()
		time.Sleep(3 * time.Second) // Wait a bit to ensure no more executions
		if counter >= currentCounter+2 {
			t.Fatalf("Scheduled workflow continued executing after stopping scheduler: %d (expected < %d)", counter, currentCounter+2)
		}
	})
}

var (
	sendWf                       = WithWorkflow(sendWorkflow)
	receiveWf                    = WithWorkflow(receiveWorkflow)
	receiveWfCoordinated         = WithWorkflow(receiveWorkflowCoordinated)
	sendStructWf                 = WithWorkflow(sendStructWorkflow)
	receiveStructWf              = WithWorkflow(receiveStructWorkflow)
	sendIdempotencyWf            = WithWorkflow(sendIdempotencyWorkflow)
	sendIdempotencyEvent         = NewEvent()
	recvIdempotencyWf            = WithWorkflow(receiveIdempotencyWorkflow)
	receiveIdempotencyStartEvent = NewEvent()
	receiveIdempotencyStopEvent  = NewEvent()
	sendWithinStepWf             = WithWorkflow(workflowThatCallsSendInStep)
	numConcurrentRecvWfs         = 5
	concurrentRecvReadyEvents    = make([]*Event, numConcurrentRecvWfs)
	concurrentRecvStartEvent     = NewEvent()
)

type sendWorkflowInput struct {
	DestinationID string
	Topic         string
}

func sendWorkflow(ctx context.Context, input sendWorkflowInput) (string, error) {
	err := Send(ctx, WorkflowSendInput[string]{DestinationID: input.DestinationID, Topic: input.Topic, Message: "message1"})
	if err != nil {
		return "", err
	}
	err = Send(ctx, WorkflowSendInput[string]{DestinationID: input.DestinationID, Topic: input.Topic, Message: "message2"})
	if err != nil {
		return "", err
	}
	err = Send(ctx, WorkflowSendInput[string]{DestinationID: input.DestinationID, Topic: input.Topic, Message: "message3"})
	if err != nil {
		return "", err
	}
	return "", nil
}

func receiveWorkflow(ctx context.Context, topic string) (string, error) {
	msg1, err := Recv[string](ctx, WorkflowRecvInput{Topic: topic, Timeout: 3 * time.Second})
	if err != nil {
		return "", err
	}
	msg2, err := Recv[string](ctx, WorkflowRecvInput{Topic: topic, Timeout: 3 * time.Second})
	if err != nil {
		return "", err
	}
	msg3, err := Recv[string](ctx, WorkflowRecvInput{Topic: topic, Timeout: 3 * time.Second})
	if err != nil {
		return "", err
	}
	return msg1 + "-" + msg2 + "-" + msg3, nil
}

func receiveWorkflowCoordinated(ctx context.Context, input struct {
	Topic string
	i     int
}) (string, error) {
	// Signal that this workflow has started and is ready
	concurrentRecvReadyEvents[input.i].Set()

	// Wait for the coordination event before starting to receive

	concurrentRecvStartEvent.Wait()

	// Do a single Recv call with timeout
	msg, err := Recv[string](ctx, WorkflowRecvInput{Topic: input.Topic, Timeout: 3 * time.Second})
	if err != nil {
		return "", err
	}
	return msg, nil
}

func sendStructWorkflow(ctx context.Context, input sendWorkflowInput) (string, error) {
	testStruct := sendRecvType{Value: "test-struct-value"}
	err := Send(ctx, WorkflowSendInput[sendRecvType]{DestinationID: input.DestinationID, Topic: input.Topic, Message: testStruct})
	return "", err
}

func receiveStructWorkflow(ctx context.Context, topic string) (sendRecvType, error) {
	return Recv[sendRecvType](ctx, WorkflowRecvInput{Topic: topic, Timeout: 3 * time.Second})
}

func sendIdempotencyWorkflow(ctx context.Context, input sendWorkflowInput) (string, error) {
	err := Send(ctx, WorkflowSendInput[string]{DestinationID: input.DestinationID, Topic: input.Topic, Message: "m1"})
	if err != nil {
		return "", err
	}
	sendIdempotencyEvent.Wait()
	return "idempotent-send-completed", nil
}

func receiveIdempotencyWorkflow(ctx context.Context, topic string) (string, error) {
	msg, err := Recv[string](ctx, WorkflowRecvInput{Topic: topic, Timeout: 3 * time.Second})
	if err != nil {
		return "", err
	}
	receiveIdempotencyStartEvent.Set()
	receiveIdempotencyStopEvent.Wait()
	return msg, nil
}

func stepThatCallsSend(ctx context.Context, input sendWorkflowInput) (string, error) {
	err := Send(ctx, WorkflowSendInput[string]{
		DestinationID: input.DestinationID,
		Topic:         input.Topic,
		Message:       "message-from-step",
	})
	if err != nil {
		return "", err
	}
	return "send-completed", nil
}

func workflowThatCallsSendInStep(ctx context.Context, input sendWorkflowInput) (string, error) {
	return RunAsStep(ctx, stepThatCallsSend, input)
}

type sendRecvType struct {
	Value string
}

func TestSendRecv(t *testing.T) {
	setupDBOS(t)

	t.Run("SendRecvSuccess", func(t *testing.T) {
		// Start the receive workflow
		receiveHandle, err := receiveWf(context.Background(), "test-topic")
		if err != nil {
			t.Fatalf("failed to start receive workflow: %v", err)
		}

		// Send a message to the receive workflow
		handle, err := sendWf(context.Background(), sendWorkflowInput{
			DestinationID: receiveHandle.GetWorkflowID(),
			Topic:         "test-topic",
		})
		if err != nil {
			t.Fatalf("failed to send message: %v", err)
		}
		_, err = handle.GetResult(context.Background())
		if err != nil {
			t.Fatalf("failed to get result from send workflow: %v", err)
		}

		start := time.Now()
		result, err := receiveHandle.GetResult(context.Background())
		if err != nil {
			t.Fatalf("failed to get result from receive workflow: %v", err)
		}
		if result != "message1-message2-message3" {
			t.Fatalf("expected received message to be 'message1-message2-message3', got '%s'", result)
		}
		// XXX let's see how this works when all the tests run
		if time.Since(start) > 5*time.Second {
			t.Fatalf("receive workflow took too long to complete, expected < 5s, got %v", time.Since(start))
		}
	})

	t.Run("SendRecvCustomStruct", func(t *testing.T) {
		// Start the receive workflow
		receiveHandle, err := receiveStructWf(context.Background(), "struct-topic")
		if err != nil {
			t.Fatalf("failed to start receive workflow: %v", err)
		}

		// Send the struct to the receive workflow
		sendHandle, err := sendStructWf(context.Background(), sendWorkflowInput{
			DestinationID: receiveHandle.GetWorkflowID(),
			Topic:         "struct-topic",
		})
		if err != nil {
			t.Fatalf("failed to send struct: %v", err)
		}

		_, err = sendHandle.GetResult(context.Background())
		if err != nil {
			t.Fatalf("failed to get result from send workflow: %v", err)
		}

		// Get the result from receive workflow
		result, err := receiveHandle.GetResult(context.Background())
		if err != nil {
			t.Fatalf("failed to get result from receive workflow: %v", err)
		}

		// Verify the struct was received correctly
		if result.Value != "test-struct-value" {
			t.Fatalf("expected received struct value to be 'test-struct-value', got '%s'", result.Value)
		}
	})

	t.Run("SendToNonExistentUUID", func(t *testing.T) {
		// Generate a non-existent UUID
		destUUID := uuid.NewString()

		// Send to non-existent UUID should fail
		handle, err := sendWf(context.Background(), sendWorkflowInput{
			DestinationID: destUUID,
			Topic:         "testtopic",
		})
		if err != nil {
			t.Fatalf("failed to start send workflow: %v", err)
		}

		_, err = handle.GetResult(context.Background())
		if err == nil {
			t.Fatal("expected error when sending to non-existent UUID but got none")
		}

		dbosErr, ok := err.(*DBOSError)
		if !ok {
			t.Fatalf("expected error to be of type *DBOSError, got %T", err)
		}

		if dbosErr.Code != NonExistentWorkflowError {
			t.Fatalf("expected error code to be NonExistentWorkflowError, got %v", dbosErr.Code)
		}

		expectedErrorMsg := fmt.Sprintf("workflow %s does not exist", destUUID)
		if !strings.Contains(err.Error(), expectedErrorMsg) {
			t.Fatalf("expected error message to contain '%s', got '%s'", expectedErrorMsg, err.Error())
		}
	})

	t.Run("RecvTimeout", func(t *testing.T) {
		// Create a receive workflow that tries to receive a message but no send happens
		receiveHandle, err := receiveWf(context.Background(), "timeout-test-topic")
		if err != nil {
			t.Fatalf("failed to start receive workflow: %v", err)
		}
		result, err := receiveHandle.GetResult(context.Background())
		if result != "--" {
			t.Fatalf("expected -- result on timeout, got '%s'", result)
		}
		if err != nil {
			t.Fatalf("expected no error on timeout, but got: %v", err)
		}
	})

	t.Run("RecvMustRunInsideWorkflows", func(t *testing.T) {
		ctx := context.Background()

		// Attempt to run Recv outside of a workflow context
		_, err := Recv[string](ctx, WorkflowRecvInput{Topic: "test-topic", Timeout: 1 * time.Second})
		if err == nil {
			t.Fatal("expected error when running Recv outside of workflow context, but got none")
		}

		// Check the error type
		dbosErr, ok := err.(*DBOSError)
		if !ok {
			t.Fatalf("expected error to be of type *DBOSError, got %T", err)
		}

		if dbosErr.Code != StepExecutionError {
			t.Fatalf("expected error code to be StepExecutionError, got %v", dbosErr.Code)
		}

		// Test the specific message from the error
		expectedMessagePart := "workflow state not found in context: are you running this step within a workflow?"
		if !strings.Contains(err.Error(), expectedMessagePart) {
			t.Fatalf("expected error message to contain %q, but got %q", expectedMessagePart, err.Error())
		}
	})

	t.Run("SendOutsideWorkflow", func(t *testing.T) {
		// Start a receive workflow to have a valid destination
		receiveHandle, err := receiveWf(context.Background(), "outside-workflow-topic")
		if err != nil {
			t.Fatalf("failed to start receive workflow: %v", err)
		}

		// Send messages from outside a workflow context (should work now)
		ctx := context.Background()
		for i := range 3 {
			err = Send(ctx, WorkflowSendInput[string]{
				DestinationID: receiveHandle.GetWorkflowID(),
				Topic:         "outside-workflow-topic",
				Message:       fmt.Sprintf("message%d", i+1),
			})
			if err != nil {
				t.Fatalf("failed to send message%d from outside workflow: %v", i+1, err)
			}
		}

		// Verify the receive workflow gets all messages
		result, err := receiveHandle.GetResult(context.Background())
		if err != nil {
			t.Fatalf("failed to get result from receive workflow: %v", err)
		}
		if result != "message1-message2-message3" {
			t.Fatalf("expected result to be 'message1-message2-message3', got '%s'", result)
		}
	})
	t.Run("SendRecvIdempotency", func(t *testing.T) {
		// Start the receive workflow and wait for it to be ready
		receiveHandle, err := recvIdempotencyWf(context.Background(), "idempotency-topic")
		if err != nil {
			t.Fatalf("failed to start receive idempotency workflow: %v", err)
		}

		// Send the message to the receive workflow
		sendHandle, err := sendIdempotencyWf(context.Background(), sendWorkflowInput{
			DestinationID: receiveHandle.GetWorkflowID(),
			Topic:         "idempotency-topic",
		})
		if err != nil {
			t.Fatalf("failed to send idempotency message: %v", err)
		}

		// Wait for the receive workflow to have received the message
		receiveIdempotencyStartEvent.Wait()

		// Attempt recovering both workflows. There should be only 2 steps recorded after recovery.
		recoveredHandles, err := recoverPendingWorkflows(context.Background(), []string{"local"})
		if err != nil {
			t.Fatalf("failed to recover pending workflows: %v", err)
		}
		if len(recoveredHandles) != 2 {
			t.Fatalf("expected 2 recovered handles, got %d", len(recoveredHandles))
		}
		steps, err := dbos.systemDB.GetWorkflowSteps(context.Background(), sendHandle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to get steps for send idempotency workflow: %v", err)
		}
		if len(steps) != 1 {
			t.Fatalf("expected 1 step in send idempotency workflow, got %d", len(steps))
		}
		steps, err = dbos.systemDB.GetWorkflowSteps(context.Background(), receiveHandle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to get steps for receive idempotency workflow: %v", err)
		}
		if len(steps) != 1 {
			t.Fatalf("expected 1 step in receive idempotency workflow, got %d", len(steps))
		}

		// Unblock the workflows to complete
		receiveIdempotencyStopEvent.Set()
		result, err := receiveHandle.GetResult(context.Background())
		if err != nil {
			t.Fatalf("failed to get result from receive idempotency workflow: %v", err)
		}
		if result != "m1" {
			t.Fatalf("expected result to be 'm1', got '%s'", result)
		}
		sendIdempotencyEvent.Set()
		result, err = sendHandle.GetResult(context.Background())
		if err != nil {
			t.Fatalf("failed to get result from send idempotency workflow: %v", err)
		}
		if result != "idempotent-send-completed" {
			t.Fatalf("expected result to be 'idempotent-send-completed', got '%s'", result)
		}
	})

	t.Run("SendCannotBeCalledWithinStep", func(t *testing.T) {
		// Start a receive workflow to have a valid destination
		receiveHandle, err := receiveWf(context.Background(), "send-within-step-topic")
		if err != nil {
			t.Fatalf("failed to start receive workflow: %v", err)
		}

		// Execute the workflow that tries to call Send within a step
		handle, err := sendWithinStepWf(context.Background(), sendWorkflowInput{
			DestinationID: receiveHandle.GetWorkflowID(),
			Topic:         "send-within-step-topic",
		})
		if err != nil {
			t.Fatalf("failed to start workflow: %v", err)
		}

		// Expect the workflow to fail with the specific error
		_, err = handle.GetResult(context.Background())
		if err == nil {
			t.Fatal("expected error when calling Send within a step, but got none")
		}

		// Check the error type
		dbosErr, ok := err.(*DBOSError)
		if !ok {
			t.Fatalf("expected error to be of type *DBOSError, got %T", err)
		}

		if dbosErr.Code != StepExecutionError {
			t.Fatalf("expected error code to be StepExecutionError, got %v", dbosErr.Code)
		}

		// Test the specific message from the error
		expectedMessagePart := "cannot call Send within a step"
		if !strings.Contains(err.Error(), expectedMessagePart) {
			t.Fatalf("expected error message to contain %q, but got %q", expectedMessagePart, err.Error())
		}

		// Wait for the receive workflow to time out
		result, err := receiveHandle.GetResult(context.Background())
		if err != nil {
			t.Fatalf("failed to get result from receive workflow: %v", err)
		}
		if result != "--" {
			t.Fatalf("expected receive workflow result to be '--' (timeout), got '%s'", result)
		}
	})

	t.Run("ConcurrentRecv", func(t *testing.T) {
		// Test concurrent receivers - only 1 should timeout, others should get errors
		receiveTopic := "concurrent-recv-topic"

		// Start multiple concurrent receive workflows - no messages will be sent
		numReceivers := 5
		var wg sync.WaitGroup
		results := make(chan string, numReceivers)
		errors := make(chan error, numReceivers)
		receiverHandles := make([]WorkflowHandle[string], numReceivers)

		// Start all receivers - they will signal when ready and wait for coordination
		for i := range numReceivers {
			concurrentRecvReadyEvents[i] = NewEvent()
			receiveHandle, err := receiveWfCoordinated(context.Background(), struct {
				Topic string
				i     int
			}{
				Topic: receiveTopic,
				i:     i,
			}, WithWorkflowID("concurrent-recv-wfid"))
			if err != nil {
				t.Fatalf("failed to start receive workflow %d: %v", i, err)
			}
			receiverHandles[i] = receiveHandle
		}

		// Wait for all workflows to signal they are ready
		for i := range numReceivers {
			concurrentRecvReadyEvents[i].Wait()
		}

		// Now unblock all receivers simultaneously so they race to the Recv call
		concurrentRecvStartEvent.Set()

		// Collect results from all receivers concurrently
		// Only 1 should timeout (winner of the CV), others should get errors
		wg.Add(numReceivers)
		for i := range numReceivers {
			go func(index int) {
				defer wg.Done()
				result, err := receiverHandles[index].GetResult(context.Background())
				if err != nil {
					errors <- err
				} else {
					results <- result
				}
			}(i)
		}

		wg.Wait()
		close(results)
		close(errors)

		// Count timeout results and errors
		timeoutCount := 0
		errorCount := 0

		for result := range results {
			if result == "" {
				// Empty string indicates a timeout - only 1 receiver should get this
				timeoutCount++
			}
		}

		for err := range errors {
			t.Logf("Receiver error (expected): %v", err)
			errorCount++
		}

		// Verify that exactly 1 receiver timed out and 4 got errors
		if timeoutCount != 1 {
			t.Fatalf("expected exactly 1 receiver to timeout, got %d timeouts", timeoutCount)
		}

		if errorCount != 4 {
			t.Fatalf("expected exactly 4 receivers to get errors, got %d errors", errorCount)
		}

		// Ensure total results match expected
		if timeoutCount+errorCount != numReceivers {
			t.Fatalf("expected total results (%d) to equal number of receivers (%d)", timeoutCount+errorCount, numReceivers)
		}
	})
}

var (
	setEventWf                    = WithWorkflow(setEventWorkflow)
	getEventWf                    = WithWorkflow(getEventWorkflow)
	setTwoEventsWf                = WithWorkflow(setTwoEventsWorkflow)
	setEventIdempotencyWf         = WithWorkflow(setEventIdempotencyWorkflow)
	getEventIdempotencyWf         = WithWorkflow(getEventIdempotencyWorkflow)
	setEventIdempotencyEvent      = NewEvent()
	getEventStartIdempotencyEvent = NewEvent()
	getEventStopIdempotencyEvent  = NewEvent()
	setSecondEventSignal          = NewEvent()
)

type setEventWorkflowInput struct {
	Key     string
	Message string
}

func setEventWorkflow(ctx context.Context, input setEventWorkflowInput) (string, error) {
	err := SetEvent(ctx, WorkflowSetEventInput[string]{Key: input.Key, Message: input.Message})
	if err != nil {
		return "", err
	}
	return "event-set", nil
}

func getEventWorkflow(ctx context.Context, input setEventWorkflowInput) (string, error) {
	result, err := GetEvent[string](ctx, WorkflowGetEventInput{
		TargetWorkflowID: input.Key,     // Reusing Key field as target workflow ID
		Key:              input.Message, // Reusing Message field as event key
		Timeout:          3 * time.Second,
	})
	if err != nil {
		return "", err
	}
	return result, nil
}

func setTwoEventsWorkflow(ctx context.Context, input setEventWorkflowInput) (string, error) {
	// Set the first event
	err := SetEvent(ctx, WorkflowSetEventInput[string]{Key: "event1", Message: "first-event-message"})
	if err != nil {
		return "", err
	}

	// Wait for external signal before setting the second event
	setSecondEventSignal.Wait()

	// Set the second event
	err = SetEvent(ctx, WorkflowSetEventInput[string]{Key: "event2", Message: "second-event-message"})
	if err != nil {
		return "", err
	}

	return "two-events-set", nil
}

func setEventIdempotencyWorkflow(ctx context.Context, input setEventWorkflowInput) (string, error) {
	err := SetEvent(ctx, WorkflowSetEventInput[string]{Key: input.Key, Message: input.Message})
	if err != nil {
		return "", err
	}
	setEventIdempotencyEvent.Wait()
	return "idempotent-set-completed", nil
}

func getEventIdempotencyWorkflow(ctx context.Context, input setEventWorkflowInput) (string, error) {
	result, err := GetEvent[string](ctx, WorkflowGetEventInput{
		TargetWorkflowID: input.Key,
		Key:              input.Message,
		Timeout:          3 * time.Second,
	})
	if err != nil {
		return "", err
	}
	getEventStartIdempotencyEvent.Set()
	getEventStopIdempotencyEvent.Wait()
	return result, nil
}

func TestSetGetEvent(t *testing.T) {
	setupDBOS(t)

	t.Run("SetGetEventFromWorkflow", func(t *testing.T) {
		// Clear the signal event before starting
		setSecondEventSignal.Clear()

		// Start the workflow that sets two events
		setHandle, err := setTwoEventsWf(context.Background(), setEventWorkflowInput{
			Key:     "test-workflow",
			Message: "unused",
		})
		if err != nil {
			t.Fatalf("failed to start set two events workflow: %v", err)
		}

		// Start a workflow to get the first event
		getFirstEventHandle, err := getEventWf(context.Background(), setEventWorkflowInput{
			Key:     setHandle.GetWorkflowID(), // Target workflow ID
			Message: "event1",                  // Event key
		})
		if err != nil {
			t.Fatalf("failed to start get first event workflow: %v", err)
		}

		// Verify we can get the first event
		firstMessage, err := getFirstEventHandle.GetResult(context.Background())
		if err != nil {
			t.Fatalf("failed to get result from first event workflow: %v", err)
		}
		if firstMessage != "first-event-message" {
			t.Fatalf("expected first message to be 'first-event-message', got '%s'", firstMessage)
		}

		// Signal the workflow to set the second event
		setSecondEventSignal.Set()

		// Start a workflow to get the second event
		getSecondEventHandle, err := getEventWf(context.Background(), setEventWorkflowInput{
			Key:     setHandle.GetWorkflowID(), // Target workflow ID
			Message: "event2",                  // Event key
		})
		if err != nil {
			t.Fatalf("failed to start get second event workflow: %v", err)
		}

		// Verify we can get the second event
		secondMessage, err := getSecondEventHandle.GetResult(context.Background())
		if err != nil {
			t.Fatalf("failed to get result from second event workflow: %v", err)
		}
		if secondMessage != "second-event-message" {
			t.Fatalf("expected second message to be 'second-event-message', got '%s'", secondMessage)
		}

		// Wait for the workflow to complete
		result, err := setHandle.GetResult(context.Background())
		if err != nil {
			t.Fatalf("failed to get result from set two events workflow: %v", err)
		}
		if result != "two-events-set" {
			t.Fatalf("expected result to be 'two-events-set', got '%s'", result)
		}
	})

	t.Run("GetEventFromOutsideWorkflow", func(t *testing.T) {
		// Start a workflow that sets an event
		setHandle, err := setEventWf(context.Background(), setEventWorkflowInput{
			Key:     "test-key",
			Message: "test-message",
		})
		if err != nil {
			t.Fatalf("failed to start set event workflow: %v", err)
		}

		// Wait for the event to be set
		_, err = setHandle.GetResult(context.Background())
		if err != nil {
			t.Fatalf("failed to get result from set event workflow: %v", err)
		}

		// Start a workflow that gets the event from outside the original workflow
		message, err := GetEvent[string](context.Background(), WorkflowGetEventInput{
			TargetWorkflowID: setHandle.GetWorkflowID(),
			Key:              "test-key",
			Timeout:          3 * time.Second,
		})
		if err != nil {
			t.Fatalf("failed to get event from outside workflow: %v", err)
		}
		if message != "test-message" {
			t.Fatalf("expected received message to be 'test-message', got '%s'", message)
		}
	})

	t.Run("GetEventTimeout", func(t *testing.T) {
		// Try to get an event from a non-existent workflow
		nonExistentID := uuid.NewString()
		message, err := GetEvent[string](context.Background(), WorkflowGetEventInput{
			TargetWorkflowID: nonExistentID,
			Key:              "test-key",
			Timeout:          3 * time.Second,
		})
		if err != nil {
			t.Fatal("failed to get event from non-existent workflow:", err)
		}
		if message != "" {
			t.Fatalf("expected empty result on timeout, got '%s'", message)
		}

		// Try to get an event from an existing workflow but with a key that doesn't exist
		setHandle, err := setEventWf(context.Background(), setEventWorkflowInput{
			Key:     "test-key",
			Message: "test-message",
		})
		if err != nil {
			t.Fatal("failed to set event:", err)
		}
		_, err = setHandle.GetResult(context.Background())
		if err != nil {
			t.Fatal("failed to get result from set event workflow:", err)
		}
		message, err = GetEvent[string](context.Background(), WorkflowGetEventInput{
			TargetWorkflowID: setHandle.GetWorkflowID(),
			Key:              "non-existent-key",
			Timeout:          3 * time.Second,
		})
		if err != nil {
			t.Fatal("failed to get event with non-existent key:", err)
		}
		if message != "" {
			t.Fatalf("expected empty result on timeout with non-existent key, got '%s'", message)
		}
	})

	t.Run("SetGetEventMustRunInsideWorkflows", func(t *testing.T) {
		ctx := context.Background()

		// Attempt to run SetEvent outside of a workflow context
		err := SetEvent(ctx, WorkflowSetEventInput[string]{Key: "test-key", Message: "test-message"})
		if err == nil {
			t.Fatal("expected error when running SetEvent outside of workflow context, but got none")
		}

		// Check the error type
		dbosErr, ok := err.(*DBOSError)
		if !ok {
			t.Fatalf("expected error to be of type *DBOSError, got %T", err)
		}

		if dbosErr.Code != StepExecutionError {
			t.Fatalf("expected error code to be StepExecutionError, got %v", dbosErr.Code)
		}

		// Test the specific message from the error
		expectedMessagePart := "workflow state not found in context: are you running this step within a workflow?"
		if !strings.Contains(err.Error(), expectedMessagePart) {
			t.Fatalf("expected error message to contain %q, but got %q", expectedMessagePart, err.Error())
		}
	})

	t.Run("SetGetEventIdempotency", func(t *testing.T) {
		// Start the set event workflow
		setHandle, err := setEventIdempotencyWf(context.Background(), setEventWorkflowInput{
			Key:     "idempotency-key",
			Message: "idempotency-message",
		})
		if err != nil {
			t.Fatalf("failed to start set event idempotency workflow: %v", err)
		}

		// Start the get event workflow
		getHandle, err := getEventIdempotencyWf(context.Background(), setEventWorkflowInput{
			Key:     setHandle.GetWorkflowID(),
			Message: "idempotency-key",
		})
		if err != nil {
			t.Fatalf("failed to start get event idempotency workflow: %v", err)
		}

		// Wait for the get event workflow to signal it has received the event
		getEventStartIdempotencyEvent.Wait()
		getEventStartIdempotencyEvent.Clear()

		// Attempt recovering both workflows. Each should have exactly 1 step.
		recoveredHandles, err := recoverPendingWorkflows(context.Background(), []string{"local"})
		if err != nil {
			t.Fatalf("failed to recover pending workflows: %v", err)
		}
		if len(recoveredHandles) != 2 {
			t.Fatalf("expected 2 recovered handles, got %d", len(recoveredHandles))
		}

		getEventStartIdempotencyEvent.Wait()

		// Verify step counts
		setSteps, err := dbos.systemDB.GetWorkflowSteps(context.Background(), setHandle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to get steps for set event idempotency workflow: %v", err)
		}
		if len(setSteps) != 1 {
			t.Fatalf("expected 1 step in set event idempotency workflow, got %d", len(setSteps))
		}

		getSteps, err := dbos.systemDB.GetWorkflowSteps(context.Background(), getHandle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to get steps for get event idempotency workflow: %v", err)
		}
		if len(getSteps) != 1 {
			t.Fatalf("expected 1 step in get event idempotency workflow, got %d", len(getSteps))
		}

		// Complete the workflows
		setEventIdempotencyEvent.Set()
		getEventStopIdempotencyEvent.Set()

		setResult, err := setHandle.GetResult(context.Background())
		if err != nil {
			t.Fatalf("failed to get result from set event idempotency workflow: %v", err)
		}
		if setResult != "idempotent-set-completed" {
			t.Fatalf("expected result to be 'idempotent-set-completed', got '%s'", setResult)
		}

		getResult, err := getHandle.GetResult(context.Background())
		if err != nil {
			t.Fatalf("failed to get result from get event idempotency workflow: %v", err)
		}
		if getResult != "idempotency-message" {
			t.Fatalf("expected result to be 'idempotency-message', got '%s'", getResult)
		}
	})

	t.Run("ConcurrentGetEvent", func(t *testing.T) {
		// Set event
		setHandle, err := setEventWf(context.Background(), setEventWorkflowInput{
			Key:     "concurrent-event-key",
			Message: "concurrent-event-message",
		})
		if err != nil {
			t.Fatalf("failed to start set event workflow: %v", err)
		}

		// Wait for the set event workflow to complete
		_, err = setHandle.GetResult(context.Background())
		if err != nil {
			t.Fatalf("failed to get result from set event workflow: %v", err)
		}
		// Start a few goroutines that'll concurrently get the event
		numGoroutines := 5
		var wg sync.WaitGroup
		errors := make(chan error, numGoroutines)
		wg.Add(numGoroutines)
		for range numGoroutines {
			go func() {
				defer wg.Done()
				res, err := GetEvent[string](context.Background(), WorkflowGetEventInput{
					TargetWorkflowID: setHandle.GetWorkflowID(),
					Key:              "concurrent-event-key",
					Timeout:          10 * time.Second,
				})
				if err != nil {
					errors <- fmt.Errorf("failed to get event in goroutine: %v", err)
					return
				}
				if res != "concurrent-event-message" {
					errors <- fmt.Errorf("expected result in goroutine to be 'concurrent-event-message', got '%s'", res)
					return
				}
			}()
		}
		wg.Wait()
		close(errors)

		// Check for any errors from goroutines
		for err := range errors {
			t.Fatal(err)
		}
	})
}

var (
	sleepRecoveryWf = WithWorkflow(sleepRecoveryWorkflow)
	sleepStartEvent *Event
	sleepStopEvent  *Event
)

func sleepRecoveryWorkflow(ctx context.Context, duration time.Duration) (time.Duration, error) {
	result, err := Sleep(ctx, duration)
	if err != nil {
		return 0, err
	}
	// Block after sleep so we can recover a pending workflow
	sleepStartEvent.Set()
	sleepStopEvent.Wait()
	return result, nil
}

func TestSleep(t *testing.T) {
	setupDBOS(t)

	t.Run("SleepDurableRecovery", func(t *testing.T) {
		sleepStartEvent = NewEvent()
		sleepStopEvent = NewEvent()

		// Start a workflow that sleeps for 2 seconds then blocks
		sleepDuration := 2 * time.Second

		handle, err := sleepRecoveryWf(context.Background(), sleepDuration)
		if err != nil {
			t.Fatalf("failed to start sleep recovery workflow: %v", err)
		}

		sleepStartEvent.Wait()
		sleepStartEvent.Clear()

		// Run the workflow again and check the return time was less than the durable sleep
		startTime := time.Now()
		_, err = sleepRecoveryWf(context.Background(), sleepDuration, WithWorkflowID(handle.GetWorkflowID()))
		if err != nil {
			t.Fatalf("failed to start second sleep recovery workflow: %v", err)
		}

		sleepStartEvent.Wait()
		// Time elapsed should be at most the sleep duration
		elapsed := time.Since(startTime)
		if elapsed >= sleepDuration {
			t.Fatalf("expected elapsed time to be less than %v, got %v", sleepDuration, elapsed)
		}

		// Verify the sleep step was recorded correctly
		steps, err := dbos.systemDB.GetWorkflowSteps(context.Background(), handle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to get workflow steps: %v", err)
		}

		if len(steps) != 1 {
			t.Fatalf("expected 1 step (the sleep), got %d", len(steps))
		}

		step := steps[0]
		if step.FunctionName != "DBOS.sleep" {
			t.Fatalf("expected step name to be 'DBOS.sleep', got '%s'", step.FunctionName)
		}

		if step.Error != nil {
			t.Fatalf("expected step to have no error, got %v", step.Error)
		}

		sleepStopEvent.Set()
	})

	t.Run("SleepCannotBeCalledOutsideWorkflow", func(t *testing.T) {
		ctx := context.Background()

		// Attempt to call Sleep outside of a workflow context
		_, err := Sleep(ctx, 1*time.Second)
		if err == nil {
			t.Fatal("expected error when calling Sleep outside of workflow context, but got none")
		}

		// Check the error type
		dbosErr, ok := err.(*DBOSError)
		if !ok {
			t.Fatalf("expected error to be of type *DBOSError, got %T", err)
		}

		if dbosErr.Code != StepExecutionError {
			t.Fatalf("expected error code to be StepExecutionError, got %v", dbosErr.Code)
		}

		// Test the specific message from the error
		expectedMessagePart := "workflow state not found in context: are you running this step within a workflow?"
		if !strings.Contains(err.Error(), expectedMessagePart) {
			t.Fatalf("expected error message to contain %q, but got %q", expectedMessagePart, err.Error())
		}
	})
}
