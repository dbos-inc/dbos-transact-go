package dbos

/**
Test workflow and steps features
[x] Wrapping various golang methods in DBOS workflows
[x] workflow idempotency
[x] workflow DLQ
[x] workflow conflicting name
[x] workflow timeouts & deadlines (including child workflows)
*/

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Global counter for idempotency testing
var idempotencyCounter int64

func simpleWorkflow(dbosCtx DBOSContext, input string) (string, error) {
	return input, nil
}

func simpleWorkflowError(dbosCtx DBOSContext, input string) (int, error) {
	return 0, fmt.Errorf("failure")
}

func simpleWorkflowWithStep(dbosCtx DBOSContext, input string) (string, error) {
	return RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return simpleStep(ctx)
	})
}

func simpleStep(_ context.Context) (string, error) {
	return "from step", nil
}

func simpleStepError(_ context.Context) (string, error) {
	return "", fmt.Errorf("step failure")
}

func simpleWorkflowWithStepError(dbosCtx DBOSContext, input string) (string, error) {
	return RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return simpleStepError(ctx)
	})
}

// idempotencyWorkflow increments a global counter and returns the input
func incrementCounter(_ context.Context, value int64) (int64, error) {
	idempotencyCounter += value
	return idempotencyCounter, nil
}

// Unified struct that demonstrates both pointer and value receiver methods
type workflowStruct struct{}

// Pointer receiver method
func (w *workflowStruct) simpleWorkflow(dbosCtx DBOSContext, input string) (string, error) {
	return simpleWorkflow(dbosCtx, input)
}

// Value receiver method on the same struct
func (w workflowStruct) simpleWorkflowValue(dbosCtx DBOSContext, input string) (string, error) {
	return input + "-value", nil
}

// interface for workflow methods
type TestWorkflowInterface interface {
	Execute(dbosCtx DBOSContext, input string) (string, error)
}

type workflowImplementation struct {
	field string
}

func (w *workflowImplementation) Execute(dbosCtx DBOSContext, input string) (string, error) {
	return input + "-" + w.field + "-interface", nil
}

// Generic workflow function
func Identity[T any](dbosCtx DBOSContext, in T) (T, error) {
	return in, nil
}

func TestWorkflowsRegistration(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)

	// Setup workflows with executor
	RegisterWorkflow(dbosCtx, simpleWorkflow)
	RegisterWorkflow(dbosCtx, simpleWorkflowError)
	RegisterWorkflow(dbosCtx, simpleWorkflowWithStep)
	RegisterWorkflow(dbosCtx, simpleWorkflowWithStepError)
	// struct methods
	s := workflowStruct{}
	RegisterWorkflow(dbosCtx, s.simpleWorkflow)
	RegisterWorkflow(dbosCtx, s.simpleWorkflowValue)
	// interface method workflow
	workflowIface := TestWorkflowInterface(&workflowImplementation{
		field: "example",
	})
	RegisterWorkflow(dbosCtx, workflowIface.Execute)
	// Generic workflow
	RegisterWorkflow(dbosCtx, Identity[int])
	// Closure with captured state
	prefix := "hello-"
	closureWorkflow := func(dbosCtx DBOSContext, in string) (string, error) {
		return prefix + in, nil
	}
	RegisterWorkflow(dbosCtx, closureWorkflow)
	// Anonymous workflow
	anonymousWorkflow := func(dbosCtx DBOSContext, in string) (string, error) {
		return "anonymous-" + in, nil
	}
	RegisterWorkflow(dbosCtx, anonymousWorkflow)

	type testCase struct {
		name           string
		workflowFunc   func(DBOSContext, string, ...WorkflowOption) (any, error)
		input          string
		expectedResult any
		expectError    bool
		expectedError  string
	}

	tests := []testCase{
		{
			name: "SimpleWorkflow",
			workflowFunc: func(dbosCtx DBOSContext, input string, opts ...WorkflowOption) (any, error) {
				handle, err := RunAsWorkflow(dbosCtx, simpleWorkflow, input, opts...)
				if err != nil {
					return nil, err
				}
				result, err := handle.GetResult()
				_, err2 := handle.GetResult()
				if err2 == nil {
					return nil, fmt.Errorf("Second call to GetResult should return an error")
				}
				expectedErrorMsg := "workflow result channel is already closed. Did you call GetResult() twice on the same workflow handle?"
				if err2.Error() != expectedErrorMsg {
					return nil, fmt.Errorf("Unexpected error message: %v, expected: %s", err2, expectedErrorMsg)
				}
				return result, err
			},
			input:          "echo",
			expectedResult: "echo",
			expectError:    false,
		},
		{
			name: "SimpleWorkflowError",
			workflowFunc: func(dbosCtx DBOSContext, input string, opts ...WorkflowOption) (any, error) {
				handle, err := RunAsWorkflow(dbosCtx, simpleWorkflowError, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:         "echo",
			expectError:   true,
			expectedError: "failure",
		},
		{
			name: "SimpleWorkflowWithStep",
			workflowFunc: func(dbosCtx DBOSContext, input string, opts ...WorkflowOption) (any, error) {
				handle, err := RunAsWorkflow(dbosCtx, simpleWorkflowWithStep, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "echo",
			expectedResult: "from step",
			expectError:    false,
		},
		{
			name: "SimpleWorkflowStruct",
			workflowFunc: func(dbosCtx DBOSContext, input string, opts ...WorkflowOption) (any, error) {
				handle, err := RunAsWorkflow(dbosCtx, s.simpleWorkflow, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "echo",
			expectedResult: "echo",
			expectError:    false,
		},
		{
			name: "ValueReceiverWorkflow",
			workflowFunc: func(dbosCtx DBOSContext, input string, opts ...WorkflowOption) (any, error) {
				handle, err := RunAsWorkflow(dbosCtx, s.simpleWorkflowValue, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "echo",
			expectedResult: "echo-value",
			expectError:    false,
		},
		{
			name: "interfaceMethodWorkflow",
			workflowFunc: func(dbosCtx DBOSContext, input string, opts ...WorkflowOption) (any, error) {
				handle, err := RunAsWorkflow(dbosCtx, workflowIface.Execute, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "echo",
			expectedResult: "echo-example-interface",
			expectError:    false,
		},
		{
			name: "GenericWorkflow",
			workflowFunc: func(dbosCtx DBOSContext, input string, opts ...WorkflowOption) (any, error) {
				handle, err := RunAsWorkflow(dbosCtx, Identity, 42, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "42", // input not used in this case
			expectedResult: 42,
			expectError:    false,
		},
		{
			name: "ClosureWithCapturedState",
			workflowFunc: func(dbosCtx DBOSContext, input string, opts ...WorkflowOption) (any, error) {
				handle, err := RunAsWorkflow(dbosCtx, closureWorkflow, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "world",
			expectedResult: "hello-world",
			expectError:    false,
		},
		{
			name: "AnonymousClosure",
			workflowFunc: func(dbosCtx DBOSContext, input string, opts ...WorkflowOption) (any, error) {
				handle, err := RunAsWorkflow(dbosCtx, anonymousWorkflow, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "test",
			expectedResult: "anonymous-test",
			expectError:    false,
		},
		{
			name: "SimpleWorkflowWithStepError",
			workflowFunc: func(dbosCtx DBOSContext, input string, opts ...WorkflowOption) (any, error) {
				handle, err := RunAsWorkflow(dbosCtx, simpleWorkflowWithStepError, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:         "echo",
			expectError:   true,
			expectedError: "step failure",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := tc.workflowFunc(dbosCtx, tc.input, WithWorkflowID(uuid.NewString()))

			if tc.expectError {
				require.Error(t, err, "expected error but got none")
				if tc.expectedError != "" {
					assert.Equal(t, tc.expectedError, err.Error())
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expectedResult, result)
			}
		})
	}

	t.Run("DoubleRegistrationWithoutName", func(t *testing.T) {
		// Create a fresh DBOS context for this test
		freshCtx := setupDBOS(t, false, false) // Don't check for leaks and don't reset DB

		// First registration should work
		RegisterWorkflow(freshCtx, simpleWorkflow)

		// Second registration of the same workflow should panic with ConflictingRegistrationError
		defer func() {
			r := recover()
			require.NotNil(t, r, "expected panic from double registration but got none")
			dbosErr, ok := r.(*DBOSError)
			require.True(t, ok, "expected panic to be *DBOSError, got %T", r)
			assert.Equal(t, ConflictingRegistrationError, dbosErr.Code)
		}()
		RegisterWorkflow(freshCtx, simpleWorkflow)
	})

	t.Run("DoubleRegistrationWithCustomName", func(t *testing.T) {
		// Create a fresh DBOS context for this test
		freshCtx := setupDBOS(t, false, false) // Don't check for leaks and don't reset DB

		// First registration with custom name should work
		RegisterWorkflow(freshCtx, simpleWorkflow, WithWorkflowName("custom-workflow"))

		// Second registration with same custom name should panic with ConflictingRegistrationError
		defer func() {
			r := recover()
			require.NotNil(t, r, "expected panic from double registration with custom name but got none")
			dbosErr, ok := r.(*DBOSError)
			require.True(t, ok, "expected panic to be *DBOSError, got %T", r)
			assert.Equal(t, ConflictingRegistrationError, dbosErr.Code)
		}()
		RegisterWorkflow(freshCtx, simpleWorkflow, WithWorkflowName("custom-workflow"))
	})

	t.Run("RegisterAfterLaunchPanics", func(t *testing.T) {
		// Create a fresh DBOS context for this test
		freshCtx := setupDBOS(t, false, false) // Don't check for leaks and don't reset DB

		// Launch DBOS context
		err := freshCtx.Launch()
		require.NoError(t, err)
		defer freshCtx.Cancel()

		// Attempting to register after launch should panic
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic from registration after launch but got none")
			}
		}()
		RegisterWorkflow(freshCtx, simpleWorkflow)
	})
}

func stepWithinAStep(ctx context.Context) (string, error) {
	return simpleStep(ctx)
}

func stepWithinAStepWorkflow(dbosCtx DBOSContext, input string) (string, error) {
	return RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return stepWithinAStep(ctx)
	})
}

// Global counter for retry testing
var stepRetryAttemptCount int

func stepRetryAlwaysFailsStep(_ context.Context) (string, error) {
	stepRetryAttemptCount++
	return "", fmt.Errorf("always fails - attempt %d", stepRetryAttemptCount)
}

var stepIdempotencyCounter int

func stepIdempotencyTest(_ context.Context) (string, error) {
	stepIdempotencyCounter++
	return "", nil
}

func stepRetryWorkflow(dbosCtx DBOSContext, input string) (string, error) {
	RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return stepIdempotencyTest(ctx)
	})
	stepCtx := WithValue(dbosCtx, StepParamsKey, &StepParams{
		MaxRetries:   5,
		BaseInterval: 1 * time.Millisecond,
		MaxInterval:  10 * time.Millisecond,
	})

	return RunAsStep(stepCtx, func(ctx context.Context) (string, error) {
		return stepRetryAlwaysFailsStep(ctx)
	})
}

func TestSteps(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)

	// Create workflows with executor
	RegisterWorkflow(dbosCtx, stepWithinAStepWorkflow)
	RegisterWorkflow(dbosCtx, stepRetryWorkflow)

	t.Run("StepsMustRunInsideWorkflows", func(t *testing.T) {
		// Attempt to run a step outside of a workflow context
		_, err := RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
			return simpleStep(ctx)
		})
		require.Error(t, err, "expected error when running step outside of workflow context, but got none")

		// Check the error type
		dbosErr, ok := err.(*DBOSError)
		require.True(t, ok, "expected error to be of type *DBOSError, got %T", err)

		require.Equal(t, StepExecutionError, dbosErr.Code, "expected error code to be StepExecutionError, got %v", dbosErr.Code)

		// Test the specific message from the 3rd argument
		expectedMessagePart := "workflow state not found in context: are you running this step within a workflow?"
		require.Contains(t, err.Error(), expectedMessagePart, "expected error message to contain %q, but got %q", expectedMessagePart, err.Error())
	})

	t.Run("StepWithinAStepAreJustFunctions", func(t *testing.T) {
		handle, err := RunAsWorkflow(dbosCtx, stepWithinAStepWorkflow, "test")
		require.NoError(t, err, "failed to run step within a step")
		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from step within a step")
		assert.Equal(t, "from step", result)

		steps, err := dbosCtx.(*dbosContext).systemDB.getWorkflowSteps(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to list steps")
		require.Len(t, steps, 1, "expected 1 step, got %d", len(steps))
	})

	t.Run("StepRetryWithExponentialBackoff", func(t *testing.T) {
		// Reset the global counters before test
		stepRetryAttemptCount = 0
		stepIdempotencyCounter = 0

		// Execute the workflow
		handle, err := RunAsWorkflow(dbosCtx, stepRetryWorkflow, "test")
		require.NoError(t, err, "failed to start retry workflow")

		_, err = handle.GetResult()
		require.Error(t, err, "expected error from failing workflow but got none")

		// Verify the step was called exactly 6 times (max attempts + 1 initial attempt)
		assert.Equal(t, 6, stepRetryAttemptCount, "expected 6 attempts")

		// Verify the error is a MaxStepRetriesExceeded error
		dbosErr, ok := err.(*DBOSError)
		require.True(t, ok, "expected error to be of type *DBOSError, got %T", err)

		assert.Equal(t, MaxStepRetriesExceeded, dbosErr.Code, "expected error code to be MaxStepRetriesExceeded")

		// Verify the error contains the step name and max retries
		expectedErrorMessage := "has exceeded its maximum of 5 retries"
		assert.Contains(t, dbosErr.Message, expectedErrorMessage, "expected error message to contain expected text")

		// Verify each error message is present in the joined error
		for i := 1; i <= 5; i++ {
			expectedMsg := fmt.Sprintf("always fails - attempt %d", i)
			assert.Contains(t, dbosErr.Error(), expectedMsg, "expected joined error to contain expected message")
		}

		// Verify that the failed step was still recorded in the database
		steps, err := dbosCtx.(*dbosContext).systemDB.getWorkflowSteps(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps")

		require.Len(t, steps, 2, "expected 2 recorded steps")

		// Verify the second step has the error
		step := steps[1]
		require.NotNil(t, step.Error, "expected error in recorded step, got none")

		assert.Equal(t, dbosErr.Error(), step.Error.Error(), "expected recorded step error to match joined error")

		// Verify the idempotency step was executed only once
		assert.Equal(t, 1, stepIdempotencyCounter, "expected idempotency step to be executed only once")
	})
}

func TestChildWorkflow(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)

	type Inheritance struct {
		ParentID string
		Index    int
	}

	// Create child workflows with executor
	childWf := func(dbosCtx DBOSContext, input Inheritance) (string, error) {
		workflowID, err := GetWorkflowID(dbosCtx)
		if err != nil {
			return "", fmt.Errorf("failed to get workflow ID: %w", err)
		}
		expectedCurrentID := fmt.Sprintf("%s-0", input.ParentID)
		if workflowID != expectedCurrentID {
			return "", fmt.Errorf("expected childWf workflow ID to be %s, got %s", expectedCurrentID, workflowID)
		}
		// Steps of a child workflow start with an incremented step ID, because the first step ID is allocated to the child workflow
		return RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
			return simpleStep(ctx)
		})
	}
	RegisterWorkflow(dbosCtx, childWf)

	parentWf := func(ctx DBOSContext, input Inheritance) (string, error) {
		workflowID, err := GetWorkflowID(ctx)
		if err != nil {
			return "", fmt.Errorf("failed to get workflow ID: %w", err)
		}

		childHandle, err := RunAsWorkflow(ctx, childWf, Inheritance{ParentID: workflowID})
		if err != nil {
			return "", fmt.Errorf("failed to run child workflow: %w", err)
		}

		// Check this wf ID is built correctly
		expectedParentID := fmt.Sprintf("%s-%d", input.ParentID, input.Index)
		if workflowID != expectedParentID {
			return "", fmt.Errorf("expected parentWf workflow ID to be %s, got %s", expectedParentID, workflowID)
		}
		res, err := childHandle.GetResult()
		if err != nil {
			return "", fmt.Errorf("failed to get result from child workflow: %w", err)
		}

		// Check the steps from this workflow
		steps, err := ctx.(*dbosContext).systemDB.getWorkflowSteps(ctx, workflowID)
		if err != nil {
			return "", fmt.Errorf("failed to get workflow steps: %w", err)
		}
		if len(steps) != 2 {
			return "", fmt.Errorf("expected 2 recorded steps, got %d", len(steps))
		}
		// Verify the first step is the child workflow
		if steps[0].StepID != 0 {
			return "", fmt.Errorf("expected first step ID to be 0, got %d", steps[0].StepID)
		}
		if steps[0].StepName != runtime.FuncForPC(reflect.ValueOf(childWf).Pointer()).Name() {
			return "", fmt.Errorf("expected first step to be child workflow, got %s", steps[0].StepName)
		}
		if steps[0].Output != nil {
			return "", fmt.Errorf("expected first step output to be nil, got %s", steps[0].Output)
		}
		if steps[1].Error != nil {
			return "", fmt.Errorf("expected second step error to be nil, got %s", steps[1].Error)
		}
		if steps[0].ChildWorkflowID != childHandle.GetWorkflowID() {
			return "", fmt.Errorf("expected first step child workflow ID to be %s, got %s", childHandle.GetWorkflowID(), steps[0].ChildWorkflowID)
		}

		// The second step is the result from the child workflow
		if steps[1].StepID != 1 {
			return "", fmt.Errorf("expected second step ID to be 1, got %d", steps[1].StepID)
		}
		if steps[1].StepName != "DBOS.getResult" {
			return "", fmt.Errorf("expected second step name to be getResult, got %s", steps[1].StepName)
		}
		if steps[1].Output != "from step" {
			return "", fmt.Errorf("expected second step output to be 'from step', got %s", steps[1].Output)
		}
		if steps[1].Error != nil {
			return "", fmt.Errorf("expected second step error to be nil, got %s", steps[1].Error)
		}
		if steps[1].ChildWorkflowID != childHandle.GetWorkflowID() {
			return "", fmt.Errorf("expected second step child workflow ID to be %s, got %s", childHandle.GetWorkflowID(), steps[1].ChildWorkflowID)
		}

		return res, nil
	}
	RegisterWorkflow(dbosCtx, parentWf)

	grandParentWf := func(ctx DBOSContext, r int) (string, error) {
		workflowID, err := GetWorkflowID(ctx)
		if err != nil {
			return "", fmt.Errorf("failed to get workflow ID: %w", err)
		}

		// 2 steps per loop: spawn child and get result
		for i := range r {
			expectedStepID := (2 * i)
			parentHandle, err := RunAsWorkflow(ctx, parentWf, Inheritance{ParentID: workflowID, Index: expectedStepID})
			if err != nil {
				return "", fmt.Errorf("failed to run parent workflow: %w", err)
			}

			// Verify parent (this workflow's child) ID follows the pattern: parentID-functionID
			parentWorkflowID := parentHandle.GetWorkflowID()

			expectedParentID := fmt.Sprintf("%s-%d", workflowID, expectedStepID)
			if parentWorkflowID != expectedParentID {
				return "", fmt.Errorf("expected parent workflow ID to be %s, got %s", expectedParentID, parentWorkflowID)
			}

			result, err := parentHandle.GetResult()
			if err != nil {
				return "", fmt.Errorf("failed to get result from parent workflow: %w", err)
			}
			if result != "from step" {
				return "", fmt.Errorf("expected result from parent workflow to be 'from step', got %s", result)
			}

		}
		// Check the steps from this workflow
		steps, err := ctx.(*dbosContext).systemDB.getWorkflowSteps(ctx, workflowID)
		if err != nil {
			return "", fmt.Errorf("failed to get workflow steps: %w", err)
		}
		if len(steps) != r*2 {
			return "", fmt.Errorf("expected 2 recorded steps, got %d", len(steps))
		}

		// We do expect the steps to be returned in the order of execution, which seems to be the case even without an ORDER BY function_id ASC clause in the SQL query
		for i := 0; i < r; i += 2 {
			expectedStepID := i
			expectedChildID := fmt.Sprintf("%s-%d", workflowID, i)
			childWfStep := steps[i]
			getResultStep := steps[i+1]

			if childWfStep.StepID != expectedStepID {
				return "", fmt.Errorf("expected child wf step ID to be %d, got %d", expectedStepID, childWfStep.StepID)
			}
			if getResultStep.StepID != expectedStepID+1 {
				return "", fmt.Errorf("expected get result step ID to be %d, got %d", expectedStepID+1, getResultStep.StepID)
			}
			expectedName := runtime.FuncForPC(reflect.ValueOf(parentWf).Pointer()).Name()
			if childWfStep.StepName != expectedName {
				return "", fmt.Errorf("expected child wf step name to be %s, got %s", expectedName, childWfStep.StepName)
			}
			expectedName = "DBOS.getResult"
			if getResultStep.StepName != expectedName {
				return "", fmt.Errorf("expected get result step name to be %s, got %s", expectedName, getResultStep.StepName)
			}

			if childWfStep.Output != nil {
				return "", fmt.Errorf("expected child wf step output to be nil, got %s", childWfStep.Output)
			}
			if getResultStep.Output != "from step" {
				return "", fmt.Errorf("expected get result step output to be 'from step', got %s", getResultStep.Output)
			}

			if childWfStep.Error != nil {
				return "", fmt.Errorf("expected child wf step error to be nil, got %s", childWfStep.Error)
			}
			if getResultStep.Error != nil {
				return "", fmt.Errorf("expected get result step error to be nil, got %s", getResultStep.Error)
			}
			if childWfStep.ChildWorkflowID != expectedChildID {
				return "", fmt.Errorf("expected step child workflow ID to be %s, got %s", expectedChildID, childWfStep.ChildWorkflowID)
			}
			if getResultStep.ChildWorkflowID != expectedChildID {
				return "", fmt.Errorf("expected step child workflow ID to be %s, got %s", expectedChildID, getResultStep.ChildWorkflowID)
			}
		}

		return "", nil
	}
	RegisterWorkflow(dbosCtx, grandParentWf)

	t.Run("ChildWorkflowIDGeneration", func(t *testing.T) {
		r := 3
		h, err := RunAsWorkflow(dbosCtx, grandParentWf, r)
		if err != nil {
			t.Fatalf("failed to execute grand parent workflow: %v", err)
		}
		_, err = h.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from grand parent workflow: %v", err)
		}
	})

	t.Run("ChildWorkflowWithCustomID", func(t *testing.T) {
		customChildID := uuid.NewString()

		simpleChildWf := func(dbosCtx DBOSContext, input string) (string, error) {
			return RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
				return simpleStep(ctx)
			})
		}
		RegisterWorkflow(dbosCtx, simpleChildWf)

		// Simple parent that starts one child with a custom workflow ID
		parentWf := func(ctx DBOSContext, input string) (string, error) {
			childHandle, err := RunAsWorkflow(ctx, simpleChildWf, "test-child-input", WithWorkflowID(customChildID))
			if err != nil {
				return "", fmt.Errorf("failed to run child workflow: %w", err)
			}

			result, err := childHandle.GetResult()
			if err != nil {
				return "", fmt.Errorf("failed to get result from child workflow: %w", err)
			}

			return result, nil
		}
		RegisterWorkflow(dbosCtx, parentWf)

		parentHandle, err := RunAsWorkflow(dbosCtx, parentWf, "test-input")
		if err != nil {
			t.Fatalf("failed to start parent workflow: %v", err)
		}

		result, err := parentHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from parent workflow: %v", err)
		}
		if result != "from step" {
			t.Fatalf("expected result 'from step', got '%s'", result)
		}

		// Verify the child workflow was recorded as step 0
		steps, err := dbosCtx.(*dbosContext).systemDB.getWorkflowSteps(dbosCtx, parentHandle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to get workflow steps: %v", err)
		}
		if len(steps) != 2 {
			require.Len(t, steps, 2, "expected 2 recorded steps, got %d", len(steps))
		}

		// Verify first step is the child workflow with stepID=0
		if steps[0].StepID != 0 {
			t.Fatalf("expected first step ID to be 0, got %d", steps[0].StepID)
		}
		if steps[0].StepName != runtime.FuncForPC(reflect.ValueOf(simpleChildWf).Pointer()).Name() {
			t.Fatalf("expected first step to be child workflow, got %s", steps[0].StepName)
		}
		if steps[0].ChildWorkflowID != customChildID {
			t.Fatalf("expected first step child workflow ID to be %s, got %s", customChildID, steps[0].ChildWorkflowID)
		}

		// Verify second step is the getResult call with stepID=1
		if steps[1].StepID != 1 {
			t.Fatalf("expected second step ID to be 1, got %d", steps[1].StepID)
		}
		if steps[1].StepName != "DBOS.getResult" {
			t.Fatalf("expected second step name to be getResult, got %s", steps[1].StepName)
		}
		if steps[1].ChildWorkflowID != customChildID {
			t.Fatalf("expected second step child workflow ID to be %s, got %s", customChildID, steps[1].ChildWorkflowID)
		}
	})

	t.Run("RecoveredChildWorkflowPollingHandle", func(t *testing.T) {
		pollingHandleStartEvent := NewEvent()
		pollingHandleCompleteEvent := NewEvent()
		knownChildID := "known-child-workflow-id"
		knownParentID := "known-parent-workflow-id"
		counter := 0

		// Simple child workflow that returns a result
		pollingHandleChildWf := func(dbosCtx DBOSContext, input string) (string, error) {
			// Wait
			pollingHandleCompleteEvent.Wait()
			return input + "-result", nil
		}
		RegisterWorkflow(dbosCtx, pollingHandleChildWf)

		pollingHandleParentWf := func(ctx DBOSContext, input string) (string, error) {
			counter++

			// Run child workflow with a known ID
			childHandle, err := RunAsWorkflow(ctx, pollingHandleChildWf, "child-input", WithWorkflowID(knownChildID))
			if err != nil {
				return "", fmt.Errorf("failed to run child workflow: %w", err)
			}

			switch counter {
			case 1:
				// First handle will be a direct handle
				_, ok := childHandle.(*workflowHandle[string])
				if !ok {
					return "", fmt.Errorf("expected child handle to be of type workflowDirectHandle, got %T", childHandle)
				}
			case 2:
				// Second handle will be a polling handle
				_, ok := childHandle.(*workflowPollingHandle[string])
				if !ok {
					return "", fmt.Errorf("expected recovered child handle to be of type workflowPollingHandle, got %T", childHandle)
				}
			}

			// Signal the child workflow is started
			pollingHandleStartEvent.Set()

			result, err := childHandle.GetResult()
			if err != nil {
				return "", fmt.Errorf("failed to get result from child workflow: %w", err)
			}
			return result, nil
		}
		RegisterWorkflow(dbosCtx, pollingHandleParentWf)

		// Execute parent workflow - it will block after starting the child
		parentHandle, err := RunAsWorkflow(dbosCtx, pollingHandleParentWf, "parent-input", WithWorkflowID(knownParentID))
		if err != nil {
			t.Fatalf("failed to start parent workflow: %v", err)
		}

		// Wait for the workflows to start
		pollingHandleStartEvent.Wait()

		// Recover pending workflows - this should give us both parent and child handles
		recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		if err != nil {
			t.Fatalf("failed to recover pending workflows: %v", err)
		}

		// Should have recovered both parent and child workflows
		if len(recoveredHandles) != 2 {
			require.Len(t, recoveredHandles, 2, "expected 2 recovered handles (parent and child), got %d", len(recoveredHandles))
		}

		// Find the child handle and verify it's a polling handle with the correct ID
		var childRecoveredHandle WorkflowHandle[any]
		for _, handle := range recoveredHandles {
			if handle.GetWorkflowID() == knownChildID {
				childRecoveredHandle = handle
				break
			}
		}

		require.NotNil(t, childRecoveredHandle, "failed to find recovered child workflow handle with ID %s", knownChildID)

		// Complete both workflows
		pollingHandleCompleteEvent.Set()
		result, err := parentHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from original parent workflow: %v", err)
		}
		if result != "child-input-result" {
			t.Fatalf("expected result 'child-input-result', got '%s'", result)
		}
		childResult, err := childRecoveredHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from recovered child handle: %v", err)
		}
		if childResult != result {
			t.Fatalf("expected child result '%s', got '%s'", result, childResult)
		}
	})
}

// Idempotency workflows moved to test functions

func idempotencyWorkflow(dbosCtx DBOSContext, input string) (string, error) {
	RunAsStep(dbosCtx, func(ctx context.Context) (int64, error) {
		return incrementCounter(ctx, int64(1))
	})
	return input, nil
}

var blockingStepStopEvent *Event

func blockingStep(_ context.Context) (string, error) {
	blockingStepStopEvent.Wait()
	return "", nil
}

var idempotencyWorkflowWithStepEvent *Event

func idempotencyWorkflowWithStep(dbosCtx DBOSContext, input string) (int64, error) {
	RunAsStep(dbosCtx, func(ctx context.Context) (int64, error) {
		return incrementCounter(ctx, int64(1))
	})
	idempotencyWorkflowWithStepEvent.Set()
	RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return blockingStep(ctx)
	})
	return idempotencyCounter, nil
}

func TestWorkflowIdempotency(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)
	RegisterWorkflow(dbosCtx, idempotencyWorkflow)

	t.Run("WorkflowExecutedOnlyOnce", func(t *testing.T) {
		idempotencyCounter = 0

		workflowID := uuid.NewString()
		input := "idempotency-test"

		// Execute the same workflow twice with the same ID
		// First execution
		handle1, err := RunAsWorkflow(dbosCtx, idempotencyWorkflow, input, WithWorkflowID(workflowID))
		if err != nil {
			t.Fatalf("failed to execute workflow first time: %v", err)
		}
		result1, err := handle1.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from first execution: %v", err)
		}

		// Second execution with the same workflow ID
		handle2, err := RunAsWorkflow(dbosCtx, idempotencyWorkflow, input, WithWorkflowID(workflowID))
		if err != nil {
			t.Fatalf("failed to execute workflow second time: %v", err)
		}
		result2, err := handle2.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from second execution: %v", err)
		}

		if handle1.GetWorkflowID() != handle2.GetWorkflowID() {
			t.Fatalf("expected both handles to represent the same workflow ID, got %s and %s", handle2.GetWorkflowID(), handle1.GetWorkflowID())
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
	dbosCtx := setupDBOS(t, true, true)
	RegisterWorkflow(dbosCtx, idempotencyWorkflowWithStep)
	t.Run("RecoveryResumeWhereItLeftOff", func(t *testing.T) {
		// Reset the global counter
		idempotencyCounter = 0

		// First execution - run the workflow once
		input := "recovery-test"
		idempotencyWorkflowWithStepEvent = NewEvent()
		blockingStepStopEvent = NewEvent()
		handle1, err := RunAsWorkflow(dbosCtx, idempotencyWorkflowWithStep, input)
		if err != nil {
			t.Fatalf("failed to execute workflow first time: %v", err)
		}

		idempotencyWorkflowWithStepEvent.Wait() // Wait for the first step to complete. The second spins forever.

		// Run recovery for pending workflows with "local" executor
		recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		if err != nil {
			t.Fatalf("failed to recover pending workflows: %v", err)
		}

		// Check that we have a single handle in the return list
		if len(recoveredHandles) != 1 {
			require.Len(t, recoveredHandles, 1, "expected 1 recovered handle, got %d", len(recoveredHandles))
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
		workflows, err := dbosCtx.(*dbosContext).systemDB.listWorkflows(context.Background(), listWorkflowsDBInput{
			workflowIDs: []string{handle1.GetWorkflowID()},
		})
		if err != nil {
			t.Fatalf("failed to list workflows: %v", err)
		}

		if len(workflows) != 1 {
			require.Len(t, workflows, 1, "expected 1 workflow, got %d", len(workflows))
		}

		workflow := workflows[0]

		// Ensure its number of attempts is 2
		if workflow.Attempts != 2 {
			t.Fatalf("expected workflow attempts to be 2, got %d", workflow.Attempts)
		}

		// unlock the workflow & wait for result
		blockingStepStopEvent.Set() // This will allow the blocking step to complete
		result, err := recoveredHandle.GetResult()
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
	deadLetterQueueStartEvent *Event
	deadLetterQueueEvent      *Event
	recoveryCount             int64
)

func deadLetterQueueWorkflow(ctx DBOSContext, input string) (int, error) {
	recoveryCount++
	wfid, err := GetWorkflowID(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get workflow ID: %v", err)
	}
	fmt.Printf("Dead letter queue workflow %s started, recovery count: %d\n", wfid, recoveryCount)
	deadLetterQueueStartEvent.Set()
	deadLetterQueueEvent.Wait()
	return 0, nil
}

func infiniteDeadLetterQueueWorkflow(ctx DBOSContext, input string) (int, error) {
	deadLetterQueueStartEvent.Set()
	deadLetterQueueEvent.Wait()
	return 0, nil
}
func TestWorkflowDeadLetterQueue(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)
	RegisterWorkflow(dbosCtx, deadLetterQueueWorkflow, WithMaxRetries(maxRecoveryAttempts))
	RegisterWorkflow(dbosCtx, infiniteDeadLetterQueueWorkflow, WithMaxRetries(-1)) // A negative value means infinite retries

	t.Run("DeadLetterQueueBehavior", func(t *testing.T) {
		deadLetterQueueEvent = NewEvent()
		deadLetterQueueStartEvent = NewEvent()
		recoveryCount = 0

		// Start a workflow that blocks forever
		wfID := uuid.NewString()
		handle, err := RunAsWorkflow(dbosCtx, deadLetterQueueWorkflow, "test", WithWorkflowID(wfID))
		if err != nil {
			t.Fatalf("failed to start dead letter queue workflow: %v", err)
		}
		deadLetterQueueStartEvent.Wait()
		deadLetterQueueStartEvent.Clear()

		// Attempt to recover the blocked workflow the maximum number of times
		for i := range maxRecoveryAttempts {
			_, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
			require.NoError(t, err, "failed to recover pending workflows on attempt %d", i+1)
			deadLetterQueueStartEvent.Wait()
			deadLetterQueueStartEvent.Clear()
			expectedCount := int64(i + 2) // +1 for initial execution, +1 for each recovery
			require.Equal(t, expectedCount, recoveryCount, "expected recovery count to be %d, got %d", expectedCount, recoveryCount)
		}

		// Verify an additional attempt throws a DLQ error and puts the workflow in the DLQ status
		_, err = recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.Error(t, err, "expected dead letter queue error but got none")

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
		_, err = RunAsWorkflow(dbosCtx, deadLetterQueueWorkflow, "test", WithWorkflowID(wfID))
		require.Error(t, err, "expected dead letter queue error when restarting workflow with same ID but got none")

		dbosErr, ok = err.(*DBOSError)
		require.True(t, ok, "expected error to be of type *DBOSError, got %T", err)
		require.Equal(t, dbosErr.Code, DeadLetterQueueError, "expected error code to be DeadLetterQueueError")

		// Now resume the workflow -- this clears the DLQ status
		resumedHandle, err := ResumeWorkflow[int](dbosCtx, wfID)
		require.NoError(t, err, "failed to resume workflow")

		// Recover pending workflows again - should work without error
		_, err = recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed to recover pending workflows after resume")

		// Complete the blocked workflow
		deadLetterQueueEvent.Set()

		// Wait for both handles to complete
		result1, err := handle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from original handle: %v", err)
		}

		result2, err := resumedHandle.GetResult()
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
			_, err = RunAsWorkflow(dbosCtx, deadLetterQueueWorkflow, "test", WithWorkflowID(wfID))
			if err != nil {
				t.Fatalf("unexpected error when retrying completed workflow: %v", err)
			}
		}
	})

	t.Run("InfiniteRetriesWorkflow", func(t *testing.T) {
		deadLetterQueueEvent = NewEvent()
		deadLetterQueueStartEvent = NewEvent()

		// Verify that a workflow with MaxRetries=0 (infinite retries) is retried infinitely
		wfID := uuid.NewString()

		handle, err := RunAsWorkflow(dbosCtx, infiniteDeadLetterQueueWorkflow, "test", WithWorkflowID(wfID))
		if err != nil {
			t.Fatalf("failed to start infinite dead letter queue workflow: %v", err)
		}

		deadLetterQueueStartEvent.Wait()
		deadLetterQueueStartEvent.Clear()
		// Attempt to recover the blocked workflow many times (should never fail)
		handles := []WorkflowHandle[any]{}
		for i := range _DEFAULT_MAX_RECOVERY_ATTEMPTS * 2 {
			recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
			if err != nil {
				t.Fatalf("failed to recover pending workflows on attempt %d: %v", i+1, err)
			}
			handles = append(handles, recoveredHandles...)
			deadLetterQueueStartEvent.Wait()
			deadLetterQueueStartEvent.Clear()
		}

		// Complete the workflow
		deadLetterQueueEvent.Set()

		result, err := handle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from infinite dead letter queue workflow: %v", err)
		}
		if result != 0 {
			t.Fatalf("expected result to be 0, got %v", result)
		}

		// Wait for all handles to complete
		for i, h := range handles {
			result, err := h.GetResult()
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
)

func TestScheduledWorkflows(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)

	RegisterWorkflow(dbosCtx, func(ctx DBOSContext, scheduledTime time.Time) (string, error) {
		startTime := time.Now()
		counter++
		if counter == 10 {
			return "", fmt.Errorf("counter reached 10, stopping workflow")
		}
		select {
		case counter1Ch <- startTime:
		default:
		}
		return fmt.Sprintf("Scheduled workflow scheduled at time %v and executed at time %v", scheduledTime, startTime), nil
	}, WithSchedule("* * * * * *")) // Every second

	err := dbosCtx.Launch()
	if err != nil {
		t.Fatalf("failed to launch DBOS: %v", err)
	}

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
				t.Fatalf("Execution %d timing deviation too large: expected around %v, got %v (delta: %v, allowed slack: %v)", i+1, expectedTime, execTime, delta, allowedSlack)
			}

			t.Logf("Execution %d: expected %v, actual %v, delta %v", i+1, expectedTime, execTime, delta)
		}

		// Stop the workflowScheduler and check if it stops executing
		dbosCtx.(*dbosContext).getWorkflowScheduler().Stop()
		time.Sleep(3 * time.Second) // Wait a bit to ensure no more executions
		currentCounter := counter   // If more scheduled executions happen, this can also trigger a data race. If the scheduler is correct, there should be no race.
		if counter >= currentCounter+2 {
			t.Fatalf("Scheduled workflow continued executing after stopping scheduler: %d (expected < %d)", counter, currentCounter+2)
		}
	})
}

var (
	sendIdempotencyEvent         = NewEvent()
	receiveIdempotencyStartEvent = NewEvent()
	receiveIdempotencyStopEvent  = NewEvent()
	numConcurrentRecvWfs         = 5
	concurrentRecvReadyEvents    = make([]*Event, numConcurrentRecvWfs)
	concurrentRecvStartEvent     = NewEvent()
)

type sendWorkflowInput struct {
	DestinationID string
	Topic         string
}

func sendWorkflow(ctx DBOSContext, input sendWorkflowInput) (string, error) {
	err := Send(ctx, GenericWorkflowSendInput[string]{
		DestinationID: input.DestinationID,
		Topic:         input.Topic,
		Message:       "message1",
	})
	if err != nil {
		return "", err
	}
	err = Send(ctx, GenericWorkflowSendInput[string]{DestinationID: input.DestinationID, Topic: input.Topic, Message: "message2"})
	if err != nil {
		return "", err
	}
	err = Send(ctx, GenericWorkflowSendInput[string]{DestinationID: input.DestinationID, Topic: input.Topic, Message: "message3"})
	if err != nil {
		return "", err
	}
	return "", nil
}

func receiveWorkflow(ctx DBOSContext, topic string) (string, error) {
	msg1, err := Recv[string](ctx, WorkflowRecvInput{Topic: topic, Timeout: 10 * time.Second})
	if err != nil {
		return "", err
	}
	msg2, err := Recv[string](ctx, WorkflowRecvInput{Topic: topic, Timeout: 10 * time.Second})
	if err != nil {
		return "", err
	}
	msg3, err := Recv[string](ctx, WorkflowRecvInput{Topic: topic, Timeout: 10 * time.Second})
	if err != nil {
		return "", err
	}
	return msg1 + "-" + msg2 + "-" + msg3, nil
}

func receiveWorkflowCoordinated(ctx DBOSContext, input struct {
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

func sendStructWorkflow(ctx DBOSContext, input sendWorkflowInput) (string, error) {
	testStruct := sendRecvType{Value: "test-struct-value"}
	err := Send(ctx, GenericWorkflowSendInput[sendRecvType]{DestinationID: input.DestinationID, Topic: input.Topic, Message: testStruct})
	return "", err
}

func receiveStructWorkflow(ctx DBOSContext, topic string) (sendRecvType, error) {
	return Recv[sendRecvType](ctx, WorkflowRecvInput{Topic: topic, Timeout: 3 * time.Second})
}

func sendIdempotencyWorkflow(ctx DBOSContext, input sendWorkflowInput) (string, error) {
	err := Send(ctx, GenericWorkflowSendInput[string]{DestinationID: input.DestinationID, Topic: input.Topic, Message: "m1"})
	if err != nil {
		return "", err
	}
	sendIdempotencyEvent.Wait()
	return "idempotent-send-completed", nil
}

func receiveIdempotencyWorkflow(ctx DBOSContext, topic string) (string, error) {
	msg, err := Recv[string](ctx, WorkflowRecvInput{Topic: topic, Timeout: 3 * time.Second})
	if err != nil {
		// Unlock the test in this case
		receiveIdempotencyStartEvent.Set()
		return "", err
	}
	receiveIdempotencyStartEvent.Set()
	receiveIdempotencyStopEvent.Wait()
	return msg, nil
}

func stepThatCallsSend(ctx context.Context, input sendWorkflowInput) (string, error) {
	err := Send(ctx.(DBOSContext), GenericWorkflowSendInput[string]{
		DestinationID: input.DestinationID,
		Topic:         input.Topic,
		Message:       "message-from-step",
	})
	if err != nil {
		return "", err
	}
	return "send-completed", nil
}

func workflowThatCallsSendInStep(ctx DBOSContext, input sendWorkflowInput) (string, error) {
	return RunAsStep(ctx, func(context context.Context) (string, error) {
		return stepThatCallsSend(context, input)
	})
}

type sendRecvType struct {
	Value string
}

func TestSendRecv(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)

	// Register all send/recv workflows with executor
	RegisterWorkflow(dbosCtx, sendWorkflow)
	RegisterWorkflow(dbosCtx, receiveWorkflow)
	RegisterWorkflow(dbosCtx, receiveWorkflowCoordinated)
	RegisterWorkflow(dbosCtx, sendStructWorkflow)
	RegisterWorkflow(dbosCtx, receiveStructWorkflow)
	RegisterWorkflow(dbosCtx, sendIdempotencyWorkflow)
	RegisterWorkflow(dbosCtx, receiveIdempotencyWorkflow)
	RegisterWorkflow(dbosCtx, workflowThatCallsSendInStep)

	t.Run("SendRecvSuccess", func(t *testing.T) {
		// Start the receive workflow
		receiveHandle, err := RunAsWorkflow(dbosCtx, receiveWorkflow, "test-topic")
		if err != nil {
			t.Fatalf("failed to start receive workflow: %v", err)
		}

		// Send a message to the receive workflow
		handle, err := RunAsWorkflow(dbosCtx, sendWorkflow, sendWorkflowInput{
			DestinationID: receiveHandle.GetWorkflowID(),
			Topic:         "test-topic",
		})
		if err != nil {
			t.Fatalf("failed to send message: %v", err)
		}
		_, err = handle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from send workflow: %v", err)
		}

		start := time.Now()
		result, err := receiveHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from receive workflow: %v", err)
		}
		if result != "message1-message2-message3" {
			t.Fatalf("expected received message to be 'message1-message2-message3', got '%s'", result)
		}
		// XXX This is not a great condition: when all the tests run there's quite some randomness to this
		if time.Since(start) > 10*time.Second {
			t.Fatalf("receive workflow took too long to complete, expected < 5s, got %v", time.Since(start))
		}

		// Verify step counting for send workflow (sendWorkflow calls Send 3 times)
		sendSteps, err := dbosCtx.(*dbosContext).systemDB.getWorkflowSteps(dbosCtx, handle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to get workflow steps for send workflow: %v", err)
		}
		if len(sendSteps) != 3 {
			require.Len(t, sendSteps, 3, "expected 3 steps in send workflow (3 Send calls), got %d", len(sendSteps))
		}
		for i, step := range sendSteps {
			if step.StepID != i {
				t.Fatalf("expected step %d to have StepID %d, got %d", i, step.StepID, i)
			}
			if step.StepName != "DBOS.send" {
				t.Fatalf("expected step %d to have StepName 'DBOS.send', got '%s'", i, step.StepName)
			}
		}

		// Verify step counting for receive workflow (receiveWorkflow calls Recv 3 times)
		receiveSteps, err := dbosCtx.(*dbosContext).systemDB.getWorkflowSteps(dbosCtx, receiveHandle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to get workflow steps for receive workflow: %v", err)
		}
		if len(receiveSteps) != 3 {
			require.Len(t, receiveSteps, 3, "expected 3 steps in receive workflow (3 Recv calls), got %d", len(receiveSteps))
		}
		for i, step := range receiveSteps {
			if step.StepID != i {
				t.Fatalf("expected step %d to have StepID %d, got %d", i, step.StepID, i)
			}
			if step.StepName != "DBOS.recv" {
				t.Fatalf("expected step %d to have StepName 'DBOS.recv', got '%s'", i, step.StepName)
			}
		}
	})

	t.Run("SendRecvCustomStruct", func(t *testing.T) {
		// Start the receive workflow
		receiveHandle, err := RunAsWorkflow(dbosCtx, receiveStructWorkflow, "struct-topic")
		if err != nil {
			t.Fatalf("failed to start receive workflow: %v", err)
		}

		// Send the struct to the receive workflow
		sendHandle, err := RunAsWorkflow(dbosCtx, sendStructWorkflow, sendWorkflowInput{
			DestinationID: receiveHandle.GetWorkflowID(),
			Topic:         "struct-topic",
		})
		if err != nil {
			t.Fatalf("failed to send struct: %v", err)
		}

		_, err = sendHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from send workflow: %v", err)
		}

		// Get the result from receive workflow
		result, err := receiveHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from receive workflow: %v", err)
		}

		// Verify the struct was received correctly
		if result.Value != "test-struct-value" {
			t.Fatalf("expected received struct value to be 'test-struct-value', got '%s'", result.Value)
		}

		// Verify step counting for sendStructWorkflow (calls Send 1 time)
		sendSteps, err := dbosCtx.(*dbosContext).systemDB.getWorkflowSteps(dbosCtx, sendHandle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to get workflow steps for send struct workflow: %v", err)
		}
		if len(sendSteps) != 1 {
			require.Len(t, sendSteps, 1, "expected 1 step in send struct workflow (1 Send call), got %d", len(sendSteps))
		}
		if sendSteps[0].StepID != 0 {
			t.Fatalf("expected step to have StepID 0, got %d", sendSteps[0].StepID)
		}
		if sendSteps[0].StepName != "DBOS.send" {
			t.Fatalf("expected step to have StepName 'DBOS.send', got '%s'", sendSteps[0].StepName)
		}

		// Verify step counting for receiveStructWorkflow (calls Recv 1 time)
		receiveSteps, err := dbosCtx.(*dbosContext).systemDB.getWorkflowSteps(dbosCtx, receiveHandle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to get workflow steps for receive struct workflow: %v", err)
		}
		if len(receiveSteps) != 1 {
			require.Len(t, receiveSteps, 1, "expected 1 step in receive struct workflow (1 Recv call), got %d", len(receiveSteps))
		}
		if receiveSteps[0].StepID != 0 {
			t.Fatalf("expected step to have StepID 0, got %d", receiveSteps[0].StepID)
		}
		if receiveSteps[0].StepName != "DBOS.recv" {
			t.Fatalf("expected step to have StepName 'DBOS.recv', got '%s'", receiveSteps[0].StepName)
		}
	})

	t.Run("SendToNonExistentUUID", func(t *testing.T) {
		// Generate a non-existent UUID
		destUUID := uuid.NewString()

		// Send to non-existent UUID should fail
		handle, err := RunAsWorkflow(dbosCtx, sendWorkflow, sendWorkflowInput{
			DestinationID: destUUID,
			Topic:         "testtopic",
		})
		if err != nil {
			t.Fatalf("failed to start send workflow: %v", err)
		}

		_, err = handle.GetResult()
		require.Error(t, err, "expected error when sending to non-existent UUID but got none")

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
		receiveHandle, err := RunAsWorkflow(dbosCtx, receiveWorkflow, "timeout-test-topic")
		if err != nil {
			t.Fatalf("failed to start receive workflow: %v", err)
		}
		result, err := receiveHandle.GetResult()
		if result != "--" {
			t.Fatalf("expected -- result on timeout, got '%s'", result)
		}
		if err != nil {
			t.Fatalf("expected no error on timeout, but got: %v", err)
		}
	})

	t.Run("RecvMustRunInsideWorkflows", func(t *testing.T) {
		// Attempt to run Recv outside of a workflow context
		_, err := Recv[string](dbosCtx, WorkflowRecvInput{Topic: "test-topic", Timeout: 1 * time.Second})
		require.Error(t, err, "expected error when running Recv outside of workflow context, but got none")

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
		receiveHandle, err := RunAsWorkflow(dbosCtx, receiveWorkflow, "outside-workflow-topic")
		if err != nil {
			t.Fatalf("failed to start receive workflow: %v", err)
		}

		// Send messages from outside a workflow context
		for i := range 3 {
			err = Send(dbosCtx, GenericWorkflowSendInput[string]{
				DestinationID: receiveHandle.GetWorkflowID(),
				Topic:         "outside-workflow-topic",
				Message:       fmt.Sprintf("message%d", i+1),
			})
			if err != nil {
				t.Fatalf("failed to send message%d from outside workflow: %v", i+1, err)
			}
		}

		// Verify the receive workflow gets all messages
		result, err := receiveHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from receive workflow: %v", err)
		}
		if result != "message1-message2-message3" {
			t.Fatalf("expected result to be 'message1-message2-message3', got '%s'", result)
		}

		// Verify step counting for receive workflow (calls Recv 3 times)
		receiveSteps, err := dbosCtx.(*dbosContext).systemDB.getWorkflowSteps(dbosCtx, receiveHandle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to get workflow steps for receive workflow: %v", err)
		}
		if len(receiveSteps) != 3 {
			require.Len(t, receiveSteps, 3, "expected 3 steps in receive workflow (3 Recv calls), got %d", len(receiveSteps))
		}
		for i, step := range receiveSteps {
			if step.StepID != i {
				t.Fatalf("expected step %d to have StepID %d, got %d", i, step.StepID, i)
			}
			if step.StepName != "DBOS.recv" {
				t.Fatalf("expected step %d to have StepName 'DBOS.recv', got '%s'", i, step.StepName)
			}
		}
	})
	t.Run("SendRecvIdempotency", func(t *testing.T) {
		// Start the receive workflow and wait for it to be ready
		receiveHandle, err := RunAsWorkflow(dbosCtx, receiveIdempotencyWorkflow, "idempotency-topic")
		if err != nil {
			t.Fatalf("failed to start receive idempotency workflow: %v", err)
		}

		// Send the message to the receive workflow
		sendHandle, err := RunAsWorkflow(dbosCtx, sendIdempotencyWorkflow, sendWorkflowInput{
			DestinationID: receiveHandle.GetWorkflowID(),
			Topic:         "idempotency-topic",
		})
		if err != nil {
			t.Fatalf("failed to send idempotency message: %v", err)
		}

		// Wait for the receive workflow to have received the message
		receiveIdempotencyStartEvent.Wait()

		// Attempt recovering both workflows. There should be only 2 steps recorded after recovery.
		recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		if err != nil {
			t.Fatalf("failed to recover pending workflows: %v", err)
		}
		if len(recoveredHandles) != 2 {
			require.Len(t, recoveredHandles, 2, "expected 2 recovered handles, got %d", len(recoveredHandles))
		}
		steps, err := dbosCtx.(*dbosContext).systemDB.getWorkflowSteps(dbosCtx, sendHandle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to get workflow steps: %v", err)
		}
		if len(steps) != 1 {
			require.Len(t, steps, 1, "expected 1 step in send idempotency workflow, got %d", len(steps))
		}
		if steps[0].StepID != 0 {
			t.Fatalf("expected send idempotency step to have StepID 0, got %d", steps[0].StepID)
		}
		if steps[0].StepName != "DBOS.send" {
			t.Fatalf("expected send idempotency step to have StepName 'DBOS.send', got '%s'", steps[0].StepName)
		}
		steps, err = dbosCtx.(*dbosContext).systemDB.getWorkflowSteps(dbosCtx, receiveHandle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to get steps for receive idempotency workflow: %v", err)
		}
		if len(steps) != 1 {
			require.Len(t, steps, 1, "expected 1 step in receive idempotency workflow, got %d", len(steps))
		}
		if steps[0].StepID != 0 {
			t.Fatalf("expected receive idempotency step to have StepID 0, got %d", steps[0].StepID)
		}
		if steps[0].StepName != "DBOS.recv" {
			t.Fatalf("expected receive idempotency step to have StepName 'DBOS.recv', got '%s'", steps[0].StepName)
		}

		// Unblock the workflows to complete
		receiveIdempotencyStopEvent.Set()
		result, err := receiveHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from receive idempotency workflow: %v", err)
		}
		if result != "m1" {
			t.Fatalf("expected result to be 'm1', got '%s'", result)
		}
		sendIdempotencyEvent.Set()
		result, err = sendHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from send idempotency workflow: %v", err)
		}
		if result != "idempotent-send-completed" {
			t.Fatalf("expected result to be 'idempotent-send-completed', got '%s'", result)
		}
	})

	t.Run("SendCannotBeCalledWithinStep", func(t *testing.T) {
		// Start a receive workflow to have a valid destination
		receiveHandle, err := RunAsWorkflow(dbosCtx, receiveWorkflow, "send-within-step-topic")
		if err != nil {
			t.Fatalf("failed to start receive workflow: %v", err)
		}

		// Execute the workflow that tries to call Send within a step
		handle, err := RunAsWorkflow(dbosCtx, workflowThatCallsSendInStep, sendWorkflowInput{
			DestinationID: receiveHandle.GetWorkflowID(),
			Topic:         "send-within-step-topic",
		})
		if err != nil {
			t.Fatalf("failed to start workflow: %v", err)
		}

		// Expect the workflow to fail with the specific error
		_, err = handle.GetResult()
		require.Error(t, err, "expected error when calling Send within a step, but got none")

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
		result, err := receiveHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from receive workflow: %v", err)
		}
		if result != "--" {
			t.Fatalf("expected receive workflow result to be '--' (timeout), got '%s'", result)
		}
	})

	t.Run("TestSendRecv", func(t *testing.T) {
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
			receiveHandle, err := RunAsWorkflow(dbosCtx, receiveWorkflowCoordinated, struct {
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
				result, err := receiverHandles[index].GetResult()
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
	setEventStartIdempotencyEvent = NewEvent()
	setEvenStopIdempotencyEvent   = NewEvent()
	getEventStartIdempotencyEvent = NewEvent()
	getEventStopIdempotencyEvent  = NewEvent()
	setSecondEventSignal          = NewEvent()
)

type setEventWorkflowInput struct {
	Key     string
	Message string
}

func setEventWorkflow(ctx DBOSContext, input setEventWorkflowInput) (string, error) {
	err := SetEvent(ctx, GenericWorkflowSetEventInput[string](input))
	if err != nil {
		return "", err
	}
	return "event-set", nil
}

func getEventWorkflow(ctx DBOSContext, input setEventWorkflowInput) (string, error) {
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

func setTwoEventsWorkflow(ctx DBOSContext, input setEventWorkflowInput) (string, error) {
	// Set the first event
	err := SetEvent(ctx, GenericWorkflowSetEventInput[string]{Key: "event1", Message: "first-event-message"})
	if err != nil {
		return "", err
	}

	// Wait for external signal before setting the second event
	setSecondEventSignal.Wait()

	// Set the second event
	err = SetEvent(ctx, GenericWorkflowSetEventInput[string]{Key: "event2", Message: "second-event-message"})
	if err != nil {
		return "", err
	}

	return "two-events-set", nil
}

func setEventIdempotencyWorkflow(ctx DBOSContext, input setEventWorkflowInput) (string, error) {
	err := SetEvent(ctx, GenericWorkflowSetEventInput[string](input))
	if err != nil {
		return "", err
	}
	setEventStartIdempotencyEvent.Set()
	setEvenStopIdempotencyEvent.Wait()
	return "idempotent-set-completed", nil
}

func getEventIdempotencyWorkflow(ctx DBOSContext, input setEventWorkflowInput) (string, error) {
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

// Test workflows and steps for parameter mismatch validation
func conflictWorkflowA(dbosCtx DBOSContext, input string) (string, error) {
	return RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return conflictStepA(ctx)
	})
}

func conflictWorkflowB(dbosCtx DBOSContext, input string) (string, error) {
	return RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return conflictStepB(ctx)
	})
}

func conflictStepA(_ context.Context) (string, error) {
	return "step-a-result", nil
}

func conflictStepB(_ context.Context) (string, error) {
	return "step-b-result", nil
}

func workflowWithMultipleSteps(dbosCtx DBOSContext, input string) (string, error) {
	// First step
	result1, err := RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return conflictStepA(ctx)
	})
	if err != nil {
		return "", err
	}

	// Second step - this is where we'll test step name conflicts
	result2, err := RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return conflictStepB(ctx)
	})
	if err != nil {
		return "", err
	}

	return result1 + "-" + result2, nil
}

func TestWorkflowExecutionMismatch(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)

	// Register workflows for testing
	RegisterWorkflow(dbosCtx, conflictWorkflowA)
	RegisterWorkflow(dbosCtx, conflictWorkflowB)
	RegisterWorkflow(dbosCtx, workflowWithMultipleSteps)

	t.Run("WorkflowNameConflict", func(t *testing.T) {
		workflowID := uuid.NewString()

		// First, run conflictWorkflowA with a specific workflow ID
		handle, err := RunAsWorkflow(dbosCtx, conflictWorkflowA, "test-input", WithWorkflowID(workflowID))
		if err != nil {
			t.Fatalf("failed to start first workflow: %v", err)
		}

		// Get the result to ensure it completes
		result, err := handle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from first workflow: %v", err)
		}
		if result != "step-a-result" {
			t.Fatalf("expected 'step-a-result', got '%s'", result)
		}

		// Now try to run conflictWorkflowB with the same workflow ID
		// This should return a ConflictingWorkflowError
		_, err = RunAsWorkflow(dbosCtx, conflictWorkflowB, "test-input", WithWorkflowID(workflowID))
		require.Error(t, err, "expected ConflictingWorkflowError when running different workflow with same ID, but got none")

		// Check that it's the correct error type
		dbosErr, ok := err.(*DBOSError)
		if !ok {
			t.Fatalf("expected error to be of type *DBOSError, got %T", err)
		}

		if dbosErr.Code != ConflictingWorkflowError {
			t.Fatalf("expected error code to be ConflictingWorkflowError, got %v", dbosErr.Code)
		}

		// Check that the error message contains the workflow names
		expectedMsgPart := "Workflow already exists with a different name"
		if !strings.Contains(err.Error(), expectedMsgPart) {
			t.Fatalf("expected error message to contain '%s', got '%s'", expectedMsgPart, err.Error())
		}
	})

	t.Run("StepNameConflict", func(t *testing.T) {
		handle, err := RunAsWorkflow(dbosCtx, workflowWithMultipleSteps, "test-input")
		if err != nil {
			t.Fatalf("failed to start workflow: %v", err)
		}
		result, err := handle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from workflow: %v", err)
		}
		if result != "step-a-result-step-b-result" {
			t.Fatalf("expected 'step-a-result-step-b-result', got '%s'", result)
		}

		// Check operation execution with a different step name for the same step ID
		workflowID := handle.GetWorkflowID()

		// This directly tests the CheckOperationExecution method with mismatched step name
		wrongStepName := "wrong-step-name"
		_, err = dbosCtx.(*dbosContext).systemDB.checkOperationExecution(dbosCtx, checkOperationExecutionDBInput{
			workflowID: workflowID,
			stepID:     0,
			stepName:   wrongStepName,
		})

		require.Error(t, err, "expected UnexpectedStep error when checking operation with wrong step name, but got none")

		// Check that it's the correct error type
		dbosErr, ok := err.(*DBOSError)
		if !ok {
			t.Fatalf("expected error to be of type *DBOSError, got %T", err)
		}

		if dbosErr.Code != UnexpectedStep {
			t.Fatalf("expected error code to be UnexpectedStep, got %v", dbosErr.Code)
		}

		// Check that the error message contains step information
		if !strings.Contains(err.Error(), "Check that your workflow is deterministic") {
			t.Fatalf("expected error message to contain 'Check that your workflow is deterministic', got '%s'", err.Error())
		}
		if !strings.Contains(err.Error(), wrongStepName) {
			t.Fatalf("expected error message to contain wrong step name '%s', got '%s'", wrongStepName, err.Error())
		}
	})
}

func TestSetGetEvent(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)

	// Register all set/get event workflows with executor
	RegisterWorkflow(dbosCtx, setEventWorkflow)
	RegisterWorkflow(dbosCtx, getEventWorkflow)
	RegisterWorkflow(dbosCtx, setTwoEventsWorkflow)
	RegisterWorkflow(dbosCtx, setEventIdempotencyWorkflow)
	RegisterWorkflow(dbosCtx, getEventIdempotencyWorkflow)

	t.Run("SetGetEventFromWorkflow", func(t *testing.T) {
		// Clear the signal event before starting
		setSecondEventSignal.Clear()

		// Start the workflow that sets two events
		setHandle, err := RunAsWorkflow(dbosCtx, setTwoEventsWorkflow, setEventWorkflowInput{
			Key:     "test-workflow",
			Message: "unused",
		})
		if err != nil {
			t.Fatalf("failed to start set two events workflow: %v", err)
		}

		// Start a workflow to get the first event
		getFirstEventHandle, err := RunAsWorkflow(dbosCtx, getEventWorkflow, setEventWorkflowInput{
			Key:     setHandle.GetWorkflowID(), // Target workflow ID
			Message: "event1",                  // Event key
		})
		if err != nil {
			t.Fatalf("failed to start get first event workflow: %v", err)
		}

		// Verify we can get the first event
		firstMessage, err := getFirstEventHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from first event workflow: %v", err)
		}
		if firstMessage != "first-event-message" {
			t.Fatalf("expected first message to be 'first-event-message', got '%s'", firstMessage)
		}

		// Signal the workflow to set the second event
		setSecondEventSignal.Set()

		// Start a workflow to get the second event
		getSecondEventHandle, err := RunAsWorkflow(dbosCtx, getEventWorkflow, setEventWorkflowInput{
			Key:     setHandle.GetWorkflowID(), // Target workflow ID
			Message: "event2",                  // Event key
		})
		if err != nil {
			t.Fatalf("failed to start get second event workflow: %v", err)
		}

		// Verify we can get the second event
		secondMessage, err := getSecondEventHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from second event workflow: %v", err)
		}
		if secondMessage != "second-event-message" {
			t.Fatalf("expected second message to be 'second-event-message', got '%s'", secondMessage)
		}

		// Wait for the workflow to complete
		result, err := setHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from set two events workflow: %v", err)
		}
		if result != "two-events-set" {
			t.Fatalf("expected result to be 'two-events-set', got '%s'", result)
		}

		// Verify step counting for setTwoEventsWorkflow (calls SetEvent 2 times)
		setSteps, err := dbosCtx.(*dbosContext).systemDB.getWorkflowSteps(dbosCtx, setHandle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to get workflow steps for set two events workflow: %v", err)
		}
		if len(setSteps) != 2 {
			require.Len(t, setSteps, 2, "expected 2 steps in set two events workflow (2 SetEvent calls), got %d", len(setSteps))
		}
		for i, step := range setSteps {
			if step.StepID != i {
				t.Fatalf("expected step %d to have StepID %d, got %d", i, step.StepID, i)
			}
			if step.StepName != "DBOS.setEvent" {
				t.Fatalf("expected step %d to have StepName 'DBOS.setEvent', got '%s'", i, step.StepName)
			}
		}

		// Verify step counting for getFirstEventHandle (calls GetEvent 1 time)
		getFirstSteps, err := dbosCtx.(*dbosContext).systemDB.getWorkflowSteps(dbosCtx, getFirstEventHandle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to get workflow steps for get first event workflow: %v", err)
		}
		if len(getFirstSteps) != 1 {
			require.Len(t, getFirstSteps, 1, "expected 1 step in get first event workflow (1 GetEvent call), got %d", len(getFirstSteps))
		}
		if getFirstSteps[0].StepID != 0 {
			t.Fatalf("expected step to have StepID 0, got %d", getFirstSteps[0].StepID)
		}
		if getFirstSteps[0].StepName != "DBOS.getEvent" {
			t.Fatalf("expected step to have StepName 'DBOS.getEvent', got '%s'", getFirstSteps[0].StepName)
		}

		// Verify step counting for getSecondEventHandle (calls GetEvent 1 time)
		getSecondSteps, err := dbosCtx.(*dbosContext).systemDB.getWorkflowSteps(dbosCtx, getSecondEventHandle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to get workflow steps for get second event workflow: %v", err)
		}
		if len(getSecondSteps) != 1 {
			require.Len(t, getSecondSteps, 1, "expected 1 step in get second event workflow (1 GetEvent call), got %d", len(getSecondSteps))
		}
		if getSecondSteps[0].StepID != 0 {
			t.Fatalf("expected step to have StepID 0, got %d", getSecondSteps[0].StepID)
		}
		if getSecondSteps[0].StepName != "DBOS.getEvent" {
			t.Fatalf("expected step to have StepName 'DBOS.getEvent', got '%s'", getSecondSteps[0].StepName)
		}
	})

	t.Run("GetEventFromOutsideWorkflow", func(t *testing.T) {
		// Start a workflow that sets an event
		setHandle, err := RunAsWorkflow(dbosCtx, setEventWorkflow, setEventWorkflowInput{
			Key:     "test-key",
			Message: "test-message",
		})
		if err != nil {
			t.Fatalf("failed to start set event workflow: %v", err)
		}

		// Wait for the event to be set
		_, err = setHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from set event workflow: %v", err)
		}

		// Start a workflow that gets the event from outside the original workflow
		message, err := GetEvent[string](dbosCtx, WorkflowGetEventInput{
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

		// Verify step counting for setEventWorkflow (calls SetEvent 1 time)
		setSteps, err := dbosCtx.(*dbosContext).systemDB.getWorkflowSteps(dbosCtx, setHandle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to get workflow steps for set event workflow: %v", err)
		}
		if len(setSteps) != 1 {
			require.Len(t, setSteps, 1, "expected 1 step in set event workflow (1 SetEvent call), got %d", len(setSteps))
		}
		if setSteps[0].StepID != 0 {
			t.Fatalf("expected step to have StepID 0, got %d", setSteps[0].StepID)
		}
		if setSteps[0].StepName != "DBOS.setEvent" {
			t.Fatalf("expected step to have StepName 'DBOS.setEvent', got '%s'", setSteps[0].StepName)
		}
	})

	t.Run("GetEventTimeout", func(t *testing.T) {
		// Try to get an event from a non-existent workflow
		nonExistentID := uuid.NewString()
		message, err := GetEvent[string](dbosCtx, WorkflowGetEventInput{
			TargetWorkflowID: nonExistentID,
			Key:              "test-key",
			Timeout:          3 * time.Second,
		})
		require.NoError(t, err, "failed to get event from non-existent workflow")
		if message != "" {
			t.Fatalf("expected empty result on timeout, got '%s'", message)
		}

		// Try to get an event from an existing workflow but with a key that doesn't exist
		setHandle, err := RunAsWorkflow(dbosCtx, setEventWorkflow, setEventWorkflowInput{
			Key:     "test-key",
			Message: "test-message",
		})
		require.NoError(t, err, "failed to set event")
		_, err = setHandle.GetResult()
		require.NoError(t, err, "failed to get result from set event workflow")
		message, err = GetEvent[string](dbosCtx, WorkflowGetEventInput{
			TargetWorkflowID: setHandle.GetWorkflowID(),
			Key:              "non-existent-key",
			Timeout:          3 * time.Second,
		})
		require.NoError(t, err, "failed to get event with non-existent key")
		if message != "" {
			t.Fatalf("expected empty result on timeout with non-existent key, got '%s'", message)
		}
	})

	t.Run("SetGetEventMustRunInsideWorkflows", func(t *testing.T) {
		// Attempt to run SetEvent outside of a workflow context
		err := SetEvent(dbosCtx, GenericWorkflowSetEventInput[string]{Key: "test-key", Message: "test-message"})
		require.Error(t, err, "expected error when running SetEvent outside of workflow context, but got none")

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
		setHandle, err := RunAsWorkflow(dbosCtx, setEventIdempotencyWorkflow, setEventWorkflowInput{
			Key:     "idempotency-key",
			Message: "idempotency-message",
		})
		if err != nil {
			t.Fatalf("failed to start set event idempotency workflow: %v", err)
		}

		// Start the get event workflow
		getHandle, err := RunAsWorkflow(dbosCtx, getEventIdempotencyWorkflow, setEventWorkflowInput{
			Key:     setHandle.GetWorkflowID(),
			Message: "idempotency-key",
		})
		if err != nil {
			t.Fatalf("failed to start get event idempotency workflow: %v", err)
		}

		// Wait for the workflows to signal it has received the event
		getEventStartIdempotencyEvent.Wait()
		getEventStartIdempotencyEvent.Clear()
		setEventStartIdempotencyEvent.Wait()
		setEventStartIdempotencyEvent.Clear()

		// Attempt recovering both workflows. Each should have exactly 1 step.
		recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		if err != nil {
			t.Fatalf("failed to recover pending workflows: %v", err)
		}
		if len(recoveredHandles) != 2 {
			require.Len(t, recoveredHandles, 2, "expected 2 recovered handles, got %d", len(recoveredHandles))
		}

		getEventStartIdempotencyEvent.Wait()
		setEventStartIdempotencyEvent.Wait()

		// Verify step counts
		setSteps, err := dbosCtx.(*dbosContext).systemDB.getWorkflowSteps(dbosCtx, setHandle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to get steps for set event idempotency workflow: %v", err)
		}
		if len(setSteps) != 1 {
			require.Len(t, setSteps, 1, "expected 1 step in set event idempotency workflow, got %d", len(setSteps))
		}
		if setSteps[0].StepID != 0 {
			t.Fatalf("expected set event idempotency step to have StepID 0, got %d", setSteps[0].StepID)
		}
		if setSteps[0].StepName != "DBOS.setEvent" {
			t.Fatalf("expected set event idempotency step to have StepName 'DBOS.setEvent', got '%s'", setSteps[0].StepName)
		}

		getSteps, err := dbosCtx.(*dbosContext).systemDB.getWorkflowSteps(dbosCtx, getHandle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to get steps for get event idempotency workflow: %v", err)
		}
		if len(getSteps) != 1 {
			require.Len(t, getSteps, 1, "expected 1 step in get event idempotency workflow, got %d", len(getSteps))
		}
		if getSteps[0].StepID != 0 {
			t.Fatalf("expected get event idempotency step to have StepID 0, got %d", getSteps[0].StepID)
		}
		if getSteps[0].StepName != "DBOS.getEvent" {
			t.Fatalf("expected get event idempotency step to have StepName 'DBOS.getEvent', got '%s'", getSteps[0].StepName)
		}

		// Complete the workflows
		setEvenStopIdempotencyEvent.Set()
		getEventStopIdempotencyEvent.Set()

		setResult, err := setHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from set event idempotency workflow: %v", err)
		}
		if setResult != "idempotent-set-completed" {
			t.Fatalf("expected result to be 'idempotent-set-completed', got '%s'", setResult)
		}

		getResult, err := getHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from get event idempotency workflow: %v", err)
		}
		if getResult != "idempotency-message" {
			t.Fatalf("expected result to be 'idempotency-message', got '%s'", getResult)
		}

		// Check the recovered handle returns the same result
		for _, recoveredHandle := range recoveredHandles {
			if recoveredHandle.GetWorkflowID() == setHandle.GetWorkflowID() {
				recoveredSetResult, err := recoveredHandle.GetResult()
				if err != nil {
					t.Fatalf("failed to get result from recovered set event idempotency workflow: %v", err)
				}
				if recoveredSetResult != "idempotent-set-completed" {
					t.Fatalf("expected recovered result to be 'idempotent-set-completed', got '%s'", recoveredSetResult)

				}
			}
			if recoveredHandle.GetWorkflowID() == getHandle.GetWorkflowID() {
				recoveredGetResult, err := recoveredHandle.GetResult()
				if err != nil {
					t.Fatalf("failed to get result from recovered get event idempotency workflow: %v", err)
				}
				if recoveredGetResult != "idempotency-message" {
					t.Fatalf("expected recovered result to be 'idempotency-message', got '%s'", recoveredGetResult)
				}
			}
		}
	})

	t.Run("ConcurrentGetEvent", func(t *testing.T) {
		// Set event
		setHandle, err := RunAsWorkflow(dbosCtx, setEventWorkflow, setEventWorkflowInput{
			Key:     "concurrent-event-key",
			Message: "concurrent-event-message",
		})
		if err != nil {
			t.Fatalf("failed to start set event workflow: %v", err)
		}

		// Wait for the set event workflow to complete
		_, err = setHandle.GetResult()
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
				res, err := GetEvent[string](dbosCtx, WorkflowGetEventInput{
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
			require.FailNow(t, "goroutine error: %v", err)
		}
	})
}

var (
	sleepStartEvent *Event
	sleepStopEvent  *Event
)

func sleepRecoveryWorkflow(dbosCtx DBOSContext, duration time.Duration) (time.Duration, error) {
	result, err := Sleep(dbosCtx, duration)
	if err != nil {
		return 0, err
	}
	// Block after sleep so we can recover a pending workflow
	sleepStartEvent.Set()
	sleepStopEvent.Wait()
	return result, nil
}

func TestSleep(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)
	RegisterWorkflow(dbosCtx, sleepRecoveryWorkflow)

	t.Run("SleepDurableRecovery", func(t *testing.T) {
		sleepStartEvent = NewEvent()
		sleepStopEvent = NewEvent()

		// Start a workflow that sleeps for 2 seconds then blocks
		sleepDuration := 2 * time.Second

		handle, err := RunAsWorkflow(dbosCtx, sleepRecoveryWorkflow, sleepDuration)
		if err != nil {
			t.Fatalf("failed to start sleep recovery workflow: %v", err)
		}

		sleepStartEvent.Wait()
		sleepStartEvent.Clear()

		// Run the workflow again and check the return time was less than the durable sleep
		startTime := time.Now()
		_, err = RunAsWorkflow(dbosCtx, sleepRecoveryWorkflow, sleepDuration, WithWorkflowID(handle.GetWorkflowID()))
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
		steps, err := dbosCtx.(*dbosContext).systemDB.getWorkflowSteps(dbosCtx, handle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to get workflow steps: %v", err)
		}

		if len(steps) != 1 {
			require.Len(t, steps, 1, "expected 1 step (the sleep), got %d", len(steps))
		}

		step := steps[0]
		if step.StepID != 0 {
			t.Fatalf("expected step to have StepID 0, got %d", step.StepID)
		}
		if step.StepName != "DBOS.sleep" {
			t.Fatalf("expected step name to be 'DBOS.sleep', got '%s'", step.StepName)
		}

		if step.Error != nil {
			t.Fatalf("expected step to have no error, got %v", step.Error)
		}

		sleepStopEvent.Set()

		_, err = handle.GetResult()
		if err != nil {
			t.Fatalf("failed to get sleep workflow result: %v", err)
		}
	})

	t.Run("SleepCannotBeCalledOutsideWorkflow", func(t *testing.T) {
		// Attempt to call Sleep outside of a workflow context
		_, err := Sleep(dbosCtx, 1*time.Second)
		require.Error(t, err, "expected error when calling Sleep outside of workflow context, but got none")

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

func TestWorkflowTimeout(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)

	waitForCancelWorkflow := func(ctx DBOSContext, _ string) (string, error) {
		// This workflow will wait indefinitely until it is cancelled
		<-ctx.Done()
		if !errors.Is(ctx.Err(), context.Canceled) && !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Fatalf("workflow was cancelled, but context error is not context.Canceled nor context.DeadlineExceeded: %v", ctx.Err())
		}
		return "", ctx.Err()
	}
	RegisterWorkflow(dbosCtx, waitForCancelWorkflow)

	t.Run("WorkflowTimeout", func(t *testing.T) {
		// Start a workflow that will wait indefinitely
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, 1*time.Millisecond)
		defer cancelFunc() // Ensure we clean up the context
		handle, err := RunAsWorkflow(cancelCtx, waitForCancelWorkflow, "wait-for-cancel")
		if err != nil {
			t.Fatalf("failed to start wait for cancel workflow: %v", err)
		}

		// Wait for the workflow to complete and get the result
		result, err := handle.GetResult()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Expected deadline exceeded error, got: %v", err)
		}
		if result != "" {
			t.Fatalf("expected result to be an empty string, got '%s'", result)
		}

		// Check the workflow status: should be cancelled
		status, err := handle.GetStatus()
		if err != nil {
			t.Fatalf("failed to get workflow status: %v", err)
		}
		if status.Status != WorkflowStatusCancelled {
			t.Fatalf("expected workflow status to be WorkflowStatusCancelled, got %v", status.Status)
		}
	})

	t.Run("ManuallyCancelWorkflow", func(t *testing.T) {
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, 5*time.Second)
		defer cancelFunc() // Ensure we clean up the context
		handle, err := RunAsWorkflow(cancelCtx, waitForCancelWorkflow, "manual-cancel")
		if err != nil {
			t.Fatalf("failed to start manual cancel workflow: %v", err)
		}

		// Cancel the workflow manually
		cancelFunc()
		result, err := handle.GetResult()
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled error, got: %v", err)
		}
		if result != "" {
			t.Fatalf("expected result to be an empty string, got '%s'", result)
		}

		// Check the workflow status: should be cancelled
		status, err := handle.GetStatus()
		if err != nil {
			t.Fatalf("failed to get workflow status: %v", err)
		}
		if status.Status != WorkflowStatusCancelled {
			t.Fatalf("expected workflow status to be WorkflowStatusCancelled, got %v", status.Status)
		}
	})

	waitForCancelStep := func(ctx context.Context) (string, error) {
		// This step will trigger cancellation of the entire workflow context
		<-ctx.Done()
		if !errors.Is(ctx.Err(), context.Canceled) && !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return "", fmt.Errorf("step was cancelled, but context error is not context.Canceled nor context.DeadlineExceeded: %v", ctx.Err())
		}
		return "", ctx.Err()
	}

	waitForCancelWorkflowWithStep := func(ctx DBOSContext, _ string) (string, error) {
		return RunAsStep(ctx, func(context context.Context) (string, error) {
			return waitForCancelStep(context)
		})
	}
	RegisterWorkflow(dbosCtx, waitForCancelWorkflowWithStep)

	t.Run("WorkflowWithStepTimeout", func(t *testing.T) {
		// Start a workflow that will run a step that triggers cancellation
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, 1*time.Millisecond)
		defer cancelFunc() // Ensure we clean up the context
		handle, err := RunAsWorkflow(cancelCtx, waitForCancelWorkflowWithStep, "wf-with-step-timeout")
		if err != nil {
			t.Fatalf("failed to start workflow with step timeout: %v", err)
		}

		// Wait for the workflow to complete and get the result
		result, err := handle.GetResult()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Expected deadline exceeded error, got: %v", err)
		}
		if result != "" {
			t.Fatalf("expected result to be an empty string, got '%s'", result)
		}

		// Check the workflow status: should be cancelled
		status, err := handle.GetStatus()
		if err != nil {
			t.Fatalf("failed to get workflow status: %v", err)
		}
		if status.Status != WorkflowStatusCancelled {
			t.Fatalf("expected workflow status to be WorkflowStatusCancelled, got %v", status.Status)
		}
	})

	shorterStepTimeoutWorkflow := func(ctx DBOSContext, _ string) (string, error) {
		// This workflow will run a step that has a shorter timeout than the workflow itself
		// The timeout will trigger a step error, the workflow can do whatever it wants with that error
		stepCtx, stepCancelFunc := WithTimeout(ctx, 1*time.Millisecond)
		defer stepCancelFunc() // Ensure we clean up the context
		_, err := RunAsStep(stepCtx, func(context context.Context) (string, error) {
			return waitForCancelStep(context)
		})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected step to timeout, got: %v", err)
		}
		return "step-timed-out", nil
	}
	RegisterWorkflow(dbosCtx, shorterStepTimeoutWorkflow)

	t.Run("ShorterStepTimeout", func(t *testing.T) {
		// Start a workflow that runs a step with a shorter timeout than the workflow itself
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, 5*time.Second)
		defer cancelFunc() // Ensure we clean up the context
		handle, err := RunAsWorkflow(cancelCtx, shorterStepTimeoutWorkflow, "shorter-step-timeout")
		if err != nil {
			t.Fatalf("failed to start shorter step timeout workflow: %v", err)
		}
		// Wait for the workflow to complete and get the result
		result, err := handle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from shorter step timeout workflow: %v", err)
		}
		if result != "step-timed-out" {
			t.Fatalf("expected result to be 'step-timed-out', got '%s'", result)
		}
		// Status is SUCCESS
		status, err := handle.GetStatus()
		if err != nil {
			t.Fatalf("failed to get workflow status: %v", err)
		}
		if status.Status != WorkflowStatusSuccess {
			t.Fatalf("expected workflow status to be WorkflowStatusSuccess, got %v", status.Status)
		}
	})

	detachedStep := func(ctx context.Context, timeout time.Duration) (string, error) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(timeout):
		}
		return "detached-step-completed", nil
	}

	detachedStepWorkflow := func(ctx DBOSContext, timeout time.Duration) (string, error) {
		// This workflow will run a step that is not cancelable.
		// What this means is the workflow *will* be cancelled, but the step will run normally
		stepCtx := WithoutCancel(ctx)
		res, err := RunAsStep(stepCtx, func(context context.Context) (string, error) {
			return detachedStep(context, timeout*2)
		})
		if err != nil {
			t.Fatalf("failed to run detached step: %v", err)
		}
		if res != "detached-step-completed" {
			t.Fatalf("expected detached step result to be 'detached-step-completed', got '%s'", res)
		}
		return res, ctx.Err()
	}
	RegisterWorkflow(dbosCtx, detachedStepWorkflow)

	t.Run("DetachedStepWorkflow", func(t *testing.T) {
		// Start a workflow that runs a step that is not cancelable
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, 1*time.Millisecond)
		defer cancelFunc() // Ensure we clean up the context

		handle, err := RunAsWorkflow(cancelCtx, detachedStepWorkflow, 1*time.Second)
		if err != nil {
			t.Fatalf("failed to start detached step workflow: %v", err)
		}
		// Wait for the workflow to complete and get the result
		result, err := handle.GetResult()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Expected deadline exceeded error, got: %v", err)
		}
		if result != "detached-step-completed" {
			t.Fatalf("expected result to be 'detached-step-completed', got '%s'", result)
		}
		// Check the workflow status: should be cancelled
		status, err := handle.GetStatus()
		if err != nil {
			t.Fatalf("failed to get workflow status: %v", err)
		}
		if status.Status != WorkflowStatusCancelled {
			t.Fatalf("expected workflow status to be WorkflowStatusCancelled, got %v", status.Status)
		}
	})

	waitForCancelParent := func(ctx DBOSContext, _ string) (string, error) {
		// This workflow will run a child workflow that waits indefinitely until it is cancelled
		childHandle, err := RunAsWorkflow(ctx, waitForCancelWorkflow, "child-wait-for-cancel")
		if err != nil {
			t.Fatalf("failed to start child workflow: %v", err)
		}

		// Wait for the child workflow to complete
		result, err := childHandle.GetResult()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected child workflow to be cancelled, got: %v", err)
		}
		// Check the child workflow status: should be cancelled
		status, err := childHandle.GetStatus()
		if err != nil {
			t.Fatalf("failed to get child workflow status: %v", err)
		}
		if status.Status != WorkflowStatusCancelled {
			t.Fatalf("expected child workflow status to be WorkflowStatusCancelled, got %v", status.Status)
		}
		return result, ctx.Err()
	}
	RegisterWorkflow(dbosCtx, waitForCancelParent)

	t.Run("ChildWorkflowTimesout", func(t *testing.T) {
		// Start a parent workflow that runs a child workflow that waits indefinitely
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, 1*time.Millisecond)
		defer cancelFunc() // Ensure we clean up the context

		handle, err := RunAsWorkflow(cancelCtx, waitForCancelParent, "parent-wait-for-child-cancel")
		if err != nil {
			t.Fatalf("failed to start parent workflow: %v", err)
		}

		// Wait for the parent workflow to complete and get the result
		result, err := handle.GetResult()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Expected deadline exceeded error, got: %v", err)
		}
		if result != "" {
			t.Fatalf("expected result to be an empty string, got '%s'", result)
		}

		// Check the workflow status: should be cancelled
		status, err := handle.GetStatus()
		if err != nil {
			t.Fatalf("failed to get workflow status: %v", err)
		}
		if status.Status != WorkflowStatusCancelled {
			t.Fatalf("expected workflow status to be WorkflowStatusCancelled, got %v", status.Status)
		}
	})

	detachedChild := func(ctx DBOSContext, timeout time.Duration) (string, error) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(timeout):
		}
		return "detached-step-completed", nil
	}
	RegisterWorkflow(dbosCtx, detachedChild)

	detachedChildWorkflowParent := func(ctx DBOSContext, timeout time.Duration) (string, error) {
		childCtx := WithoutCancel(ctx)
		childHandle, err := RunAsWorkflow(childCtx, detachedChild, timeout*2)
		if err != nil {
			t.Fatalf("failed to start child workflow: %v", err)
		}

		// Wait for the child workflow to complete
		result, err := childHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from child workflow: %v", err)
		}
		// Check the child workflow status: should be cancelled
		status, err := childHandle.GetStatus()
		if err != nil {
			t.Fatalf("failed to get child workflow status: %v", err)
		}
		if status.Status != WorkflowStatusSuccess {
			t.Fatalf("expected child workflow status to be WorkflowStatusSuccess, got %v", status.Status)
		}
		// The child spun for timeout*2 so ctx.Err() should be context.DeadlineExceeded
		return result, ctx.Err()
	}
	RegisterWorkflow(dbosCtx, detachedChildWorkflowParent)

	t.Run("ChildWorkflowDetached", func(t *testing.T) {
		timeout := 500 * time.Millisecond
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, timeout)
		defer cancelFunc()
		handle, err := RunAsWorkflow(cancelCtx, detachedChildWorkflowParent, timeout)
		if err != nil {
			t.Fatalf("failed to start parent workflow with detached child: %v", err)
		}

		// Wait for the parent workflow to complete and get the result
		result, err := handle.GetResult()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Expected deadline exceeded error, got: %v", err)
		}
		if result != "detached-step-completed" {
			t.Fatalf("expected result to be 'detached-step-completed', got '%s'", result)
		}

		// Check the workflow status: should be cancelled
		status, err := handle.GetStatus()
		if err != nil {
			t.Fatalf("failed to get workflow status: %v", err)
		}
		if status.Status != WorkflowStatusCancelled {
			t.Fatalf("expected workflow status to be WorkflowStatusCancelled, got %v", status.Status)
		}
	})

	t.Run("RecoverWaitForCancelWorkflow", func(t *testing.T) {
		start := time.Now()
		timeout := 1 * time.Second
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, timeout)
		defer cancelFunc()
		handle, err := RunAsWorkflow(cancelCtx, waitForCancelWorkflow, "recover-wait-for-cancel")
		if err != nil {
			t.Fatalf("failed to start wait for cancel workflow: %v", err)
		}

		// Recover the pending workflow
		recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		if err != nil {
			t.Fatalf("failed to recover pending workflows: %v", err)
		}
		if len(recoveredHandles) != 1 {
			require.Len(t, recoveredHandles, 1, "expected 1 recovered handle, got %d", len(recoveredHandles))
		}
		recoveredHandle := recoveredHandles[0]
		if recoveredHandle.GetWorkflowID() != handle.GetWorkflowID() {
			t.Fatalf("expected recovered handle to have ID %s, got %s", handle.GetWorkflowID(), recoveredHandle.GetWorkflowID())
		}

		// Wait for the workflow to complete and check the result. Should we AwaitedWorkflowCancelled
		result, err := recoveredHandle.GetResult()
		if result != "" {
			t.Fatalf("expected result to be an empty string, got '%s'", result)
		}
		// Check the error type
		dbosErr, ok := err.(*DBOSError)
		if !ok {
			t.Fatalf("expected error to be of type *DBOSError, got %T", err)
		}

		if dbosErr.Code != AwaitedWorkflowCancelled {
			t.Fatalf("expected error code to be AwaitedWorkflowCancelled, got %v", dbosErr.Code)
		}

		// Check the workflow status: should be cancelled
		status, err := recoveredHandle.GetStatus()
		if err != nil {
			t.Fatalf("failed to get recovered workflow status: %v", err)
		}
		if status.Status != WorkflowStatusCancelled {
			t.Fatalf("expected recovered workflow status to be WorkflowStatusCancelled, got %v", status.Status)
		}

		// Check the deadline on the status was is within an expected range (start time + timeout * .1)
		// XXX this might be flaky and frankly not super useful
		expectedDeadline := start.Add(timeout * 10 / 100)
		if status.Deadline.Before(expectedDeadline) || status.Deadline.After(start.Add(timeout)) {
			t.Fatalf("expected workflow deadline to be within %v and %v, got %v", expectedDeadline, start.Add(timeout), status.Deadline)
		}
	})
}

func notificationWaiterWorkflow(ctx DBOSContext, pairID int) (string, error) {
	result, err := GetEvent[string](ctx, WorkflowGetEventInput{
		TargetWorkflowID: fmt.Sprintf("notification-setter-%d", pairID),
		Key:              "event-key",
		Timeout:          10 * time.Second,
	})
	if err != nil {
		return "", err
	}
	return result, nil
}

func notificationSetterWorkflow(ctx DBOSContext, pairID int) (string, error) {
	err := SetEvent(ctx, GenericWorkflowSetEventInput[string]{
		Key:     "event-key",
		Message: fmt.Sprintf("notification-message-%d", pairID),
	})
	if err != nil {
		return "", err
	}
	return "event-set", nil
}

func sendRecvReceiverWorkflow(ctx DBOSContext, pairID int) (string, error) {
	result, err := Recv[string](ctx, WorkflowRecvInput{
		Topic:   "send-recv-topic",
		Timeout: 10 * time.Second,
	})
	if err != nil {
		return "", err
	}
	return result, nil
}

func sendRecvSenderWorkflow(ctx DBOSContext, pairID int) (string, error) {
	err := Send(ctx, GenericWorkflowSendInput[string]{
		DestinationID: fmt.Sprintf("send-recv-receiver-%d", pairID),
		Topic:         "send-recv-topic",
		Message:       fmt.Sprintf("send-recv-message-%d", pairID),
	})
	if err != nil {
		return "", err
	}
	return "message-sent", nil
}

func concurrentSimpleWorkflow(dbosCtx DBOSContext, input int) (int, error) {
	return RunAsStep(dbosCtx, func(ctx context.Context) (int, error) {
		return input * 2, nil
	})
}

func TestConcurrentWorkflows(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)
	RegisterWorkflow(dbosCtx, concurrentSimpleWorkflow)
	RegisterWorkflow(dbosCtx, notificationWaiterWorkflow)
	RegisterWorkflow(dbosCtx, notificationSetterWorkflow)
	RegisterWorkflow(dbosCtx, sendRecvReceiverWorkflow)
	RegisterWorkflow(dbosCtx, sendRecvSenderWorkflow)

	t.Run("SimpleWorkflow", func(t *testing.T) {
		const numGoroutines = 500
		var wg sync.WaitGroup
		results := make(chan int, numGoroutines)
		errors := make(chan error, numGoroutines)

		wg.Add(numGoroutines)
		for i := range numGoroutines {
			go func(input int) {
				defer wg.Done()
				handle, err := RunAsWorkflow(dbosCtx, concurrentSimpleWorkflow, input)
				if err != nil {
					errors <- fmt.Errorf("failed to start workflow %d: %w", input, err)
					return
				}
				result, err := handle.GetResult()
				if err != nil {
					errors <- fmt.Errorf("failed to get result for workflow %d: %w", input, err)
					return
				}
				expectedResult := input * 2
				if result != expectedResult {
					errors <- fmt.Errorf("workflow %d: expected result %d, got %d", input, expectedResult, result)
					return
				}
				results <- result
			}(i)
		}

		wg.Wait()
		close(results)
		close(errors)

		if len(errors) > 0 {
			for err := range errors {
				t.Errorf("Workflow error: %v", err)
			}
			t.Fatalf("Expected no errors from concurrent workflows, got %d errors", len(errors))
		}

		resultCount := 0
		receivedResults := make(map[int]bool)
		for result := range results {
			resultCount++
			if result < 0 || result >= numGoroutines*2 || result%2 != 0 {
				t.Errorf("Unexpected result %d", result)
			} else {
				receivedResults[result] = true
			}
		}

		if resultCount != numGoroutines {
			t.Fatalf("Expected %d results, got %d", numGoroutines, resultCount)
		}
	})

	t.Run("NotificationWorkflows", func(t *testing.T) {
		const numPairs = 500
		var wg sync.WaitGroup
		waiterResults := make(chan string, numPairs)
		setterResults := make(chan string, numPairs)
		errors := make(chan error, numPairs*2)

		wg.Add(numPairs * 2)

		for i := range numPairs {
			go func(pairID int) {
				defer wg.Done()
				handle, err := RunAsWorkflow(dbosCtx, notificationSetterWorkflow, pairID, WithWorkflowID(fmt.Sprintf("notification-setter-%d", pairID)))
				if err != nil {
					errors <- fmt.Errorf("failed to start setter workflow %d: %w", pairID, err)
					return
				}
				result, err := handle.GetResult()
				if err != nil {
					errors <- fmt.Errorf("failed to get result for setter workflow %d: %w", pairID, err)
					return
				}
				setterResults <- result
			}(i)

			go func(pairID int) {
				defer wg.Done()
				handle, err := RunAsWorkflow(dbosCtx, notificationWaiterWorkflow, pairID)
				if err != nil {
					errors <- fmt.Errorf("failed to start waiter workflow %d: %w", pairID, err)
					return
				}
				result, err := handle.GetResult()
				if err != nil {
					errors <- fmt.Errorf("failed to get result for waiter workflow %d: %w", pairID, err)
					return
				}
				expectedMessage := fmt.Sprintf("notification-message-%d", pairID)
				if result != expectedMessage {
					errors <- fmt.Errorf("waiter workflow %d: expected message '%s', got '%s'", pairID, expectedMessage, result)
					return
				}
				waiterResults <- result
			}(i)
		}

		wg.Wait()
		close(waiterResults)
		close(setterResults)
		close(errors)

		if len(errors) > 0 {
			for err := range errors {
				t.Errorf("Workflow error: %v", err)
			}
			t.Fatalf("Expected no errors from notification workflows, got %d errors", len(errors))
		}

		waiterCount := 0
		receivedWaiterResults := make(map[string]bool)
		for result := range waiterResults {
			waiterCount++
			receivedWaiterResults[result] = true
		}

		setterCount := 0
		for result := range setterResults {
			setterCount++
			if result != "event-set" {
				t.Errorf("Expected setter result to be 'event-set', got '%s'", result)
			}
		}

		if waiterCount != numPairs {
			t.Fatalf("Expected %d waiter results, got %d", numPairs, waiterCount)
		}

		if setterCount != numPairs {
			t.Fatalf("Expected %d setter results, got %d", numPairs, setterCount)
		}

		for i := range numPairs {
			expectedWaiterResult := fmt.Sprintf("notification-message-%d", i)
			if !receivedWaiterResults[expectedWaiterResult] {
				t.Errorf("Expected waiter result '%s' not found", expectedWaiterResult)
			}
		}
	})

	t.Run("SendRecvWorkflows", func(t *testing.T) {
		const numPairs = 500
		var wg sync.WaitGroup
		receiverResults := make(chan string, numPairs)
		senderResults := make(chan string, numPairs)
		errors := make(chan error, numPairs*2)

		wg.Add(numPairs * 2)

		for i := range numPairs {
			go func(pairID int) {
				defer wg.Done()
				handle, err := RunAsWorkflow(dbosCtx, sendRecvReceiverWorkflow, pairID, WithWorkflowID(fmt.Sprintf("send-recv-receiver-%d", pairID)))
				if err != nil {
					errors <- fmt.Errorf("failed to start receiver workflow %d: %w", pairID, err)
					return
				}
				result, err := handle.GetResult()
				if err != nil {
					errors <- fmt.Errorf("failed to get result for receiver workflow %d: %w", pairID, err)
					return
				}
				expectedMessage := fmt.Sprintf("send-recv-message-%d", pairID)
				if result != expectedMessage {
					errors <- fmt.Errorf("receiver workflow %d: expected message '%s', got '%s'", pairID, expectedMessage, result)
					return
				}
				receiverResults <- result
			}(i)

			go func(pairID int) {
				defer wg.Done()
				handle, err := RunAsWorkflow(dbosCtx, sendRecvSenderWorkflow, pairID)
				if err != nil {
					errors <- fmt.Errorf("failed to start sender workflow %d: %w", pairID, err)
					return
				}
				result, err := handle.GetResult()
				if err != nil {
					errors <- fmt.Errorf("failed to get result for sender workflow %d: %w", pairID, err)
					return
				}
				senderResults <- result
			}(i)
		}

		wg.Wait()
		close(receiverResults)
		close(senderResults)
		close(errors)

		if len(errors) > 0 {
			for err := range errors {
				t.Errorf("Workflow error: %v", err)
			}
			t.Fatalf("Expected no errors from send/recv workflows, got %d errors", len(errors))
		}

		receiverCount := 0
		receivedReceiverResults := make(map[string]bool)
		for result := range receiverResults {
			receiverCount++
			receivedReceiverResults[result] = true
		}

		senderCount := 0
		for result := range senderResults {
			senderCount++
			if result != "message-sent" {
				t.Errorf("Expected sender result to be 'message-sent', got '%s'", result)
			}
		}

		if receiverCount != numPairs {
			t.Fatalf("Expected %d receiver results, got %d", numPairs, receiverCount)
		}

		if senderCount != numPairs {
			t.Fatalf("Expected %d sender results, got %d", numPairs, senderCount)
		}

		for i := range numPairs {
			expectedReceiverResult := fmt.Sprintf("send-recv-message-%d", i)
			if !receivedReceiverResults[expectedReceiverResult] {
				t.Errorf("Expected receiver result '%s' not found", expectedReceiverResult)
			}
		}
	})
}

func TestWorkflowAtVersion(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)

	RegisterWorkflow(dbosCtx, simpleWorkflow)

	version := "test-app-version-12345"
	handle, err := RunAsWorkflow(dbosCtx, simpleWorkflow, "input", WithApplicationVersion(version))
	if err != nil {
		t.Fatalf("failed to start workflow: %v", err)
	}

	_, err = handle.GetResult()
	if err != nil {
		t.Fatalf("failed to get workflow result: %v", err)
	}

	retrieved, err := RetrieveWorkflow[string](dbosCtx, handle.GetWorkflowID())
	if err != nil {
		t.Fatalf("failed to retrieve workflow: %v", err)
	}

	status, err := retrieved.GetStatus()
	if err != nil {
		t.Fatalf("failed to get workflow status: %v", err)
	}
	if status.ApplicationVersion != version {
		t.Fatalf("expected application version %q, got %q", version, status.ApplicationVersion)
	}
}
