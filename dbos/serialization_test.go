package dbos

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

/** Test serialization and deserialization
[x] Built in types
[x] User defined types (structs)
[x] Workflow inputs/outputs
[x] Step inputs/outputs
[x] Direct handlers, polling handler, list workflows results, get step infos
[x] Set/get event with user defined types
*/

// Builtin types
func encodingStepBuiltinTypes(_ context.Context, input int) (int, error) {
	return input, errors.New("step error")
}

func encodingWorkflowBuiltinTypes(ctx DBOSContext, input string) (string, error) {
	stepResult, err := RunAsStep[int](ctx, encodingStepBuiltinTypes, 123)
	return fmt.Sprintf("%d", stepResult), fmt.Errorf("workflow error: %v", err)
}

// Struct types
type StepOutputStruct struct {
	A StepInputStruct
	B string
}

type StepInputStruct struct {
	A SimpleStruct
	B string
}

type WorkflowInputStruct struct {
	A SimpleStruct
	B int
}

type SimpleStruct struct {
	A string
	B int
}

func encodingWorkflowStruct(ctx DBOSContext, input WorkflowInputStruct) (StepOutputStruct, error) {
	return RunAsStep[StepOutputStruct](ctx, encodingStepStruct, StepInputStruct{
		A: input.A,
		B: fmt.Sprintf("%d", input.B),
	})
}

func encodingStepStruct(ctx context.Context, input StepInputStruct) (StepOutputStruct, error) {
	return StepOutputStruct{
		A: input,
		B: "processed by encodingStepStruct",
	}, nil
}

func TestWorkflowEncoding(t *testing.T) {
	executor := setupDBOS(t, true, true)

	// Register workflows with executor
	RegisterWorkflow(executor, encodingWorkflowBuiltinTypes)
	RegisterWorkflow(executor, encodingWorkflowStruct)

	t.Run("BuiltinTypes", func(t *testing.T) {
		// Test a workflow that uses a built-in type (string)
		directHandle, err := RunAsWorkflow(executor, encodingWorkflowBuiltinTypes, "test")
		if err != nil {
			t.Fatalf("failed to execute workflow: %v", err)
		}

		// Test result and error from direct handle
		directHandleResult, err := directHandle.GetResult()
		if directHandleResult != "123" {
			t.Fatalf("expected direct handle result to be '123', got %v", directHandleResult)
		}
		if err.Error() != "workflow error: step error" {
			t.Fatalf("expected error to be 'workflow error: step error', got %v", err)
		}

		// Test result from polling handle
		retrieveHandler, err := RetrieveWorkflow[string](executor.(*dbosContext), directHandle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to retrieve workflow: %v", err)
		}
		retrievedResult, err := retrieveHandler.GetResult()
		if retrievedResult != "123" {
			t.Fatalf("expected retrieved result to be '123', got %v", retrievedResult)
		}
		if err.Error() != "workflow error: step error" {
			t.Fatalf("expected error to be 'workflow error: step error', got %v", err)
		}

		// Test results from ListWorkflows
		workflows, err := executor.(*dbosContext).systemDB.ListWorkflows(context.Background(), listWorkflowsDBInput{
			workflowIDs: []string{directHandle.GetWorkflowID()},
		})
		if err != nil {
			t.Fatalf("failed to list workflows: %v", err)
		}
		if len(workflows) != 1 {
			t.Fatalf("expected 1 workflow, got %d", len(workflows))
		}
		workflow := workflows[0]
		if workflow.Input == nil {
			t.Fatal("expected workflow input to be non-nil")
		}
		workflowInput, ok := workflow.Input.(string)
		if !ok {
			t.Fatalf("expected workflow input to be of type string, got %T", workflow.Input)
		}
		if workflowInput != "test" {
			t.Fatalf("expected workflow input to be 'test', got %v", workflowInput)
		}
		if workflow.Output == nil {
			t.Fatal("expected workflow output to be non-nil")
		}
		workflowOutput, ok := workflow.Output.(string)
		if !ok {
			t.Fatalf("expected workflow output to be of type string, got %T", workflow.Output)
		}
		if workflowOutput != "123" {
			t.Fatalf("expected workflow output to be '123', got %v", workflowOutput)
		}
		if workflow.Error == nil {
			t.Fatal("expected workflow error to be non-nil")
		}
		if workflow.Error.Error() != "workflow error: step error" {
			t.Fatalf("expected workflow error to be 'workflow error: step error', got %v", workflow.Error.Error())
		}

		// Test results from GetWorkflowSteps
		steps, err := executor.(*dbosContext).systemDB.GetWorkflowSteps(context.Background(), directHandle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to get workflow steps: %v", err)
		}
		if len(steps) != 1 {
			t.Fatalf("expected 1 step, got %d", len(steps))
		}
		step := steps[0]
		if step.Output == nil {
			t.Fatal("expected step output to be non-nil")
		}
		stepOutput, ok := step.Output.(int)
		if !ok {
			t.Fatalf("expected step output to be of type int, got %T", step.Output)
		}
		if stepOutput != 123 {
			t.Fatalf("expected step output to be 123, got %v", stepOutput)
		}
		if step.Error == nil {
			t.Fatal("expected step error to be non-nil")
		}
		if step.Error.Error() != "step error" {
			t.Fatalf("expected step error to be 'step error', got %v", step.Error.Error())
		}
	})

	t.Run("StructType", func(t *testing.T) {
		// Test a workflow that calls a step with struct types to verify serialization/deserialization
		input := WorkflowInputStruct{
			A: SimpleStruct{A: "test", B: 123},
			B: 456,
		}

		directHandle, err := RunAsWorkflow(executor, encodingWorkflowStruct, input)
		if err != nil {
			t.Fatalf("failed to execute step workflow: %v", err)
		}

		// Test result from direct handle
		directResult, err := directHandle.GetResult()
		if err != nil {
			t.Fatalf("expected no error but got: %v", err)
		}
		if directResult.A.A.A != input.A.A {
			t.Fatalf("expected direct result input data name to be %v, got %v", input.A.A, directResult.A.A.A)
		}
		if directResult.A.A.B != input.A.B {
			t.Fatalf("expected direct result input data value to be %v, got %v", input.A.B, directResult.A.A.B)
		}
		if directResult.A.B != fmt.Sprintf("%d", input.B) {
			t.Fatalf("expected direct result input ID to be %v, got %v", fmt.Sprintf("%d", input.B), directResult.A.B)
		}
		if directResult.B != "processed by encodingStepStruct" {
			t.Fatalf("expected direct result step info to be 'processed by encodingStepStruct', got %v", directResult.B)
		}

		// Test result from polling handle
		retrieveHandler, err := RetrieveWorkflow[StepOutputStruct](executor.(*dbosContext), directHandle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to retrieve step workflow: %v", err)
		}
		retrievedResult, err := retrieveHandler.GetResult()
		if err != nil {
			t.Fatalf("expected no error but got: %v", err)
		}
		if retrievedResult.A.A.A != input.A.A {
			t.Fatalf("expected retrieved result input data name to be %v, got %v", input.A.A, retrievedResult.A.A.A)
		}
		if retrievedResult.A.A.B != input.A.B {
			t.Fatalf("expected retrieved result input data value to be %v, got %v", input.A.B, retrievedResult.A.A.B)
		}
		if retrievedResult.A.B != fmt.Sprintf("%d", input.B) {
			t.Fatalf("expected retrieved result input ID to be %v, got %v", fmt.Sprintf("%d", input.B), retrievedResult.A.B)
		}
		if retrievedResult.B != "processed by encodingStepStruct" {
			t.Fatalf("expected retrieved result step info to be 'processed by encodingStepStruct', got %v", retrievedResult.B)
		}

		// Test results from ListWorkflows
		workflows, err := executor.(*dbosContext).systemDB.ListWorkflows(context.Background(), listWorkflowsDBInput{
			workflowIDs: []string{directHandle.GetWorkflowID()},
		})
		if err != nil {
			t.Fatalf("failed to list workflows: %v", err)
		}
		workflow := workflows[0]
		if workflow.Input == nil {
			t.Fatal("expected workflow input to be non-nil")
		}
		workflowInput, ok := workflow.Input.(WorkflowInputStruct)
		if !ok {
			t.Fatalf("expected workflow input to be of type WorkflowInputStruct, got %T", workflow.Input)
		}
		if workflowInput.A.A != input.A.A {
			t.Fatalf("expected workflow input data name to be %v, got %v", input.A.A, workflowInput.A.A)
		}
		if workflowInput.A.B != input.A.B {
			t.Fatalf("expected workflow input data value to be %v, got %v", input.A.B, workflowInput.A.B)
		}
		if workflowInput.B != input.B {
			t.Fatalf("expected workflow input ID to be %v, got %v", input.B, workflowInput.B)
		}

		workflowOutput, ok := workflow.Output.(StepOutputStruct)
		if !ok {
			t.Fatalf("expected workflow output to be of type StepOutputStruct, got %T", workflow.Output)
		}
		if workflowOutput.A.A.A != input.A.A {
			t.Fatalf("expected workflow output input data name to be %v, got %v", input.A.A, workflowOutput.A.A.A)
		}
		if workflowOutput.A.A.B != input.A.B {
			t.Fatalf("expected workflow output input data value to be %v, got %v", input.A.B, workflowOutput.A.A.B)
		}
		if workflowOutput.A.B != fmt.Sprintf("%d", input.B) {
			t.Fatalf("expected workflow output input ID to be %v, got %v", fmt.Sprintf("%d", input.B), workflowOutput.A.B)
		}
		if workflowOutput.B != "processed by encodingStepStruct" {
			t.Fatalf("expected workflow output step info to be 'processed by encodingStepStruct', got %v", workflowOutput.B)
		}

		// Test results from GetWorkflowSteps
		steps, err := executor.(*dbosContext).systemDB.GetWorkflowSteps(context.Background(), directHandle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to get workflow steps: %v", err)
		}
		if len(steps) != 1 {
			t.Fatalf("expected 1 step, got %d", len(steps))
		}
		step := steps[0]
		if step.Output == nil {
			t.Fatal("expected step output to be non-nil")
		}
		stepOutput, ok := step.Output.(StepOutputStruct)
		if !ok {
			t.Fatalf("expected step output to be of type StepOutputStruct, got %T", step.Output)
		}
		if stepOutput.A.A.A != input.A.A {
			t.Fatalf("expected step output input data name to be %v, got %v", input.A.A, stepOutput.A.A.A)
		}
		if stepOutput.A.A.B != input.A.B {
			t.Fatalf("expected step output input data value to be %v, got %v", input.A.B, stepOutput.A.A.B)
		}
		if stepOutput.A.B != fmt.Sprintf("%d", input.B) {
			t.Fatalf("expected step output input ID to be %v, got %v", fmt.Sprintf("%d", input.B), stepOutput.A.B)
		}
		if stepOutput.B != "processed by encodingStepStruct" {
			t.Fatalf("expected step output step info to be 'processed by encodingStepStruct', got %v", stepOutput.B)
		}
		if step.Error != nil {
			t.Fatalf("expected step error to be nil, got %v", step.Error)
		}
	})
}

type UserDefinedEventData struct {
	ID      int    `json:"id"`
	Name    string `json:"name"`
	Details struct {
		Description string   `json:"description"`
		Tags        []string `json:"tags"`
	} `json:"details"`
}

func setEventUserDefinedTypeWorkflow(ctx DBOSContext, input string) (string, error) {
	eventData := UserDefinedEventData{
		ID:   42,
		Name: "test-event",
		Details: struct {
			Description string   `json:"description"`
			Tags        []string `json:"tags"`
		}{
			Description: "This is a test event with user-defined data",
			Tags:        []string{"test", "user-defined", "serialization"},
		},
	}

	err := SetEvent(ctx, WorkflowSetEventInputGeneric[UserDefinedEventData]{Key: input, Message: eventData})
	if err != nil {
		return "", err
	}
	return "user-defined-event-set", nil
}

func TestSetEventSerialize(t *testing.T) {
	executor := setupDBOS(t, true, true)

	// Register workflow with executor
	RegisterWorkflow(executor, setEventUserDefinedTypeWorkflow)

	t.Run("SetEventUserDefinedType", func(t *testing.T) {
		// Start a workflow that sets an event with a user-defined type
		setHandle, err := RunAsWorkflow(executor, setEventUserDefinedTypeWorkflow, "user-defined-key")
		if err != nil {
			t.Fatalf("failed to start workflow with user-defined event type: %v", err)
		}

		// Wait for the workflow to complete
		result, err := setHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from user-defined event workflow: %v", err)
		}
		if result != "user-defined-event-set" {
			t.Fatalf("expected result to be 'user-defined-event-set', got '%s'", result)
		}

		// Retrieve the event to verify it was properly serialized and can be deserialized
		retrievedEvent, err := GetEvent[UserDefinedEventData](executor, WorkflowGetEventInput{
			TargetWorkflowID: setHandle.GetWorkflowID(),
			Key:              "user-defined-key",
			Timeout:          3 * time.Second,
		})
		if err != nil {
			t.Fatalf("failed to get user-defined event: %v", err)
		}

		// Verify the retrieved data matches what we set
		if retrievedEvent.ID != 42 {
			t.Fatalf("expected ID to be 42, got %d", retrievedEvent.ID)
		}
		if retrievedEvent.Name != "test-event" {
			t.Fatalf("expected Name to be 'test-event', got '%s'", retrievedEvent.Name)
		}
		if retrievedEvent.Details.Description != "This is a test event with user-defined data" {
			t.Fatalf("expected Description to be 'This is a test event with user-defined data', got '%s'", retrievedEvent.Details.Description)
		}
		if len(retrievedEvent.Details.Tags) != 3 {
			t.Fatalf("expected 3 tags, got %d", len(retrievedEvent.Details.Tags))
		}
		expectedTags := []string{"test", "user-defined", "serialization"}
		for i, tag := range retrievedEvent.Details.Tags {
			if tag != expectedTags[i] {
				t.Fatalf("expected tag %d to be '%s', got '%s'", i, expectedTags[i], tag)
			}
		}
	})
}

func sendUserDefinedTypeWorkflow(ctx DBOSContext, destinationID string) (string, error) {
	// Create an instance of our user-defined type inside the workflow
	sendData := UserDefinedEventData{
		ID:   42,
		Name: "test-send-message",
		Details: struct {
			Description string   `json:"description"`
			Tags        []string `json:"tags"`
		}{
			Description: "This is a test send message with user-defined data",
			Tags:        []string{"test", "user-defined", "serialization", "send"},
		},
	}

	// Send should automatically register this type with gob
	// Note the explicit type parameter since compiler cannot infer UserDefinedEventData from string input
	err := Send(ctx, WorkflowSendInput[UserDefinedEventData]{
		DestinationID: destinationID,
		Topic:         "user-defined-topic",
		Message:       sendData,
	})
	if err != nil {
		return "", err
	}
	return "user-defined-message-sent", nil
}

func recvUserDefinedTypeWorkflow(ctx DBOSContext, input string) (UserDefinedEventData, error) {
	// Receive the user-defined type message
	result, err := Recv[UserDefinedEventData](ctx, WorkflowRecvInput{
		Topic:   "user-defined-topic",
		Timeout: 3 * time.Second,
	})
	return result, err
}

func TestSendSerialize(t *testing.T) {
	executor := setupDBOS(t, true, true)

	// Register workflows with executor
	RegisterWorkflow(executor, sendUserDefinedTypeWorkflow)
	RegisterWorkflow(executor, recvUserDefinedTypeWorkflow)

	t.Run("SendUserDefinedType", func(t *testing.T) {
		// Start a receiver workflow first
		recvHandle, err := RunAsWorkflow(executor, recvUserDefinedTypeWorkflow, "recv-input")
		if err != nil {
			t.Fatalf("failed to start receive workflow: %v", err)
		}

		// Start a sender workflow that sends a message with a user-defined type
		sendHandle, err := RunAsWorkflow(executor, sendUserDefinedTypeWorkflow, recvHandle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to start workflow with user-defined send type: %v", err)
		}

		// Wait for the sender workflow to complete
		sendResult, err := sendHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from user-defined send workflow: %v", err)
		}
		if sendResult != "user-defined-message-sent" {
			t.Fatalf("expected result to be 'user-defined-message-sent', got '%s'", sendResult)
		}

		// Wait for the receiver workflow to complete and get the message
		receivedData, err := recvHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from receive workflow: %v", err)
		}

		// Verify the received data matches what we sent
		if receivedData.ID != 42 {
			t.Fatalf("expected ID to be 42, got %d", receivedData.ID)
		}
		if receivedData.Name != "test-send-message" {
			t.Fatalf("expected Name to be 'test-send-message', got '%s'", receivedData.Name)
		}
		if receivedData.Details.Description != "This is a test send message with user-defined data" {
			t.Fatalf("expected Description to be 'This is a test send message with user-defined data', got '%s'", receivedData.Details.Description)
		}

		// Verify tags
		expectedTags := []string{"test", "user-defined", "serialization", "send"}
		if len(receivedData.Details.Tags) != len(expectedTags) {
			t.Fatalf("expected %d tags, got %d", len(expectedTags), len(receivedData.Details.Tags))
		}
		for i, tag := range receivedData.Details.Tags {
			if tag != expectedTags[i] {
				t.Fatalf("expected tag %d to be '%s', got '%s'", i, expectedTags[i], tag)
			}
		}
	})
}
