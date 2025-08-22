package dbos

import (
	"encoding/json"
	"strconv"
	"time"
)

// MessageType represents the type of message exchanged with the conductor
type MessageType string

const (
	ExecutorInfo               MessageType = "executor_info"
	RecoveryMessage            MessageType = "recovery"
	CancelWorkflowMessage      MessageType = "cancel"
	ListWorkflowsMessage       MessageType = "list_workflows"
	ListQueuedWorkflowsMessage MessageType = "list_queued_workflows"
	ListStepsMessage           MessageType = "list_steps"
	GetWorkflowMessage         MessageType = "get_workflow"
	ForkWorkflowMessage        MessageType = "fork_workflow"
)

// baseMessage represents the common structure of all conductor messages
type baseMessage struct {
	Type      MessageType `json:"type"`
	RequestID string      `json:"request_id"`
}

// BaseResponse extends BaseMessage with optional error handling
type BaseResponse struct {
	baseMessage
	ErrorMessage *string `json:"error_message,omitempty"`
}

// ExecutorInfoRequest is sent by the conductor to request executor information
type ExecutorInfoRequest struct {
	baseMessage
}

// ExecutorInfoResponse is sent in response to executor info requests
type ExecutorInfoResponse struct {
	baseMessage
	ExecutorID         string  `json:"executor_id"`
	ApplicationVersion string  `json:"application_version"`
	Hostname           *string `json:"hostname,omitempty"`
	ErrorMessage       *string `json:"error_message,omitempty"`
}

// listWorkflowsConductorRequestBody contains filter parameters for listing workflows
type listWorkflowsConductorRequestBody struct {
	WorkflowUUIDs      []string   `json:"workflow_uuids,omitempty"`
	WorkflowName       *string    `json:"workflow_name,omitempty"`
	AuthenticatedUser  *string    `json:"authenticated_user,omitempty"`
	StartTime          *time.Time `json:"start_time,omitempty"`
	EndTime            *time.Time `json:"end_time,omitempty"`
	Status             *string    `json:"status,omitempty"`
	ApplicationVersion *string    `json:"application_version,omitempty"`
	QueueName          *string    `json:"queue_name,omitempty"`
	Limit              *int       `json:"limit,omitempty"`
	Offset             *int       `json:"offset,omitempty"`
	SortDesc           bool       `json:"sort_desc"`
	LoadInput          bool       `json:"load_input"`
	LoadOutput         bool       `json:"load_output"`
}

// listWorkflowsConductorRequest is sent by the conductor to list workflows
type listWorkflowsConductorRequest struct {
	baseMessage
	Body listWorkflowsConductorRequestBody `json:"body"`
}

// listWorkflowsConductorResponseBody represents a single workflow in the list response
type listWorkflowsConductorResponseBody struct {
	WorkflowUUID       string  `json:"WorkflowUUID"`
	Status             *string `json:"Status,omitempty"`
	WorkflowName       *string `json:"WorkflowName,omitempty"`
	WorkflowClassName  *string `json:"WorkflowClassName,omitempty"`
	WorkflowConfigName *string `json:"WorkflowConfigName,omitempty"`
	AuthenticatedUser  *string `json:"AuthenticatedUser,omitempty"`
	AssumedRole        *string `json:"AssumedRole,omitempty"`
	AuthenticatedRoles *string `json:"AuthenticatedRoles,omitempty"`
	Input              *string `json:"Input,omitempty"`
	Output             *string `json:"Output,omitempty"`
	Error              *string `json:"Error,omitempty"`
	CreatedAt          *string `json:"CreatedAt,omitempty"`
	UpdatedAt          *string `json:"UpdatedAt,omitempty"`
	QueueName          *string `json:"QueueName,omitempty"`
	ApplicationVersion *string `json:"ApplicationVersion,omitempty"`
	ExecutorID         *string `json:"ExecutorID,omitempty"`
}

// listWorkflowsConductorResponse is sent in response to list workflows requests
type listWorkflowsConductorResponse struct {
	baseMessage
	Output       []listWorkflowsConductorResponseBody `json:"output"`
	ErrorMessage *string                              `json:"error_message,omitempty"`
}

// formatListWorkflowsResponseBody converts WorkflowStatus to listWorkflowsConductorResponseBody for the conductor protocol
func formatListWorkflowsResponseBody(wf WorkflowStatus) listWorkflowsConductorResponseBody {
	output := listWorkflowsConductorResponseBody{
		WorkflowUUID: wf.ID,
	}

	// Convert status
	if wf.Status != "" {
		status := string(wf.Status)
		output.Status = &status
	}

	// Convert workflow name
	if wf.Name != "" {
		output.WorkflowName = &wf.Name
	}

	// Copy optional fields
	output.AuthenticatedUser = wf.AuthenticatedUser
	output.AssumedRole = wf.AssumedRole
	output.AuthenticatedRoles = wf.AuthenticatedRoles

	// Convert input/output to JSON strings if present
	if wf.Input != nil {
		inputJSON, err := json.Marshal(wf.Input)
		if err == nil {
			inputStr := string(inputJSON)
			output.Input = &inputStr
		}
	}
	if wf.Output != nil {
		outputJSON, err := json.Marshal(wf.Output)
		if err == nil {
			outputStr := string(outputJSON)
			output.Output = &outputStr
		}
	}

	// Convert error to string
	if wf.Error != nil {
		errorStr := wf.Error.Error()
		output.Error = &errorStr
	}

	// Convert timestamps to RFC3339 strings
	if !wf.CreatedAt.IsZero() {
		createdStr := strconv.FormatInt(wf.CreatedAt.UnixMilli(), 10)
		output.CreatedAt = &createdStr
	}
	if !wf.UpdatedAt.IsZero() {
		updatedStr := strconv.FormatInt(wf.UpdatedAt.UnixMilli(), 10)
		output.UpdatedAt = &updatedStr
	}

	// Copy queue name
	if wf.QueueName != "" {
		output.QueueName = &wf.QueueName
	}

	// Copy application version
	if wf.ApplicationVersion != "" {
		output.ApplicationVersion = &wf.ApplicationVersion
	}

	// Copy executor ID
	if wf.ExecutorID != "" {
		output.ExecutorID = &wf.ExecutorID
	}

	return output
}

// listStepsConductorRequest is sent by the conductor to list workflow steps
type listStepsConductorRequest struct {
	baseMessage
	WorkflowID string `json:"workflow_id"`
}

// workflowStepsConductorResponseBody represents a single workflow step in the list response
type workflowStepsConductorResponseBody struct {
	FunctionID      int     `json:"function_id"`
	FunctionName    string  `json:"function_name"`
	Output          *string `json:"output,omitempty"`
	Error           *string `json:"error,omitempty"`
	ChildWorkflowID *string `json:"child_workflow_id,omitempty"`
}

// listStepsConductorResponse is sent in response to list steps requests
type listStepsConductorResponse struct {
	baseMessage
	Output       *[]workflowStepsConductorResponseBody `json:"output,omitempty"`
	ErrorMessage *string                               `json:"error_message,omitempty"`
}

// formatWorkflowStepsResponseBody converts stepInfo to workflowStepsConductorResponseBody for the conductor protocol
func formatWorkflowStepsResponseBody(step stepInfo) workflowStepsConductorResponseBody {
	output := workflowStepsConductorResponseBody{
		FunctionID:   step.StepID,
		FunctionName: step.StepName,
	}

	// Convert output to JSON string if present
	if step.Output != nil {
		outputJSON, err := json.Marshal(step.Output)
		if err == nil {
			outputStr := string(outputJSON)
			output.Output = &outputStr
		}
	}

	// Convert error to string if present
	if step.Error != nil {
		errorStr := step.Error.Error()
		output.Error = &errorStr
	}

	// Set child workflow ID if present
	if step.ChildWorkflowID != "" {
		output.ChildWorkflowID = &step.ChildWorkflowID
	}

	return output
}

// getWorkflowConductorRequest is sent by the conductor to get a specific workflow
type getWorkflowConductorRequest struct {
	baseMessage
	WorkflowID string `json:"workflow_id"`
}

// getWorkflowConductorResponse is sent in response to get workflow requests
type getWorkflowConductorResponse struct {
	baseMessage
	Output       *listWorkflowsConductorResponseBody `json:"output,omitempty"`
	ErrorMessage *string                             `json:"error_message,omitempty"`
}

// forkWorkflowConductorRequestBody contains the fork workflow parameters
type forkWorkflowConductorRequestBody struct {
	WorkflowID         string  `json:"workflow_id"`
	StartStep          int     `json:"start_step"`
	ApplicationVersion *string `json:"application_version,omitempty"`
	NewWorkflowID      *string `json:"new_workflow_id,omitempty"`
}

// forkWorkflowConductorRequest is sent by the conductor to fork a workflow
type forkWorkflowConductorRequest struct {
	baseMessage
	Body forkWorkflowConductorRequestBody `json:"body"`
}

// forkWorkflowConductorResponse is sent in response to fork workflow requests
type forkWorkflowConductorResponse struct {
	baseMessage
	NewWorkflowID *string `json:"new_workflow_id,omitempty"`
	ErrorMessage  *string `json:"error_message,omitempty"`
}

// cancelWorkflowConductorRequest is sent by the conductor to cancel a workflow
type cancelWorkflowConductorRequest struct {
	baseMessage
	WorkflowID string `json:"workflow_id"`
}

// cancelWorkflowConductorResponse is sent in response to cancel workflow requests
type cancelWorkflowConductorResponse struct {
	baseMessage
	Success      bool    `json:"success"`
	ErrorMessage *string `json:"error_message,omitempty"`
}

// recoveryConductorRequest is sent by the conductor to request recovery of pending workflows
type recoveryConductorRequest struct {
	baseMessage
	ExecutorIDs []string `json:"executor_ids"`
}

// recoveryConductorResponse is sent in response to recovery requests
type recoveryConductorResponse struct {
	baseMessage
	Success      bool    `json:"success"`
	ErrorMessage *string `json:"error_message,omitempty"`
}
