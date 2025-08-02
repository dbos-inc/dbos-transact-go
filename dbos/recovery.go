package dbos

import (
	"strings"
)

func recoverPendingWorkflows(ctx *dbosContext, executorIDs []string) ([]WorkflowHandle[any], error) {
	workflowHandles := make([]WorkflowHandle[any], 0)
	// List pending workflows for the executors
	pendingWorkflows, err := ctx.systemDB.ListWorkflows(ctx, listWorkflowsDBInput{
		status:             []WorkflowStatusType{WorkflowStatusPending},
		executorIDs:        executorIDs,
		applicationVersion: ctx.applicationVersion,
	})
	if err != nil {
		return nil, err
	}

	for _, workflow := range pendingWorkflows {
		if inputStr, ok := workflow.Input.(string); ok {
			if strings.Contains(inputStr, "Failed to decode") {
				getLogger().Warn("Skipping workflow recovery due to input decoding failure", "workflow_id", workflow.ID, "name", workflow.Name)
				continue
			}
		}

		if workflow.QueueName != "" {
			cleared, err := ctx.systemDB.ClearQueueAssignment(ctx.ctx, workflow.ID)
			if err != nil {
				getLogger().Error("Error clearing queue assignment for workflow", "workflow_id", workflow.ID, "name", workflow.Name, "error", err)
				continue
			}
			if cleared {
				workflowHandles = append(workflowHandles, &workflowPollingHandle[any]{workflowID: workflow.ID, dbosContext: ctx})
			}
			continue
		}

		registeredWorkflow, exists := ctx.workflowRegistry[workflow.Name]
		if !exists {
			getLogger().Error("Workflow function not found in registry", "workflow_id", workflow.ID, "name", workflow.Name)
			continue
		}

		// Convert workflow parameters to options
		opts := []WorkflowOption{
			WithWorkflowID(workflow.ID),
		}
		// Create a workflow context from the executor context
		handle, err := registeredWorkflow.wrappedFunction(ctx, workflow.Input, opts...)
		if err != nil {
			return nil, err
		}
		workflowHandles = append(workflowHandles, handle)
	}

	return workflowHandles, nil
}
