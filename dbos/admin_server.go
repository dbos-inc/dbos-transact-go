package dbos

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"
)

const (
	// HTTP handler patterns with verbs
	_HEALTHCHECK_PATTERN              = "GET /dbos-healthz"
	_WORKFLOW_RECOVERY_PATTERN        = "POST /dbos-workflow-recovery"
	_DEACTIVATE_PATTERN               = "GET /deactivate"
	_WORKFLOW_QUEUES_METADATA_PATTERN = "GET /dbos-workflow-queues-metadata"
	_GARBAGE_COLLECT_PATTERN          = "POST /dbos-garbage-collect"
	_GLOBAL_TIMEOUT_PATTERN           = "POST /dbos-global-timeout"
	_QUEUED_WORKFLOWS_PATTERN         = "POST /queues"
	_WORKFLOWS_PATTERN                = "POST /workflows"
	_WORKFLOW_PATTERN                 = "GET /workflows/{id}"
	_WORKFLOW_STEPS_PATTERN           = "GET /workflows/{id}/steps"
	_WORKFLOW_CANCEL_PATTERN          = "POST /workflows/{id}/cancel"
	_WORKFLOW_RESUME_PATTERN          = "POST /workflows/{id}/resume"
	_WORKFLOW_RESTART_PATTERN         = "POST /workflows/{id}/restart"
	_WORKFLOW_FORK_PATTERN            = "POST /workflows/{id}/fork"

	_ADMIN_SERVER_READ_HEADER_TIMEOUT = 5 * time.Second
	_ADMIN_SERVER_SHUTDOWN_TIMEOUT    = 10 * time.Second
)

// listWorkflowsRequest represents the request structure for listing workflows
type listWorkflowsRequest struct {
	WorkflowUUIDs      []string             `json:"workflow_uuids"`      // Filter by specific workflow IDs
	AuthenticatedUser  *string              `json:"authenticated_user"`  // Filter by user who initiated the workflow
	StartTime          *time.Time           `json:"start_time"`          // Filter workflows created after this time (RFC3339 format)
	EndTime            *time.Time           `json:"end_time"`            // Filter workflows created before this time (RFC3339 format)
	Status             []WorkflowStatusType `json:"status"`              // Filter by workflow status(es)
	ApplicationVersion *string              `json:"application_version"` // Filter by application version
	WorkflowName       *string              `json:"workflow_name"`       // Filter by workflow function name
	Limit              *int                 `json:"limit"`               // Maximum number of results to return
	Offset             *int                 `json:"offset"`              // Offset for pagination
	SortDesc           *bool                `json:"sort_desc"`           // Sort in descending order by creation time
	WorkflowIDPrefix   *string              `json:"workflow_id_prefix"`  // Filter by workflow ID prefix
	LoadInput          *bool                `json:"load_input"`          // Include workflow input in response
	LoadOutput         *bool                `json:"load_output"`         // Include workflow output in response
	QueueName          *string              `json:"queue_name"`          // Filter by queue name (for queued workflows)
}

// buildOptions converts the request struct into a slice of ListWorkflowsOption
func (req *listWorkflowsRequest) toListWorkflowsOptions() []ListWorkflowsOption {
	var opts []ListWorkflowsOption
	if len(req.WorkflowUUIDs) > 0 {
		opts = append(opts, WithWorkflowIDs(req.WorkflowUUIDs))
	}
	if req.AuthenticatedUser != nil {
		opts = append(opts, WithUser(*req.AuthenticatedUser))
	}
	if req.StartTime != nil {
		opts = append(opts, WithStartTime(*req.StartTime))
	}
	if req.EndTime != nil {
		opts = append(opts, WithEndTime(*req.EndTime))
	}
	if len(req.Status) > 0 {
		opts = append(opts, WithStatus(req.Status))
	}
	if req.ApplicationVersion != nil {
		opts = append(opts, WithAppVersion(*req.ApplicationVersion))
	}
	if req.WorkflowName != nil {
		opts = append(opts, WithName(*req.WorkflowName))
	}
	if req.Limit != nil {
		opts = append(opts, WithLimit(*req.Limit))
	}
	if req.Offset != nil {
		opts = append(opts, WithOffset(*req.Offset))
	}
	if req.SortDesc != nil {
		opts = append(opts, WithSortDesc(*req.SortDesc))
	}
	if req.WorkflowIDPrefix != nil {
		opts = append(opts, WithWorkflowIDPrefix(*req.WorkflowIDPrefix))
	}
	if req.LoadInput != nil {
		opts = append(opts, WithLoadInput(*req.LoadInput))
	}
	if req.LoadOutput != nil {
		opts = append(opts, WithLoadOutput(*req.LoadOutput))
	}
	if req.QueueName != nil {
		opts = append(opts, WithQueueName(*req.QueueName))
	}
	return opts
}

type adminServer struct {
	server        *http.Server
	logger        *slog.Logger
	port          int
	isDeactivated atomic.Int32
}

// workflowStatusToUTC converts a WorkflowStatus to a map with all time fields in UTC
// not super ergonomic but the DBOS console excepts unix timestamps
func workflowStatusToUTC(ws WorkflowStatus) map[string]any {
	result := map[string]any{
		"WorkflowUUID":       ws.ID,
		"Status":             ws.Status,
		"WorkflowName":       ws.Name,
		"AuthenticatedUser":  ws.AuthenticatedUser,
		"AssumedRole":        ws.AssumedRole,
		"AuthenticatedRoles": ws.AuthenticatedRoles,
		"Output":             ws.Output,
		"Error":              ws.Error,
		"ExecutorID":         ws.ExecutorID,
		"ApplicationVersion": ws.ApplicationVersion,
		"ApplicationID":      ws.ApplicationID,
		"Attempts":           ws.Attempts,
		"QueueName":          ws.QueueName,
		"Timeout":            ws.Timeout,
		"DeduplicationID":    ws.DeduplicationID,
		"Input":              ws.Input,
	}

	// Convert time fields to UTC Unix timestamps (milliseconds)
	if !ws.CreatedAt.IsZero() {
		result["CreatedAt"] = ws.CreatedAt.UTC().UnixMilli()
	} else {
		result["CreatedAt"] = nil
	}

	if !ws.UpdatedAt.IsZero() {
		result["UpdatedAt"] = ws.UpdatedAt.UTC().UnixMilli()
	} else {
		result["UpdatedAt"] = nil
	}

	if !ws.Deadline.IsZero() {
		result["Deadline"] = ws.Deadline.UTC().UnixMilli()
	} else {
		result["Deadline"] = nil
	}

	if !ws.StartedAt.IsZero() {
		result["StartedAt"] = ws.StartedAt.UTC().UnixMilli()
	} else {
		result["StartedAt"] = nil
	}

	return result
}

func newAdminServer(ctx *dbosContext, port int) *adminServer {
	as := &adminServer{
		logger: ctx.logger,
		port:   port,
	}

	mux := http.NewServeMux()

	ctx.logger.Debug("Registering admin server endpoint", "pattern", _HEALTHCHECK_PATTERN)
	mux.HandleFunc(_HEALTHCHECK_PATTERN, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{"status":"healthy"}`))
		if err != nil {
			ctx.logger.Error("Error writing health check response", "error", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
	})

	ctx.logger.Debug("Registering admin server endpoint", "pattern", _WORKFLOW_RECOVERY_PATTERN)
	mux.HandleFunc(_WORKFLOW_RECOVERY_PATTERN, func(w http.ResponseWriter, r *http.Request) {
		var executorIDs []string
		if err := json.NewDecoder(r.Body).Decode(&executorIDs); err != nil {
			http.Error(w, "Invalid JSON body", http.StatusBadRequest)
			return
		}

		ctx.logger.Info("Recovering workflows for executors", "executors", executorIDs)

		handles, err := recoverPendingWorkflows(ctx, executorIDs)
		if err != nil {
			ctx.logger.Error("Error recovering workflows", "error", err)
			http.Error(w, fmt.Sprintf("Recovery failed: %v", err), http.StatusInternalServerError)
			return
		}

		// Extract workflow IDs from handles
		workflowIDs := make([]string, len(handles))
		for i, handle := range handles {
			workflowIDs[i] = handle.GetWorkflowID()
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(workflowIDs); err != nil {
			ctx.logger.Error("Error encoding response", "error", err)
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
			return
		}
	})

	ctx.logger.Debug("Registering admin server endpoint", "pattern", _DEACTIVATE_PATTERN)
	mux.HandleFunc(_DEACTIVATE_PATTERN, func(w http.ResponseWriter, r *http.Request) {
		if as.isDeactivated.CompareAndSwap(0, 1) {
			ctx.logger.Info("Deactivating DBOS executor", "executor_id", ctx.executorID, "app_version", ctx.applicationVersion)
			// TODO: Stop queue runner, workflow scheduler, etc
		}

		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("deactivated")); err != nil {
			ctx.logger.Error("Error writing deactivate response", "error", err)
		}
	})

	ctx.logger.Debug("Registering admin server endpoint", "pattern", _WORKFLOW_QUEUES_METADATA_PATTERN)
	mux.HandleFunc(_WORKFLOW_QUEUES_METADATA_PATTERN, func(w http.ResponseWriter, r *http.Request) {
		queueMetadataArray := ctx.queueRunner.listQueues()

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(queueMetadataArray); err != nil {
			ctx.logger.Error("Error encoding queue metadata response", "error", err)
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
			return
		}
	})

	ctx.logger.Debug("Registering admin server endpoint", "pattern", _GARBAGE_COLLECT_PATTERN)
	mux.HandleFunc(_GARBAGE_COLLECT_PATTERN, func(w http.ResponseWriter, r *http.Request) {
		var inputs struct {
			CutoffEpochTimestampMs *int64 `json:"cutoff_epoch_timestamp_ms"`
			RowsThreshold          *int   `json:"rows_threshold"`
		}

		if err := json.NewDecoder(r.Body).Decode(&inputs); err != nil {
			http.Error(w, "Invalid JSON body", http.StatusBadRequest)
			return
		}

		// TODO: Implement garbage collection
		// err := garbageCollect(ctx, inputs.CutoffEpochTimestampMs, inputs.RowsThreshold)
		// if err != nil {
		//     ctx.logger.Error("Garbage collection failed", "error", err)
		//     http.Error(w, fmt.Sprintf("Garbage collection failed: %v", err), http.StatusInternalServerError)
		//     return
		// }

		w.WriteHeader(http.StatusNoContent)
	})

	ctx.logger.Debug("Registering admin server endpoint", "pattern", _GLOBAL_TIMEOUT_PATTERN)
	mux.HandleFunc(_GLOBAL_TIMEOUT_PATTERN, func(w http.ResponseWriter, r *http.Request) {
		var inputs struct {
			CutoffEpochTimestampMs *int64 `json:"cutoff_epoch_timestamp_ms"`
		}

		if err := json.NewDecoder(r.Body).Decode(&inputs); err != nil {
			http.Error(w, "Invalid JSON body", http.StatusBadRequest)
			return
		}

		// TODO: Implement global timeout
		// err := globalTimeout(ctx, inputs.CutoffEpochTimestampMs)
		// if err != nil {
		//     ctx.logger.Error("Global timeout failed", "error", err)
		//     http.Error(w, fmt.Sprintf("Global timeout failed: %v", err), http.StatusInternalServerError)
		//     return
		// }

		w.WriteHeader(http.StatusNoContent)
	})

	ctx.logger.Debug("Registering admin server endpoint", "pattern", _WORKFLOWS_PATTERN)
	mux.HandleFunc(_WORKFLOWS_PATTERN, func(w http.ResponseWriter, r *http.Request) {
		var req listWorkflowsRequest
		if r.ContentLength > 0 {
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, fmt.Sprintf("Invalid JSON input: %v", err), http.StatusBadRequest)
				return
			}
		}

		workflows, err := ctx.ListWorkflows(req.toListWorkflowsOptions()...)
		if err != nil {
			ctx.logger.Error("Failed to list workflows", "error", err)
			http.Error(w, fmt.Sprintf("Failed to list workflows: %v", err), http.StatusInternalServerError)
			return
		}

		// Transform to UTC before encoding
		utcWorkflows := make([]map[string]any, len(workflows))
		for i, wf := range workflows {
			utcWorkflows[i] = workflowStatusToUTC(wf)
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(utcWorkflows); err != nil {
			ctx.logger.Error("Error encoding workflows response", "error", err)
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
		}
	})

	ctx.logger.Debug("Registering admin server endpoint", "pattern", _WORKFLOW_PATTERN)
	mux.HandleFunc(_WORKFLOW_PATTERN, func(w http.ResponseWriter, r *http.Request) {
		workflowID := r.PathValue("id")

		// Use ListWorkflows with the specific workflow ID filter
		opts := []ListWorkflowsOption{WithWorkflowIDs([]string{workflowID})}
		workflows, err := ctx.ListWorkflows(opts...)
		if err != nil {
			ctx.logger.Error("Failed to get workflow", "workflow_id", workflowID, "error", err)
			http.Error(w, fmt.Sprintf("Failed to get workflow: %v", err), http.StatusInternalServerError)
			return
		}

		// If no workflow found, return 404
		if len(workflows) == 0 {
			http.Error(w, "Workflow not found", http.StatusNotFound)
			return
		}

		// Return the first (and only) workflow, transformed to UTC
		workflow := workflowStatusToUTC(workflows[0])

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(workflow); err != nil {
			ctx.logger.Error("Error encoding workflow response", "error", err)
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
		}
	})

	ctx.logger.Debug("Registering admin server endpoint", "pattern", _QUEUED_WORKFLOWS_PATTERN)
	mux.HandleFunc(_QUEUED_WORKFLOWS_PATTERN, func(w http.ResponseWriter, r *http.Request) {
		var req listWorkflowsRequest
		if r.ContentLength > 0 {
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, fmt.Sprintf("Invalid JSON input: %v", err), http.StatusBadRequest)
				return
			}
		}

		workflows, err := ctx.ListWorkflows(req.toListWorkflowsOptions()...)
		if err != nil {
			ctx.logger.Error("Failed to list queued workflows", "error", err)
			http.Error(w, fmt.Sprintf("Failed to list queued workflows: %v", err), http.StatusInternalServerError)
			return
		}

		// If not queue was specified, filter out non-queued workflows
		if req.QueueName == nil {
			filtered := make([]WorkflowStatus, 0, len(workflows))
			for _, wf := range workflows {
				if len(wf.QueueName) > 0 {
					filtered = append(filtered, wf)
				}
			}
			workflows = filtered
		}

		// Transform to UNIX timestamps before encoding
		utcWorkflows := make([]map[string]any, len(workflows))
		for i, wf := range workflows {
			utcWorkflows[i] = workflowStatusToUTC(wf)
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(utcWorkflows); err != nil {
			ctx.logger.Error("Error encoding queued workflows response", "error", err)
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
		}
	})

	ctx.logger.Debug("Registering admin server endpoint", "pattern", _WORKFLOW_STEPS_PATTERN)
	mux.HandleFunc(_WORKFLOW_STEPS_PATTERN, func(w http.ResponseWriter, r *http.Request) {
		workflowID := r.PathValue("id")

		steps, err := ctx.systemDB.getWorkflowSteps(ctx, workflowID)
		if err != nil {
			ctx.logger.Error("Failed to list workflow steps", "workflow_id", workflowID, "error", err)
			http.Error(w, fmt.Sprintf("Failed to list steps: %v", err), http.StatusInternalServerError)
			return
		}

		// Transform to snake_case format with function_id and function_name
		formattedSteps := make([]map[string]any, len(steps))
		for i, step := range steps {
			formattedSteps[i] = map[string]any{
				"function_id":       step.StepID,
				"function_name":     step.StepName,
				"output":            step.Output,
				"error":             step.Error,
				"child_workflow_id": step.ChildWorkflowID,
			}
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(formattedSteps); err != nil {
			ctx.logger.Error("Error encoding steps response", "error", err)
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
		}
	})

	ctx.logger.Debug("Registering admin server endpoint", "pattern", _WORKFLOW_CANCEL_PATTERN)
	mux.HandleFunc(_WORKFLOW_CANCEL_PATTERN, func(w http.ResponseWriter, r *http.Request) {
		workflowID := r.PathValue("id")
		ctx.logger.Info("Cancelling workflow", "workflow_id", workflowID)

		err := ctx.CancelWorkflow(workflowID)
		if err != nil {
			ctx.logger.Error("Failed to cancel workflow", "workflow_id", workflowID, "error", err)
			http.Error(w, fmt.Sprintf("Failed to cancel workflow: %v", err), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})

	ctx.logger.Debug("Registering admin server endpoint", "pattern", _WORKFLOW_RESUME_PATTERN)
	mux.HandleFunc(_WORKFLOW_RESUME_PATTERN, func(w http.ResponseWriter, r *http.Request) {
		workflowID := r.PathValue("id")
		ctx.logger.Info("Resuming workflow", "workflow_id", workflowID)

		_, err := ctx.ResumeWorkflow(ctx, workflowID)
		if err != nil {
			ctx.logger.Error("Failed to resume workflow", "workflow_id", workflowID, "error", err)
			http.Error(w, fmt.Sprintf("Failed to resume workflow: %v", err), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})

	ctx.logger.Debug("Registering admin server endpoint", "pattern", _WORKFLOW_RESTART_PATTERN)
	mux.HandleFunc(_WORKFLOW_RESTART_PATTERN, func(w http.ResponseWriter, r *http.Request) {
		workflowID := r.PathValue("id")
		ctx.logger.Info("Restarting workflow", "workflow_id", workflowID)

		// Restart is like forking but starting at step 0
		input := ForkWorkflowInput{
			OriginalWorkflowID: workflowID,
		}

		handle, err := ctx.ForkWorkflow(ctx, input)
		if err != nil {
			ctx.logger.Error("Failed to restart workflow", "workflow_id", workflowID, "error", err)
			http.Error(w, fmt.Sprintf("Failed to restart workflow: %v", err), http.StatusInternalServerError)
			return
		}

		response := map[string]string{
			"workflow_id": handle.GetWorkflowID(),
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			ctx.logger.Error("Error encoding restart response", "error", err)
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
		}
	})

	ctx.logger.Debug("Registering admin server endpoint", "pattern", _WORKFLOW_FORK_PATTERN)
	mux.HandleFunc(_WORKFLOW_FORK_PATTERN, func(w http.ResponseWriter, r *http.Request) {
		workflowID := r.PathValue("id")
		var data struct {
			StartStep          *uint   `json:"start_step"`
			ForkedWorkflowID   *string `json:"new_workflow_id"`
			ApplicationVersion *string `json:"application_version"`
		}

		if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
			http.Error(w, fmt.Sprintf("Invalid JSON input: %v", err), http.StatusBadRequest)
			return
		}

		// Prepare fork input
		input := ForkWorkflowInput{
			OriginalWorkflowID: workflowID,
		}
		if data.StartStep != nil {
			input.StartStep = *data.StartStep
		}
		if data.ForkedWorkflowID != nil {
			input.ForkedWorkflowID = *data.ForkedWorkflowID
		}
		if data.ApplicationVersion != nil {
			input.ApplicationVersion = *data.ApplicationVersion
		}

		ctx.logger.Info("Forking workflow", "workflow_id", workflowID, "start_step", input.StartStep)

		handle, err := ctx.ForkWorkflow(ctx, input)
		if err != nil {
			ctx.logger.Error("Failed to fork workflow", "workflow_id", workflowID, "error", err)
			http.Error(w, fmt.Sprintf("Failed to fork workflow: %v", err), http.StatusInternalServerError)
			return
		}

		response := map[string]string{
			"workflow_id": handle.GetWorkflowID(),
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			ctx.logger.Error("Error encoding fork response", "error", err)
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
		}
	})

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           mux,
		ReadHeaderTimeout: _ADMIN_SERVER_READ_HEADER_TIMEOUT,
	}

	as.server = server
	return as
}

func (as *adminServer) Start() error {
	as.logger.Info("Starting admin server", "port", as.port)

	go func() {
		if err := as.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			as.logger.Error("Admin server error", "error", err)
		}
	}()

	return nil
}

func (as *adminServer) Shutdown(ctx context.Context) error {
	as.logger.Info("Shutting down admin server")

	// Note: consider moving the grace period to DBOSContext.Shutdown()
	ctx, cancel := context.WithTimeout(ctx, _ADMIN_SERVER_SHUTDOWN_TIMEOUT)
	defer cancel()

	if err := as.server.Shutdown(ctx); err != nil {
		as.logger.Error("Admin server shutdown error", "error", err)
		return fmt.Errorf("failed to shutdown admin server: %w", err)
	}

	as.logger.Info("Admin server shutdown complete")
	return nil
}
