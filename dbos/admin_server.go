package dbos

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

const (
	healthCheckPath            = "/dbos-healthz"
	workflowRecoveryPath       = "/dbos-workflow-recovery"
	workflowQueuesMetadataPath = "/dbos-workflow-queues-metadata"
)

type adminServer struct {
	server *http.Server
}

type queueMetadata struct {
	Name              string       `json:"name"`
	Concurrency       *int         `json:"concurrency,omitempty"`
	WorkerConcurrency *int         `json:"workerConcurrency,omitempty"`
	RateLimit         *RateLimiter `json:"rateLimit,omitempty"`
}

func newAdminServer(ctx *dbosContext, port int) *adminServer {
	mux := http.NewServeMux()

	// Health endpoint
	mux.HandleFunc(healthCheckPath, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"healthy"}`))
	})

	// Recovery endpoint
	mux.HandleFunc(workflowRecoveryPath, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var executorIDs []string
		if err := json.NewDecoder(r.Body).Decode(&executorIDs); err != nil {
			http.Error(w, "Invalid JSON body", http.StatusBadRequest)
			return
		}

		getLogger().Info("Recovering workflows for executors", "executors", executorIDs)

		handles, err := recoverPendingWorkflows(ctx, executorIDs)
		if err != nil {
			getLogger().Error("Error recovering workflows", "error", err)
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
			getLogger().Error("Error encoding response", "error", err)
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
			return
		}
	})

	// Queue metadata endpoint
	mux.HandleFunc(workflowQueuesMetadataPath, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var queueMetadataArray []queueMetadata

		// Iterate through all queues in the registry
		for _, queue := range workflowQueueRegistry {
			queueMetadata := queueMetadata{
				Name:              queue.name,
				WorkerConcurrency: queue.workerConcurrency,
				Concurrency:       queue.globalConcurrency,
				RateLimit:         queue.limiter,
			}

			queueMetadataArray = append(queueMetadataArray, queueMetadata)
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(queueMetadataArray); err != nil {
			getLogger().Error("Error encoding queue metadata response", "error", err)
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
			return
		}
	})

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	return &adminServer{
		server: server,
	}
}

func (as *adminServer) Start() error {
	getLogger().Info("Starting admin server", "port", 3001)

	go func() {
		if err := as.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			getLogger().Error("Admin server error", "error", err)
		}
	}()

	return nil
}

func (as *adminServer) Shutdown() error {
	getLogger().Info("Shutting down admin server")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := as.server.Shutdown(ctx); err != nil {
		getLogger().Error("Admin server shutdown error", "error", err)
		return fmt.Errorf("failed to shutdown admin server: %w", err)
	}

	getLogger().Info("Admin server shutdown complete")
	return nil
}
