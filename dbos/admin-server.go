package dbos

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type AdminServer struct {
	server *http.Server
}

func NewAdminServer(port int) *AdminServer {
	mux := http.NewServeMux()

	// Health endpoint
	mux.HandleFunc("/dbos-healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"healthy"}`))
	})

	// Recovery endpoint
	mux.HandleFunc("/dbos-workflow-recovery", func(w http.ResponseWriter, r *http.Request) {
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

		handles, err := recoverPendingWorkflows(r.Context(), executorIDs)
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
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(workflowIDs); err != nil {
			getLogger().Error("Error encoding response", "error", err)
		}
	})

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	return &AdminServer{
		server: server,
	}
}

func (as *AdminServer) Start() error {
	getLogger().Info("Starting admin server", "port", 3001)

	go func() {
		if err := as.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			getLogger().Error("Admin server error", "error", err)
		}
	}()

	return nil
}

func (as *AdminServer) Shutdown() error {
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
