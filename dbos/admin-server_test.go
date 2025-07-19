package dbos

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func TestAdminServer(t *testing.T) {
	// Skip if database is not available
	databaseURL := os.Getenv("DBOS_DATABASE_URL")
	if databaseURL == "" && os.Getenv("PGPASSWORD") == "" {
		t.Skip("Database not available (DBOS_DATABASE_URL and PGPASSWORD not set), skipping DBOS integration tests")
	}

	t.Run("Admin server is not started without WithAdminServer option", func(t *testing.T) {
		// Ensure clean state
		if dbos != nil {
			Shutdown()
		}

		// Launch DBOS without admin server option
		err := Launch()
		if err != nil {
			t.Skipf("Failed to launch DBOS (database likely not available): %v", err)
		}

		// Ensure cleanup
		defer Shutdown()

		// Give time for any startup processes
		time.Sleep(100 * time.Millisecond)

		// Verify admin server is not running
		client := &http.Client{Timeout: 1 * time.Second}
		_, err = client.Get("http://localhost:3001/dbos-healthz")
		if err == nil {
			t.Error("Expected request to fail when admin server is not started, but it succeeded")
		}

		// Verify the DBOS executor doesn't have an admin server instance
		if dbos == nil {
			t.Fatal("Expected DBOS instance to be created")
		}

		if dbos.adminServer != nil {
			t.Error("Expected admin server to be nil when not configured")
		}
	})

	t.Run("Admin server endpoints", func(t *testing.T) {
		// Ensure clean state
		if dbos != nil {
			Shutdown()
		}

		// Launch DBOS with admin server once for all endpoint tests
		err := Launch(WithAdminServer())
		if err != nil {
			t.Skipf("Failed to launch DBOS with admin server (database likely not available): %v", err)
		}

		// Ensure cleanup
		defer Shutdown()

		// Give the server a moment to start
		time.Sleep(100 * time.Millisecond)

		// Verify the DBOS executor has an admin server instance
		if dbos == nil {
			t.Fatal("Expected DBOS instance to be created")
		}

		if dbos.adminServer == nil {
			t.Fatal("Expected admin server to be created in DBOS instance")
		}

		client := &http.Client{Timeout: 5 * time.Second}

		tests := []struct {
			name           string
			method         string
			endpoint       string
			body           io.Reader
			contentType    string
			expectedStatus int
			validateResp   func(t *testing.T, resp *http.Response)
		}{
			{
				name:           "Health endpoint responds correctly",
				method:         "GET",
				endpoint:       "http://localhost:3001/dbos-healthz",
				expectedStatus: http.StatusOK,
			},
			{
				name:           "Recovery endpoint responds correctly with valid JSON",
				method:         "POST",
				endpoint:       "http://localhost:3001/dbos-workflow-recovery",
				body:           bytes.NewBuffer(mustMarshal([]string{"executor1", "executor2"})),
				contentType:    "application/json",
				expectedStatus: http.StatusOK,
				validateResp: func(t *testing.T, resp *http.Response) {
					var workflowIDs []string
					if err := json.NewDecoder(resp.Body).Decode(&workflowIDs); err != nil {
						t.Errorf("Failed to decode response as JSON array: %v", err)
					}
					if workflowIDs == nil {
						t.Error("Expected non-nil workflow IDs array")
					}
				},
			},
			{
				name:           "Recovery endpoint rejects invalid methods",
				method:         "GET",
				endpoint:       "http://localhost:3001/dbos-workflow-recovery",
				expectedStatus: http.StatusMethodNotAllowed,
			},
			{
				name:           "Recovery endpoint rejects invalid JSON",
				method:         "POST",
				endpoint:       "http://localhost:3001/dbos-workflow-recovery",
				body:           strings.NewReader(`{"invalid": json}`),
				contentType:    "application/json",
				expectedStatus: http.StatusBadRequest,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				var req *http.Request
				var err error

				if tt.body != nil {
					req, err = http.NewRequest(tt.method, tt.endpoint, tt.body)
				} else {
					req, err = http.NewRequest(tt.method, tt.endpoint, nil)
				}
				if err != nil {
					t.Fatalf("Failed to create request: %v", err)
				}

				if tt.contentType != "" {
					req.Header.Set("Content-Type", tt.contentType)
				}

				resp, err := client.Do(req)
				if err != nil {
					t.Fatalf("Failed to make request: %v", err)
				}
				defer resp.Body.Close()

				if resp.StatusCode != tt.expectedStatus {
					body, _ := io.ReadAll(resp.Body)
					t.Errorf("Expected status code %d, got %d. Response: %s", tt.expectedStatus, resp.StatusCode, string(body))
				}

				if tt.validateResp != nil {
					tt.validateResp(t, resp)
				}
			})
		}
	})
}

func mustMarshal(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}
