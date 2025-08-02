package dbos

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestAdminServer(t *testing.T) {
	databaseURL := getDatabaseURL()

	t.Run("Admin server is not started by default", func(t *testing.T) {

		ctx, err := NewDBOSContext(Config{
			DatabaseURL: databaseURL,
			AppName:     "test-app",
		})
		if err != nil {
			t.Skipf("Failed to initialize DBOS: %v", err)
		}
		err = ctx.Launch()
		if err != nil {
			t.Skipf("Failed to initialize DBOS: %v", err)
		}

		// Ensure cleanup
		defer func() {
			if ctx != nil {
				ctx.Shutdown()
			}
		}()

		// Give time for any startup processes
		time.Sleep(100 * time.Millisecond)

		// Verify admin server is not running
		client := &http.Client{Timeout: 1 * time.Second}
		_, err = client.Get("http://localhost:3001" + healthCheckPath)
		if err == nil {
			t.Error("Expected request to fail when admin server is not started, but it succeeded")
		}

		// Verify the DBOS executor doesn't have an admin server instance
		if ctx == nil {
			t.Fatal("Expected DBOS instance to be created")
		}

		exec := ctx.(*dbosContext)
		if exec.adminServer != nil {
			t.Error("Expected admin server to be nil when not configured")
		}
	})

	t.Run("Admin server endpoints", func(t *testing.T) {
		// Launch DBOS with admin server once for all endpoint tests
		ctx, err := NewDBOSContext(Config{
			DatabaseURL: databaseURL,
			AppName:     "test-app",
			AdminServer: true,
		})
		if err != nil {
			t.Skipf("Failed to initialize DBOS with admin server: %v", err)
		}
		err = ctx.Launch()
		if err != nil {
			t.Skipf("Failed to initialize DBOS with admin server: %v", err)
		}

		// Ensure cleanup
		defer func() {
			if ctx != nil {
				ctx.Shutdown()
			}
		}()

		// Give the server a moment to start
		time.Sleep(100 * time.Millisecond)

		// Verify the DBOS executor has an admin server instance
		if ctx == nil {
			t.Fatal("Expected DBOS instance to be created")
		}

		exec := ctx.(*dbosContext)
		if exec.adminServer == nil {
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
				endpoint:       "http://localhost:3001" + healthCheckPath,
				expectedStatus: http.StatusOK,
			},
			{
				name:           "Recovery endpoint responds correctly with valid JSON",
				method:         "POST",
				endpoint:       "http://localhost:3001" + workflowRecoveryPath,
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
				endpoint:       "http://localhost:3001" + workflowRecoveryPath,
				expectedStatus: http.StatusMethodNotAllowed,
			},
			{
				name:           "Recovery endpoint rejects invalid JSON",
				method:         "POST",
				endpoint:       "http://localhost:3001" + workflowRecoveryPath,
				body:           strings.NewReader(`{"invalid": json}`),
				contentType:    "application/json",
				expectedStatus: http.StatusBadRequest,
			},
			{
				name:           "Queue metadata endpoint responds correctly",
				method:         "GET",
				endpoint:       "http://localhost:3001" + workflowQueuesMetadataPath,
				expectedStatus: http.StatusOK,
				validateResp: func(t *testing.T, resp *http.Response) {
					var queueMetadata []queueMetadata
					if err := json.NewDecoder(resp.Body).Decode(&queueMetadata); err != nil {
						t.Errorf("Failed to decode response as QueueMetadata array: %v", err)
					}
					if queueMetadata == nil {
						t.Error("Expected non-nil queue metadata array")
					}
					// Should contain at least the internal queue
					if len(queueMetadata) == 0 {
						t.Error("Expected at least one queue in metadata")
					}
					// Verify internal queue fields
					foundInternalQueue := false
					for _, queue := range queueMetadata {
						if queue.Name == _DBOS_INTERNAL_QUEUE_NAME { // Internal queue name
							foundInternalQueue = true
							if queue.Concurrency != nil {
								t.Errorf("Expected internal queue to have no concurrency limit, but got %v", *queue.Concurrency)
							}
							if queue.WorkerConcurrency != nil {
								t.Errorf("Expected internal queue to have no worker concurrency limit, but got %v", *queue.WorkerConcurrency)
							}
							if queue.RateLimit != nil {
								t.Error("Expected internal queue to have no rate limit")
							}
							break
						}
					}
					if !foundInternalQueue {
						t.Error("Expected to find internal queue in metadata")
					}
				},
			},
			{
				name:           "Queue metadata endpoint rejects invalid methods",
				method:         "POST",
				endpoint:       "http://localhost:3001" + workflowQueuesMetadataPath,
				expectedStatus: http.StatusMethodNotAllowed,
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
