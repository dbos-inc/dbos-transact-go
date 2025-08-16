package dbos

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdminServer(t *testing.T) {
	databaseURL := getDatabaseURL()

	t.Run("Admin server is not started by default", func(t *testing.T) {
		ctx, err := NewDBOSContext(Config{
			DatabaseURL: databaseURL,
			AppName:     "test-app",
		})
		require.NoError(t, err)

		err = ctx.Launch()
		require.NoError(t, err)
		// Ensure cleanup
		defer func() {
			if ctx != nil {
				ctx.Cancel()
			}
		}()

		// Give time for any startup processes
		time.Sleep(100 * time.Millisecond)

		// Verify admin server is not running
		client := &http.Client{Timeout: 1 * time.Second}
		_, err = client.Get(fmt.Sprintf("http://localhost:3001/%s", strings.TrimPrefix(_HEALTHCHECK_PATTERN, "GET /")))
		require.Error(t, err, "Expected request to fail when admin server is not started")

		// Verify the DBOS executor doesn't have an admin server instance
		require.NotNil(t, ctx, "Expected DBOS instance to be created")

		exec := ctx.(*dbosContext)
		require.Nil(t, exec.adminServer, "Expected admin server to be nil when not configured")
	})

	t.Run("Admin server endpoints", func(t *testing.T) {
		resetTestDatabase(t, databaseURL)
		// Launch DBOS with admin server once for all endpoint tests
		ctx, err := NewDBOSContext(Config{
			DatabaseURL: databaseURL,
			AppName:     "test-app",
			AdminServer: true,
		})

		require.NoError(t, err)
		testWorkflow := func(dbosCtx DBOSContext, input string) (string, error) {
			return "result-" + input, nil
		}
		RegisterWorkflow(ctx, testWorkflow)

		err = ctx.Launch()
		require.NoError(t, err)

		// Ensure cleanup
		defer func() {
			if ctx != nil {
				ctx.Cancel()
			}
		}()

		// Give the server a moment to start
		time.Sleep(100 * time.Millisecond)

		// Verify the DBOS executor has an admin server instance
		require.NotNil(t, ctx, "Expected DBOS instance to be created")

		exec := ctx.(*dbosContext)
		require.NotNil(t, exec.adminServer, "Expected admin server to be created in DBOS instance")

		client := &http.Client{Timeout: 5 * time.Second}

		type adminServerTestCase struct {
			name           string
			method         string
			endpoint       string
			body           io.Reader
			contentType    string
			expectedStatus int
			validateResp   func(t *testing.T, resp *http.Response)
		}

		tests := []adminServerTestCase{
			{
				name:           "Health endpoint responds correctly",
				method:         "GET",
				endpoint:       fmt.Sprintf("http://localhost:3001/%s", strings.TrimPrefix(_HEALTHCHECK_PATTERN, "GET /")),
				expectedStatus: http.StatusOK,
			},
			{
				name:           "Recovery endpoint responds correctly with valid JSON",
				method:         "POST",
				endpoint:       fmt.Sprintf("http://localhost:3001/%s", strings.TrimPrefix(_WORKFLOW_RECOVERY_PATTERN, "POST /")),
				body:           bytes.NewBuffer(mustMarshal([]string{"executor1", "executor2"})),
				contentType:    "application/json",
				expectedStatus: http.StatusOK,
				validateResp: func(t *testing.T, resp *http.Response) {
					var workflowIDs []string
					err := json.NewDecoder(resp.Body).Decode(&workflowIDs)
					require.NoError(t, err, "Failed to decode response as JSON array")
					assert.NotNil(t, workflowIDs, "Expected non-nil workflow IDs array")
				},
			},
			{
				name:           "Recovery endpoint rejects invalid JSON",
				method:         "POST",
				endpoint:       fmt.Sprintf("http://localhost:3001/%s", strings.TrimPrefix(_WORKFLOW_RECOVERY_PATTERN, "POST /")),
				body:           strings.NewReader(`{"invalid": json}`),
				contentType:    "application/json",
				expectedStatus: http.StatusBadRequest,
			},
			{
				name:           "Queue metadata endpoint responds correctly",
				method:         "GET",
				endpoint:       fmt.Sprintf("http://localhost:3001/%s", strings.TrimPrefix(_WORKFLOW_QUEUES_METADATA_PATTERN, "GET /")),
				expectedStatus: http.StatusOK,
				validateResp: func(t *testing.T, resp *http.Response) {
					var queueMetadata []WorkflowQueue
					err := json.NewDecoder(resp.Body).Decode(&queueMetadata)
					require.NoError(t, err, "Failed to decode response as QueueMetadata array")
					assert.NotNil(t, queueMetadata, "Expected non-nil queue metadata array")
					// Should contain at least the internal queue
					assert.Greater(t, len(queueMetadata), 0, "Expected at least one queue in metadata")
					// Verify internal queue fields
					foundInternalQueue := false
					for _, queue := range queueMetadata {
						if queue.Name == _DBOS_INTERNAL_QUEUE_NAME { // Internal queue name
							foundInternalQueue = true
							assert.Nil(t, queue.GlobalConcurrency, "Expected internal queue to have no concurrency limit")
							assert.Nil(t, queue.WorkerConcurrency, "Expected internal queue to have no worker concurrency limit")
							assert.Nil(t, queue.RateLimit, "Expected internal queue to have no rate limit")
							break
						}
					}
					assert.True(t, foundInternalQueue, "Expected to find internal queue in metadata")
				},
			},
			{
				name:           "Workflows endpoint accepts valid request and tests time filters",
				method:         "POST",
				endpoint:       fmt.Sprintf("http://localhost:3001/%s", strings.TrimPrefix(_WORKFLOWS_PATTERN, "POST /")),
				body:           nil, // Will be set during test execution
				contentType:    "application/json",
				expectedStatus: http.StatusOK,
				validateResp: func(t *testing.T, resp *http.Response) {
					// This will be overridden in the test
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				// Special handling for workflows time filter test
				if tt.name == "List endpoints time filtering" {
					// Create first workflow
					handle1, err := RunAsWorkflow(ctx, testWorkflow, "workflow1")
					require.NoError(t, err, "Failed to create first workflow")
					result1, err := handle1.GetResult()
					require.NoError(t, err, "Failed to get first workflow result")
					assert.Equal(t, "result-workflow1", result1)
					workflowID1 := handle1.GetWorkflowID()

					// Record time between workflows
					timeBetween := time.Now()

					// Wait 1 second
					time.Sleep(1 * time.Second)

					// Create second workflow
					handle2, err := RunAsWorkflow(ctx, testWorkflow, "workflow2")
					require.NoError(t, err, "Failed to create second workflow")
					result2, err := handle2.GetResult()
					require.NoError(t, err, "Failed to get second workflow result")
					assert.Equal(t, "result-workflow2", result2)
					workflowID2 := handle2.GetWorkflowID()

					// Test 1: Query with start_time before timeBetween (should get both workflows)
					reqBody1 := map[string]any{
						"start_time": timeBetween.Add(-2 * time.Second).Format(time.RFC3339),
						"limit":      10,
					}
					req1, err := http.NewRequest(tt.method, tt.endpoint, bytes.NewBuffer(mustMarshal(reqBody1)))
					require.NoError(t, err, "Failed to create request 1")
					req1.Header.Set("Content-Type", tt.contentType)

					resp1, err := client.Do(req1)
					require.NoError(t, err, "Failed to make request 1")
					defer resp1.Body.Close()

					assert.Equal(t, tt.expectedStatus, resp1.StatusCode)

					var workflows1 []map[string]any
					err = json.NewDecoder(resp1.Body).Decode(&workflows1)
					require.NoError(t, err, "Failed to decode workflows response 1")

					// Should have exactly 2 workflows that we just created
					assert.Equal(t, 2, len(workflows1), "Expected exactly 2 workflows with start_time before timeBetween")

					// Verify timestamps are epoch milliseconds
					timeBetweenMillis := timeBetween.UnixMilli()
					for _, wf := range workflows1 {
						_, ok := wf["CreatedAt"].(float64)
						require.True(t, ok, "CreatedAt should be a number")
					}
					// Verify the timestamp is around timeBetween (within 2 seconds before or after)
					assert.Less(t, int64(workflows1[0]["CreatedAt"].(float64)), timeBetweenMillis, "first workflow CreatedAt should be before timeBetween")
					assert.Greater(t, int64(workflows1[1]["CreatedAt"].(float64)), timeBetweenMillis, "second workflow CreatedAt should be before timeBetween")

					// Verify both workflow IDs are present
					foundIDs1 := make(map[string]bool)
					for _, wf := range workflows1 {
						id, ok := wf["WorkflowUUID"].(string)
						require.True(t, ok, "WorkflowUUID should be a string")
						foundIDs1[id] = true
					}
					assert.True(t, foundIDs1[workflowID1], "Expected to find first workflow ID in results")
					assert.True(t, foundIDs1[workflowID2], "Expected to find second workflow ID in results")

					// Test 2: Query with start_time after timeBetween (should get only second workflow)
					reqBody2 := map[string]any{
						"start_time": timeBetween.Format(time.RFC3339),
						"limit":      10,
					}
					req2, err := http.NewRequest(tt.method, tt.endpoint, bytes.NewBuffer(mustMarshal(reqBody2)))
					require.NoError(t, err, "Failed to create request 2")
					req2.Header.Set("Content-Type", tt.contentType)

					resp2, err := client.Do(req2)
					require.NoError(t, err, "Failed to make request 2")
					defer resp2.Body.Close()

					assert.Equal(t, tt.expectedStatus, resp2.StatusCode)

					var workflows2 []map[string]any
					err = json.NewDecoder(resp2.Body).Decode(&workflows2)
					require.NoError(t, err, "Failed to decode workflows response 2")

					// Should have exactly 1 workflow (the second one)
					assert.Equal(t, 1, len(workflows2), "Expected exactly 1 workflow with start_time after timeBetween")

					// Verify it's the second workflow
					id2, ok := workflows2[0]["WorkflowUUID"].(string)
					require.True(t, ok, "WorkflowUUID should be a string")
					assert.Equal(t, workflowID2, id2, "Expected second workflow ID in results")

					// Also test end_time filter
					reqBody3 := map[string]any{
						"end_time": timeBetween.Format(time.RFC3339),
						"limit":    10,
					}
					req3, err := http.NewRequest(tt.method, tt.endpoint, bytes.NewBuffer(mustMarshal(reqBody3)))
					require.NoError(t, err, "Failed to create request 3")
					req3.Header.Set("Content-Type", tt.contentType)

					resp3, err := client.Do(req3)
					require.NoError(t, err, "Failed to make request 3")
					defer resp3.Body.Close()

					assert.Equal(t, tt.expectedStatus, resp3.StatusCode)

					var workflows3 []map[string]any
					err = json.NewDecoder(resp3.Body).Decode(&workflows3)
					require.NoError(t, err, "Failed to decode workflows response 3")

					// Should have exactly 1 workflow (the first one)
					assert.Equal(t, 1, len(workflows3), "Expected exactly 1 workflow with end_time before timeBetween")

					// Verify it's the first workflow
					id3, ok := workflows3[0]["WorkflowUUID"].(string)
					require.True(t, ok, "WorkflowUUID should be a string")
					assert.Equal(t, workflowID1, id3, "Expected first workflow ID in results")

					// Test 4: Query with empty body (should return all workflows)
					req4, err := http.NewRequest(tt.method, tt.endpoint, nil)
					require.NoError(t, err, "Failed to create request 4")

					resp4, err := client.Do(req4)
					require.NoError(t, err, "Failed to make request 4")
					defer resp4.Body.Close()

					assert.Equal(t, tt.expectedStatus, resp4.StatusCode)

					var workflows4 []map[string]any
					err = json.NewDecoder(resp4.Body).Decode(&workflows4)
					require.NoError(t, err, "Failed to decode workflows response 4")

					// Should have exactly 2 workflows (both that we created)
					assert.Equal(t, 2, len(workflows4), "Expected exactly 2 workflows with empty body")

					// Verify both workflow IDs are present
					foundIDs4 := make(map[string]bool)
					for _, wf := range workflows4 {
						id, ok := wf["WorkflowUUID"].(string)
						require.True(t, ok, "WorkflowUUID should be a string")
						foundIDs4[id] = true
					}
					assert.True(t, foundIDs4[workflowID1], "Expected to find first workflow ID in empty body results")
					assert.True(t, foundIDs4[workflowID2], "Expected to find second workflow ID in empty body results")

					return // Skip the normal test flow
				}

				// Normal test flow for other tests
				var req *http.Request
				var err error

				if tt.body != nil {
					req, err = http.NewRequest(tt.method, tt.endpoint, tt.body)
				} else {
					req, err = http.NewRequest(tt.method, tt.endpoint, nil)
				}
				require.NoError(t, err, "Failed to create request")

				if tt.contentType != "" {
					req.Header.Set("Content-Type", tt.contentType)
				}

				resp, err := client.Do(req)
				require.NoError(t, err, "Failed to make request")
				defer resp.Body.Close()

				assert.Equal(t, tt.expectedStatus, resp.StatusCode)

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
