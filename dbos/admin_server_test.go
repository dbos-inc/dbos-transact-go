package dbos

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
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
				ctx.Shutdown(1 * time.Minute)
			}
		}()

		// Verify admin server is not running
		client := &http.Client{Timeout: 1 * time.Second}
		_, err = client.Get(fmt.Sprintf("http://localhost:3001/%s", strings.TrimPrefix(_HEALTHCHECK_PATTERN, "GET /")))
		require.Error(t, err, "Expected request to fail when admin server is not started")

		// Verify the DBOS executor doesn't have an admin server instance
		require.NotNil(t, ctx, "Expected DBOS instance to be created")

		exec, ok := ctx.(*dbosContext)
		require.True(t, ok, "Expected ctx to be of type *dbosContext")
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

		err = ctx.Launch()
		require.NoError(t, err)

		// Ensure cleanup
		defer func() {
			if ctx != nil {
				ctx.Shutdown(1 * time.Minute)
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
				name:     "Workflows endpoint accepts all filters without error",
				method:   "POST",
				endpoint: fmt.Sprintf("http://localhost:3001/%s", strings.TrimPrefix(_WORKFLOWS_PATTERN, "POST /")),
				body: bytes.NewBuffer(mustMarshal(map[string]any{
					"workflow_uuids":      []string{"test-id-1", "test-id-2"},
					"authenticated_user":  "test-user",
					"start_time":          time.Now().Add(-24 * time.Hour).Format(time.RFC3339Nano),
					"end_time":            time.Now().Format(time.RFC3339Nano),
					"status":              "PENDING",
					"application_version": "v1.0.0",
					"workflow_name":       "testWorkflow",
					"limit":               100,
					"offset":              0,
					"sort_desc":           true,
					"workflow_id_prefix":  "test-",
					"load_input":          true,
					"load_output":         true,
					"queue_name":          "test-queue",
				})),
				contentType:    "application/json",
				expectedStatus: http.StatusOK,
				validateResp: func(t *testing.T, resp *http.Response) {
					var workflows []map[string]any
					err := json.NewDecoder(resp.Body).Decode(&workflows)
					require.NoError(t, err, "Failed to decode workflows response")
					// We expect an empty array -- there's no workflow in the db
					assert.NotNil(t, workflows, "Expected non-nil workflows array")
					assert.Empty(t, workflows, "Expected empty workflows array")
				},
			},
			{
				name:           "Get single workflow returns 404 for non-existent workflow",
				method:         "GET",
				endpoint:       "http://localhost:3001/workflow/non-existent-workflow-id",
				expectedStatus: http.StatusNotFound,
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

	t.Run("List workflows input/output values", func(t *testing.T) {
		resetTestDatabase(t, databaseURL)
		ctx, err := NewDBOSContext(Config{
			DatabaseURL: databaseURL,
			AppName:     "test-app",
			AdminServer: true,
		})
		require.NoError(t, err)

		// Define a custom struct for testing
		type TestStruct struct {
			Name  string `json:"name"`
			Value int    `json:"value"`
		}

		// Test workflow with int input/output
		intWorkflow := func(dbosCtx DBOSContext, input int) (int, error) {
			return input * 2, nil
		}
		RegisterWorkflow(ctx, intWorkflow)

		// Test workflow with empty string input/output
		emptyStringWorkflow := func(dbosCtx DBOSContext, input string) (string, error) {
			return "", nil
		}
		RegisterWorkflow(ctx, emptyStringWorkflow)

		// Test workflow with struct input/output
		structWorkflow := func(dbosCtx DBOSContext, input TestStruct) (TestStruct, error) {
			return TestStruct{Name: "output-" + input.Name, Value: input.Value * 2}, nil
		}
		RegisterWorkflow(ctx, structWorkflow)

		err = ctx.Launch()
		require.NoError(t, err)

		// Ensure cleanup
		defer func() {
			if ctx != nil {
				ctx.Shutdown(1 * time.Minute)
			}
		}()

		// Give the server a moment to start
		time.Sleep(100 * time.Millisecond)

		client := &http.Client{Timeout: 5 * time.Second}
		endpoint := fmt.Sprintf("http://localhost:3001/%s", strings.TrimPrefix(_WORKFLOWS_PATTERN, "POST /"))

		// Create workflows with different input/output types
		// 1. Integer workflow
		intHandle, err := RunAsWorkflow(ctx, intWorkflow, 42)
		require.NoError(t, err, "Failed to create int workflow")
		intResult, err := intHandle.GetResult()
		require.NoError(t, err, "Failed to get int workflow result")
		assert.Equal(t, 84, intResult)

		// 2. Empty string workflow
		emptyStringHandle, err := RunAsWorkflow(ctx, emptyStringWorkflow, "")
		require.NoError(t, err, "Failed to create empty string workflow")
		emptyStringResult, err := emptyStringHandle.GetResult()
		require.NoError(t, err, "Failed to get empty string workflow result")
		assert.Equal(t, "", emptyStringResult)

		// 3. Struct workflow
		structInput := TestStruct{Name: "test", Value: 10}
		structHandle, err := RunAsWorkflow(ctx, structWorkflow, structInput)
		require.NoError(t, err, "Failed to create struct workflow")
		structResult, err := structHandle.GetResult()
		require.NoError(t, err, "Failed to get struct workflow result")
		assert.Equal(t, TestStruct{Name: "output-test", Value: 20}, structResult)

		// Query workflows with input/output loading enabled
		// Filter by the workflow IDs we just created to avoid interference from other tests
		reqBody := map[string]any{
			"workflow_uuids": []string{
				intHandle.GetWorkflowID(),
				emptyStringHandle.GetWorkflowID(),
				structHandle.GetWorkflowID(),
			},
			"load_input":  true,
			"load_output": true,
			"limit":       10,
		}
		req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewBuffer(mustMarshal(reqBody)))
		require.NoError(t, err, "Failed to create request")
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		require.NoError(t, err, "Failed to make request")
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var workflows []map[string]any
		err = json.NewDecoder(resp.Body).Decode(&workflows)
		require.NoError(t, err, "Failed to decode workflows response")

		// Should have exactly 3 workflows
		assert.Equal(t, 3, len(workflows), "Expected exactly 3 workflows")

		// Verify each workflow's input/output marshaling
		for _, wf := range workflows {
			wfID := wf["WorkflowUUID"].(string)

			// Check input and output fields exist and are strings (JSON marshaled)
			if wfID == intHandle.GetWorkflowID() {
				// Integer workflow: input and output should be marshaled as JSON strings
				inputStr, ok := wf["Input"].(string)
				require.True(t, ok, "Int workflow Input should be a string")
				assert.Equal(t, "42", inputStr, "Int workflow input should be marshaled as '42'")

				outputStr, ok := wf["Output"].(string)
				require.True(t, ok, "Int workflow Output should be a string")
				assert.Equal(t, "84", outputStr, "Int workflow output should be marshaled as '84'")

			} else if wfID == emptyStringHandle.GetWorkflowID() {
				// Empty string workflow: both input and output are empty strings
				// According to the logic, empty strings should not have Input/Output fields
				input, hasInput := wf["Input"]
				require.Equal(t, "", input)
				require.True(t, hasInput, "Empty string workflow should have Input field")

				output, hasOutput := wf["Output"]
				require.True(t, hasOutput, "Empty string workflow should have Output field")
				require.Equal(t, "", output)

			} else if wfID == structHandle.GetWorkflowID() {
				// Struct workflow: input and output should be marshaled as JSON strings
				inputStr, ok := wf["Input"].(string)
				require.True(t, ok, "Struct workflow Input should be a string")
				var inputStruct TestStruct
				err = json.Unmarshal([]byte(inputStr), &inputStruct)
				require.NoError(t, err, "Failed to unmarshal struct workflow input")
				assert.Equal(t, structInput, inputStruct, "Struct workflow input should match")

				outputStr, ok := wf["Output"].(string)
				require.True(t, ok, "Struct workflow Output should be a string")
				var outputStruct TestStruct
				err = json.Unmarshal([]byte(outputStr), &outputStruct)
				require.NoError(t, err, "Failed to unmarshal struct workflow output")
				assert.Equal(t, TestStruct{Name: "output-test", Value: 20}, outputStruct, "Struct workflow output should match")
			}
		}
	})

	t.Run("List endpoints time filtering", func(t *testing.T) {
		resetTestDatabase(t, databaseURL)
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
				ctx.Shutdown(1 * time.Minute)
			}
		}()

		client := &http.Client{Timeout: 5 * time.Second}
		endpoint := fmt.Sprintf("http://localhost:3001/%s", strings.TrimPrefix(_WORKFLOWS_PATTERN, "POST /"))

		// Create first workflow
		handle1, err := RunAsWorkflow(ctx, testWorkflow, "workflow1")
		require.NoError(t, err, "Failed to create first workflow")
		workflowID1 := handle1.GetWorkflowID()

		// Wait for first workflow to complete
		result1, err := handle1.GetResult()
		require.NoError(t, err, "Failed to get first workflow result")
		assert.Equal(t, "result-workflow1", result1)

		// Record time between workflows
		timeBetween := time.Now()
		time.Sleep(500 * time.Millisecond)

		// Create second workflow
		handle2, err := RunAsWorkflow(ctx, testWorkflow, "workflow2")
		require.NoError(t, err, "Failed to create second workflow")
		result2, err := handle2.GetResult()
		require.NoError(t, err, "Failed to get second workflow result")
		assert.Equal(t, "result-workflow2", result2)
		workflowID2 := handle2.GetWorkflowID()

		// Test 1: Query with start_time before timeBetween (should get both workflows)
		reqBody1 := map[string]any{
			"start_time": timeBetween.Add(-2 * time.Second).Format(time.RFC3339Nano),
			"limit":      10,
		}
		req1, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewBuffer(mustMarshal(reqBody1)))
		require.NoError(t, err, "Failed to create request 1")
		req1.Header.Set("Content-Type", "application/json")

		resp1, err := client.Do(req1)
		require.NoError(t, err, "Failed to make request 1")
		defer resp1.Body.Close()

		assert.Equal(t, http.StatusOK, resp1.StatusCode)

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
			"start_time": timeBetween.Format(time.RFC3339Nano),
			"limit":      10,
		}
		req2, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewBuffer(mustMarshal(reqBody2)))
		require.NoError(t, err, "Failed to create request 2")
		req2.Header.Set("Content-Type", "application/json")

		resp2, err := client.Do(req2)
		require.NoError(t, err, "Failed to make request 2")
		defer resp2.Body.Close()

		assert.Equal(t, http.StatusOK, resp2.StatusCode)

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
			"end_time": timeBetween.Format(time.RFC3339Nano),
			"limit":    10,
		}
		req3, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewBuffer(mustMarshal(reqBody3)))
		require.NoError(t, err, "Failed to create request 3")
		req3.Header.Set("Content-Type", "application/json")

		resp3, err := client.Do(req3)
		require.NoError(t, err, "Failed to make request 3")
		defer resp3.Body.Close()

		assert.Equal(t, http.StatusOK, resp3.StatusCode)

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
		req4, err := http.NewRequest(http.MethodPost, endpoint, nil)
		require.NoError(t, err, "Failed to create request 4")

		resp4, err := client.Do(req4)
		require.NoError(t, err, "Failed to make request 4")
		defer resp4.Body.Close()

		assert.Equal(t, http.StatusOK, resp4.StatusCode)

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
	})

	t.Run("TestDeactivate", func(t *testing.T) {
		t.Run("Deactivate stops workflow scheduler", func(t *testing.T) {
			resetTestDatabase(t, databaseURL)
			ctx, err := NewDBOSContext(Config{
				DatabaseURL: databaseURL,
				AppName:     "test-app",
				AdminServer: true,
			})
			require.NoError(t, err)

			// Track scheduled workflow executions
			var executionCount atomic.Int32

			// Register a scheduled workflow that runs every second
			RegisterWorkflow(ctx, func(dbosCtx DBOSContext, scheduledTime time.Time) (string, error) {
				executionCount.Add(1)
				return fmt.Sprintf("executed at %v", scheduledTime), nil
			}, WithSchedule("* * * * * *")) // Every second

			err = ctx.Launch()
			require.NoError(t, err)

			client := &http.Client{Timeout: 5 * time.Second}

			// Ensure cleanup
			defer func() {
				if ctx != nil {
					ctx.Shutdown(1 * time.Minute)
				}
				if client.Transport != nil {
					client.Transport.(*http.Transport).CloseIdleConnections()
				}
			}()

			// Wait for 2-3 executions to verify scheduler is running
			require.Eventually(t, func() bool {
				return executionCount.Load() >= 2
			}, 3*time.Second, 100*time.Millisecond, "Expected at least 2 scheduled workflow executions")

			// Call deactivate endpoint
			endpoint := fmt.Sprintf("http://localhost:3001/%s", strings.TrimPrefix(_DEACTIVATE_PATTERN, "GET /"))
			req, err := http.NewRequest("GET", endpoint, nil)
			require.NoError(t, err, "Failed to create deactivate request")

			resp, err := client.Do(req)
			require.NoError(t, err, "Failed to call deactivate endpoint")
			defer resp.Body.Close()

			// Verify endpoint returned 200 OK
			assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected 200 OK from deactivate endpoint")

			// Verify response body
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err, "Failed to read response body")
			assert.Equal(t, "deactivated", string(body), "Expected 'deactivated' response body")

			// Record count after deactivate and wait
			countAfterDeactivate := executionCount.Load()
			time.Sleep(4 * time.Second) // Wait long enough for multiple executions if scheduler was still running

			// Verify no new executions occurred
			finalCount := executionCount.Load()
			assert.Equal(t, countAfterDeactivate, finalCount,
				"Expected no new scheduled workflows after deactivate (had %d before, %d after)",
				countAfterDeactivate, finalCount)
		})
	})

}

func mustMarshal(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}
