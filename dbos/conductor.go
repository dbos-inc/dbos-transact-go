package dbos

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const (
	_PING_INTERVAL          = 20 * time.Second
	_PING_TIMEOUT           = 30 * time.Second // Should be slightly greater than server's executorPingWait (25s)
	_INITIAL_RECONNECT_WAIT = 1 * time.Second
	_MAX_RECONNECT_WAIT     = 30 * time.Second
	_HANDSHAKE_TIMEOUT      = 10 * time.Second
)

// ConductorConfig contains configuration for the conductor
type ConductorConfig struct {
	url     string
	apiKey  string
	appName string
}

// Conductor manages the WebSocket connection to the DBOS conductor service
type Conductor struct {
	config         ConductorConfig
	conn           *websocket.Conn
	needsReconnect atomic.Bool
	wg             sync.WaitGroup
	stopOnce       sync.Once
	dbosCtx        *dbosContext

	// Connection parameters
	url           url.URL
	pingInterval  time.Duration
	pingTimeout   time.Duration
	reconnectWait time.Duration

	// writeMu protects concurrent writes to the WebSocket connection (pings + handling messages)
	writeMu sync.Mutex

	// pingCancel cancels the ping goroutine context
	pingCancel context.CancelFunc

	logger *slog.Logger
}

// Launch starts the conductor connection manager goroutine
func (c *Conductor) Launch() {
	c.logger.Info("Launching conductor")
	c.wg.Add(1)
	go c.run()
}

// NewConductor creates a new conductor instance
func NewConductor(config ConductorConfig, dbosCtx *dbosContext) (*Conductor, error) {
	if config.apiKey == "" {
		return nil, fmt.Errorf("conductor API key is required")
	}
	if config.url == "" {
		return nil, fmt.Errorf("conductor URL is required")
	}

	// Parse the base conductor URL
	baseURL, err := url.Parse(config.url)
	if err != nil {
		return nil, fmt.Errorf("invalid conductor URL: %w", err)
	}

	// Build WebSocket URL using net.JoinPath for proper path construction
	wsURL := url.URL{
		Scheme: baseURL.Scheme,
		Host:   baseURL.Host,
		Path:   baseURL.JoinPath("websocket", config.appName, config.apiKey).Path,
	}

	// Convert HTTP schemes to WebSocket schemes
	switch wsURL.Scheme {
	case "http":
		wsURL.Scheme = "ws"
	case "https":
		wsURL.Scheme = "wss"
	case "ws", "wss":
		// Already correct
	default:
		return nil, fmt.Errorf("unsupported URL scheme: %s (expected http, https, ws, or wss)", wsURL.Scheme)
	}

	c := &Conductor{
		config:        config,
		dbosCtx:       dbosCtx,
		url:           wsURL,
		pingInterval:  _PING_INTERVAL,
		pingTimeout:   _PING_TIMEOUT,
		reconnectWait: _INITIAL_RECONNECT_WAIT,
		logger:        dbosCtx.logger.With("service", "conductor"),
	}

	// Start with needsReconnect set to true so we connect on first run
	c.needsReconnect.Store(true)

	return c, nil
}

// Shutdown gracefully conductor
func (c *Conductor) Shutdown(timeout time.Duration) {
	c.stopOnce.Do(func() {
		if c.pingCancel != nil {
			c.pingCancel()
		}

		c.closeConn()

		done := make(chan struct{})
		go func() {
			c.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			c.logger.Info("Conductor shut down")
		case <-time.After(timeout):
			c.logger.Warn("Timeout waiting for conductor to shut down", "timeout", timeout)
		}
	})
}

// closeConn closes the connection and signals that reconnection is needed
func (c *Conductor) closeConn() {
	// Cancel ping goroutine first
	if c.pingCancel != nil {
		c.pingCancel()
		c.pingCancel = nil
	}

	// Acquire write mutex to ensure no concurrent writes during close
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.conn != nil {
		if err := c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
			c.logger.Warn("Failed to set write deadline", "error", err)
		}
		err := c.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "shutting down"))
		if err != nil {
			c.logger.Warn("Failed to send close message", "error", err)
		}
		err = c.conn.Close()
		if err != nil {
			c.logger.Warn("Failed to close connection", "error", err)
		}
		c.conn = nil
	}
	// Signal that we need to reconnect
	c.needsReconnect.Store(true)
}

// run manages the WebSocket connection lifecycle with reconnection
func (c *Conductor) run() {
	defer c.wg.Done()

	for {
		// Check if the context has been cancelled
		select {
		case <-c.dbosCtx.Done():
			c.logger.Info("DBOS context done, stopping conductor", "cause", context.Cause(c.dbosCtx))
			c.closeConn()
			return
		default:
		}

		// Connect if reconnection is needed
		if c.needsReconnect.Load() {
			if err := c.connect(); err != nil {
				c.logger.Warn("Failed to connect to conductor", "error", err)
				select {
				case <-c.dbosCtx.Done():
					c.logger.Info("DBOS context done, stopping conductor", "cause", context.Cause(c.dbosCtx))
					return
				case <-time.After(c.reconnectWait):
					// Exponential backoff up to max wait
					if c.reconnectWait < _MAX_RECONNECT_WAIT {
						c.reconnectWait *= 2
					}
					continue
				}
			}
			// Reset reconnect wait and clear reconnect flag on successful connection
			c.reconnectWait = _INITIAL_RECONNECT_WAIT
			c.needsReconnect.Store(false)
		}

		// This shouldn't happen but check anyway
		if c.conn == nil {
			c.needsReconnect.Store(true)
			continue
		}

		// Read message (will timeout based on read deadline set in connect)
		messageType, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				c.logger.Warn("Unexpected WebSocket close", "error", err)
			} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				c.logger.Debug("Read deadline reached", "error", err)
			} else {
				c.logger.Debug("Connection closed", "error", err)
			}
			// Close connection to trigger reconnection
			c.closeConn()
			continue
		}

		// Only accept text messages
		if messageType != websocket.TextMessage {
			c.logger.Warn("Received unexpected message type, forcing reconnection", "type", messageType)
			c.closeConn()
			continue
		}

		ht := time.Now()
		if err := c.handleMessage(message); err != nil {
			c.logger.Error("Failed to handle message", "error", err)
		}
		c.logger.Debug("Handled message", "message", messageType, "latency_us", time.Since(ht).Microseconds())
	}
}

// connect establishes a WebSocket connection to the conductor
func (c *Conductor) connect() error {
	c.logger.Debug("Connecting to conductor")

	dialer := websocket.Dialer{
		HandshakeTimeout: _HANDSHAKE_TIMEOUT,
	}

	conn, _, err := dialer.Dial(c.url.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to dial conductor: %w", err)
	}

	// Set initial read deadline
	if err := conn.SetReadDeadline(time.Now().Add(c.pingTimeout)); err != nil {
		cErr := conn.Close()
		if cErr != nil {
			c.logger.Warn("Failed to close connection", "error", cErr)
		}
		return fmt.Errorf("failed to set read deadline: %w", err)
	}

	// Set pong handler to reset read deadline
	conn.SetPongHandler(func(appData string) error {
		c.logger.Debug("Received pong from conductor")
		return conn.SetReadDeadline(time.Now().Add(c.pingTimeout))
	})

	// Store the connection
	c.conn = conn

	// Create a cancellable context for the ping goroutine
	pingCtx, pingCancel := context.WithCancel(c.dbosCtx)
	c.pingCancel = pingCancel

	// Start ping goroutine
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(c.pingInterval)
		defer ticker.Stop()

		for {
			select {
			case <-pingCtx.Done():
				c.logger.Debug("Exiting Conductor ping goroutine", "cause", context.Cause(pingCtx))
				return
			case <-ticker.C:
				if err := c.ping(); err != nil {
					c.logger.Warn("Ping failed, signaling reconnection", "error", err)
					// Signal that we need to reconnect and exit ping goroutine
					c.needsReconnect.Store(true)
					return
				}
			}
		}
	}()

	c.logger.Info("Connected to DBOS conductor")
	return nil
}

// ping sends a ping to the conductor
func (c *Conductor) ping() error {
	if c.conn == nil {
		return fmt.Errorf("no connection")
	}

	c.logger.Debug("Sending ping to conductor")

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if err := c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		c.logger.Warn("Failed to set write deadline for ping", "error", err)
	}
	if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
		return fmt.Errorf("failed to send ping: %w", err)
	}
	if err := c.conn.SetWriteDeadline(time.Time{}); err != nil {
		c.logger.Warn("Failed to clear write deadline", "error", err)
	}

	return nil
}

// handleMessage processes an incoming message from the conductor
func (c *Conductor) handleMessage(data []byte) error {
	var base baseMessage
	if err := json.Unmarshal(data, &base); err != nil {
		c.logger.Error("Failed to parse message", "error", err)
		return fmt.Errorf("failed to parse base message: %w", err)
	}
	c.logger.Debug("Received message", "type", base.Type, "request_id", base.RequestID)

	switch base.Type {
	case executorInfo:
		return c.handleExecutorInfoRequest(data, base.RequestID)
	case recoveryMessage:
		return c.handleRecoveryRequest(data, base.RequestID)
	case cancelWorkflowMessage:
		return c.handleCancelWorkflowRequest(data, base.RequestID)
	case resumeWorkflowMessage:
		return c.handleResumeWorkflowRequest(data, base.RequestID)
	case listWorkflowsMessage:
		return c.handleListWorkflowsRequest(data, base.RequestID)
	case listQueuedWorkflowsMessage:
		return c.handleListQueuedWorkflowsRequest(data, base.RequestID)
	case listStepsMessage:
		return c.handleListStepsRequest(data, base.RequestID)
	case getWorkflowMessage:
		return c.handleGetWorkflowRequest(data, base.RequestID)
	case forkWorkflowMessage:
		return c.handleForkWorkflowRequest(data, base.RequestID)
	case existPendingWorkflowsMessage:
		return c.handleExistPendingWorkflowsRequest(data, base.RequestID)
	case retentionMessage:
		return c.handleRetentionRequest(data, base.RequestID)
	default:
		c.logger.Warn("Unknown message type", "type", base.Type)
		return c.handleUnknownMessageType(base.RequestID, base.Type, "Unknown message type")
	}
}

// handleExecutorInfoRequest handles executor info requests from the conductor
func (c *Conductor) handleExecutorInfoRequest(data []byte, requestID string) error {
	var req executorInfoRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse executor info request", "error", err)
		return fmt.Errorf("failed to parse executor info request: %w", err)
	}
	c.logger.Debug("Handling executor info request", "request_id", req)

	hostname, err := os.Hostname()
	if err != nil {
		c.logger.Error("Failed to get hostname", "error", err)
		return fmt.Errorf("failed to get hostname: %w", err)
	}

	response := executorInfoResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      executorInfo,
				RequestID: requestID,
			},
		},
		ExecutorID:         c.dbosCtx.GetExecutorID(),
		ApplicationVersion: c.dbosCtx.GetApplicationVersion(),
		Hostname:           &hostname,
	}

	return c.sendResponse(response, "executor info response")
}

// handleRecoveryRequest handles recovery requests from the conductor
func (c *Conductor) handleRecoveryRequest(data []byte, requestID string) error {
	var req recoveryConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse recovery request", "error", err)
		return fmt.Errorf("failed to parse recovery request: %w", err)
	}
	c.logger.Debug("Handling recovery request", "executor_ids", req.ExecutorIDs, "request_id", requestID)

	// Recover pending workflows using the provided executor IDs
	success := true
	var errorMsg *string

	_, err := recoverPendingWorkflows(c.dbosCtx, req.ExecutorIDs)
	if err != nil {
		c.logger.Error("Failed to recover pending workflows", "executor_ids", req.ExecutorIDs, "error", err)
		errStr := fmt.Sprintf("failed to recover pending workflows: %v", err)
		errorMsg = &errStr
		success = false
	} else {
		c.logger.Info("Successfully recovered pending workflows", "executor_ids", req.ExecutorIDs)
	}

	response := recoveryConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      recoveryMessage,
				RequestID: requestID,
			},
			ErrorMessage: errorMsg,
		},
		Success: success,
	}

	return c.sendResponse(response, "recovery response")
}

// handleCancelWorkflowRequest handles cancel workflow requests from the conductor
func (c *Conductor) handleCancelWorkflowRequest(data []byte, requestID string) error {
	var req cancelWorkflowConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse cancel workflow request", "error", err)
		return fmt.Errorf("failed to parse cancel workflow request: %w", err)
	}
	c.logger.Debug("Handling cancel workflow request", "workflow_id", req.WorkflowID, "request_id", requestID)

	// Cancel the workflow using the dbosContext
	success := true
	var errorMsg *string

	if err := c.dbosCtx.CancelWorkflow(c.dbosCtx, req.WorkflowID); err != nil {
		c.logger.Error("Failed to cancel workflow", "workflow_id", req.WorkflowID, "error", err)
		errStr := fmt.Sprintf("failed to cancel workflow: %v", err)
		errorMsg = &errStr
		success = false
	} else {
		c.logger.Info("Successfully cancelled workflow", "workflow_id", req.WorkflowID)
	}

	response := cancelWorkflowConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      cancelWorkflowMessage,
				RequestID: requestID,
			},
			ErrorMessage: errorMsg,
		},
		Success: success,
	}

	return c.sendResponse(response, "cancel workflow response")
}

// handleResumeWorkflowRequest handles resume workflow requests from the conductor
func (c *Conductor) handleResumeWorkflowRequest(data []byte, requestID string) error {
	var req resumeWorkflowConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse resume workflow request", "error", err)
		return fmt.Errorf("failed to parse resume workflow request: %w", err)
	}
	c.logger.Debug("Handling resume workflow request", "workflow_id", req.WorkflowID, "request_id", requestID)

	// Resume the workflow using the dbosContext
	success := true
	var errorMsg *string

	_, err := c.dbosCtx.ResumeWorkflow(c.dbosCtx, req.WorkflowID)
	if err != nil {
		c.logger.Error("Failed to resume workflow", "workflow_id", req.WorkflowID, "error", err)
		errStr := fmt.Sprintf("failed to resume workflow: %v", err)
		errorMsg = &errStr
		success = false
	} else {
		c.logger.Info("Successfully resumed workflow", "workflow_id", req.WorkflowID)
	}

	response := resumeWorkflowConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      resumeWorkflowMessage,
				RequestID: requestID,
			},
			ErrorMessage: errorMsg,
		},
		Success: success,
	}

	return c.sendResponse(response, "resume workflow response")
}

// handleRetentionRequest handles retention policy requests from the conductor
func (c *Conductor) handleRetentionRequest(data []byte, requestID string) error {
	var req retentionConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse retention request", "error", err)
		return fmt.Errorf("failed to parse retention request: %w", err)
	}
	c.logger.Debug("Handling retention request", "request", req, "request_id", requestID)

	success := true
	var errorMsg *string

	// Handle garbage collection if parameters are provided
	if req.Body.GCCutoffEpochMs != nil || req.Body.GCRowsThreshold != nil {
		var cutoffMs *int64
		if req.Body.GCCutoffEpochMs != nil {
			ms := int64(*req.Body.GCCutoffEpochMs)
			cutoffMs = &ms
		}

		var rowsThreshold *int
		if req.Body.GCRowsThreshold != nil {
			rowsThreshold = req.Body.GCRowsThreshold
		}

		input := garbageCollectWorkflowsInput{
			cutoffEpochTimestampMs: cutoffMs,
			rowsThreshold:          rowsThreshold,
		}

		err := c.dbosCtx.systemDB.garbageCollectWorkflows(c.dbosCtx, input)
		if err != nil {
			c.logger.Error("Failed to garbage collect workflows", "error", err)
			errStr := fmt.Sprintf("failed to garbage collect workflows: %v", err)
			errorMsg = &errStr
			success = false
		} else {
			c.logger.Info("Successfully garbage collected workflows", "cutoff_ms", cutoffMs, "rows_threshold", rowsThreshold)
		}
	}

	// Handle timeout enforcement if parameter is provided and garbage collection succeeded
	if success && req.Body.TimeoutCutoffEpochMs != nil {
		cutoffTime := time.Unix(0, int64(*req.Body.TimeoutCutoffEpochMs)*int64(time.Millisecond))
		err := c.dbosCtx.systemDB.cancelAllBefore(c.dbosCtx, cutoffTime)
		if err != nil {
			c.logger.Error("Failed to timeout workflows", "cutoff_ms", *req.Body.TimeoutCutoffEpochMs, "error", err)
			errStr := fmt.Sprintf("failed to timeout workflows: %v", err)
			errorMsg = &errStr
			success = false
		} else {
			c.logger.Info("Successfully timed out workflows", "cutoff_ms", *req.Body.TimeoutCutoffEpochMs)
		}
	}

	response := retentionConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      retentionMessage,
				RequestID: requestID,
			},
			ErrorMessage: errorMsg,
		},
		Success: success,
	}

	return c.sendResponse(response, "retention response")
}

// sendResponse sends a response to the conductor via websocket
func (c *Conductor) sendResponse(response any, responseType string) error {
	if c.conn == nil {
		return fmt.Errorf("no connection")
	}

	data, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("failed to marshal %s: %w", responseType, err)
	}

	c.logger.Debug("Sending response", "type", responseType, "len", len(data))

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if err := c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		c.logger.Warn("Failed to set write deadline", "type", responseType, "error", err)
	}
	if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		c.logger.Error("Failed to send response", "type", responseType, "error", err)
		return fmt.Errorf("failed to send message: %w", err)
	}
	if err := c.conn.SetWriteDeadline(time.Time{}); err != nil {
		c.logger.Warn("Failed to clear write deadline", "type", responseType, "error", err)
	}

	return nil
}

// handleListWorkflowsRequest handles list workflows requests from the conductor
func (c *Conductor) handleListWorkflowsRequest(data []byte, requestID string) error {
	var req listWorkflowsConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse list workflows request", "error", err)
		return fmt.Errorf("failed to parse list workflows request: %w", err)
	}
	c.logger.Debug("Handling list workflows request", "request", req)

	// Build functional options for ListWorkflows
	var opts []ListWorkflowsOption
	opts = append(opts, WithLoadInput(req.Body.LoadInput))
	opts = append(opts, WithLoadOutput(req.Body.LoadOutput))
	opts = append(opts, WithSortDesc(req.Body.SortDesc))
	if len(req.Body.WorkflowUUIDs) > 0 {
		opts = append(opts, WithWorkflowIDs(req.Body.WorkflowUUIDs))
	}
	if req.Body.WorkflowName != nil {
		opts = append(opts, WithName(*req.Body.WorkflowName))
	}
	if req.Body.AuthenticatedUser != nil {
		opts = append(opts, WithUser(*req.Body.AuthenticatedUser))
	}
	if req.Body.ApplicationVersion != nil {
		opts = append(opts, WithAppVersion(*req.Body.ApplicationVersion))
	}
	if req.Body.Limit != nil {
		opts = append(opts, WithLimit(*req.Body.Limit))
	}
	if req.Body.Offset != nil {
		opts = append(opts, WithOffset(*req.Body.Offset))
	}
	if req.Body.StartTime != nil {
		opts = append(opts, WithStartTime(*req.Body.StartTime))
	}
	if req.Body.EndTime != nil {
		opts = append(opts, WithEndTime(*req.Body.EndTime))
	}
	if req.Body.Status != nil {
		opts = append(opts, WithStatus([]WorkflowStatusType{WorkflowStatusType(*req.Body.Status)}))
	}

	workflows, err := c.dbosCtx.ListWorkflows(c.dbosCtx, opts...)
	if err != nil {
		c.logger.Error("Failed to list workflows", "error", err)
		errorMsg := fmt.Sprintf("failed to list workflows: %v", err)
		response := listWorkflowsConductorResponse{
			baseResponse: baseResponse{
				baseMessage: baseMessage{
					Type:      listWorkflowsMessage,
					RequestID: requestID,
				},
				ErrorMessage: &errorMsg,
			},
			Output: []listWorkflowsConductorResponseBody{},
		}
		return c.sendResponse(response, "list workflows response")
	}

	// Prepare response payload
	formattedWorkflows := make([]listWorkflowsConductorResponseBody, len(workflows))
	for i, wf := range workflows {
		formattedWorkflows[i] = formatListWorkflowsResponseBody(wf)
	}

	response := listWorkflowsConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      listWorkflowsMessage,
				RequestID: requestID,
			},
		},
		Output: formattedWorkflows,
	}

	return c.sendResponse(response, "list workflows response")
}

// handleListQueuedWorkflowsRequest handles list queued workflows requests from the conductor
func (c *Conductor) handleListQueuedWorkflowsRequest(data []byte, requestID string) error {
	var req listWorkflowsConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse list queued workflows request", "error", err)
		return fmt.Errorf("failed to parse list queued workflows request: %w", err)
	}
	c.logger.Debug("Handling list queued workflows request", "request", req)

	// Build functional options for ListWorkflows
	var opts []ListWorkflowsOption
	opts = append(opts, WithLoadInput(req.Body.LoadInput))
	opts = append(opts, WithLoadOutput(false)) // Don't load output for queued workflows
	opts = append(opts, WithSortDesc(req.Body.SortDesc))

	// Add status filter for queued workflows
	queuedStatuses := make([]WorkflowStatusType, 0)
	if req.Body.Status != nil {
		// If a specific status is requested, use that status
		status := WorkflowStatusType(*req.Body.Status)
		if status != WorkflowStatusPending && status != WorkflowStatusEnqueued {
			c.logger.Warn("Received unexpected filtering status for listing queued workflows", "status", status)
		}
		queuedStatuses = append(queuedStatuses, status)
	}
	if len(queuedStatuses) > 0 {
		queuedStatuses = []WorkflowStatusType{WorkflowStatusPending, WorkflowStatusEnqueued}
	}
	opts = append(opts, WithStatus(queuedStatuses))

	if req.Body.WorkflowName != nil {
		opts = append(opts, WithName(*req.Body.WorkflowName))
	}
	if req.Body.Limit != nil {
		opts = append(opts, WithLimit(*req.Body.Limit))
	}
	if req.Body.Offset != nil {
		opts = append(opts, WithOffset(*req.Body.Offset))
	}
	if req.Body.StartTime != nil {
		opts = append(opts, WithStartTime(*req.Body.StartTime))
	}
	if req.Body.EndTime != nil {
		opts = append(opts, WithEndTime(*req.Body.EndTime))
	}
	if req.Body.QueueName != nil {
		opts = append(opts, WithQueueName(*req.Body.QueueName))
	}

	workflows, err := c.dbosCtx.ListWorkflows(c.dbosCtx, opts...)
	if err != nil {
		c.logger.Error("Failed to list queued workflows", "error", err)
		errorMsg := fmt.Sprintf("failed to list queued workflows: %v", err)
		response := listWorkflowsConductorResponse{
			baseResponse: baseResponse{
				baseMessage: baseMessage{
					Type:      listQueuedWorkflowsMessage,
					RequestID: requestID,
				},
				ErrorMessage: &errorMsg,
			},
			Output: []listWorkflowsConductorResponseBody{},
		}
		return c.sendResponse(response, "list workflows response")
	}

	// If no queue name was specified, only include workflows that have a queue name
	var filteredWorkflows []WorkflowStatus
	if req.Body.QueueName == nil {
		for _, wf := range workflows {
			if wf.QueueName != "" {
				filteredWorkflows = append(filteredWorkflows, wf)
			}
		}
	}

	// Prepare response payload
	formattedWorkflows := make([]listWorkflowsConductorResponseBody, len(filteredWorkflows))
	for i, wf := range filteredWorkflows {
		formattedWorkflows[i] = formatListWorkflowsResponseBody(wf)
	}

	response := listWorkflowsConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      listWorkflowsMessage,
				RequestID: requestID,
			},
		},
		Output: formattedWorkflows,
	}

	return c.sendResponse(response, "list workflows response")
}

// handleListStepsRequest handles list steps requests from the conductor
func (c *Conductor) handleListStepsRequest(data []byte, requestID string) error {
	var req listStepsConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse list steps request", "error", err)
		return fmt.Errorf("failed to parse list steps request: %w", err)
	}
	c.logger.Debug("Handling list steps request", "request", req)

	// Get workflow steps using the existing systemDB method
	steps, err := c.dbosCtx.systemDB.getWorkflowSteps(c.dbosCtx, req.WorkflowID)
	if err != nil {
		c.logger.Error("Failed to list workflow steps", "workflow_id", req.WorkflowID, "error", err)
		errorMsg := fmt.Sprintf("failed to list workflow steps: %v", err)
		response := listStepsConductorResponse{
			baseResponse: baseResponse{
				baseMessage: baseMessage{
					Type:      listStepsMessage,
					RequestID: requestID,
				},
				ErrorMessage: &errorMsg,
			},
			Output: nil,
		}
		return c.sendResponse(response, "list steps response")
	}

	// Convert steps to response format
	var formattedSteps *[]workflowStepsConductorResponseBody
	if steps != nil {
		stepsList := make([]workflowStepsConductorResponseBody, len(steps))
		for i, step := range steps {
			stepsList[i] = formatWorkflowStepsResponseBody(step)
		}
		formattedSteps = &stepsList
	}

	response := listStepsConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      listStepsMessage,
				RequestID: requestID,
			},
		},
		Output: formattedSteps,
	}

	return c.sendResponse(response, "list steps response")
}

// handleGetWorkflowRequest handles get workflow requests from the conductor
func (c *Conductor) handleGetWorkflowRequest(data []byte, requestID string) error {
	var req getWorkflowConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse get workflow request", "error", err)
		return fmt.Errorf("failed to parse get workflow request: %w", err)
	}
	c.logger.Debug("Handling get workflow request", "workflow_id", req.WorkflowID)

	// Get workflow using ListWorkflows with the specific workflow ID filter
	workflows, err := c.dbosCtx.ListWorkflows(c.dbosCtx, WithWorkflowIDs([]string{req.WorkflowID}))
	if err != nil {
		c.logger.Error("Failed to get workflow", "workflow_id", req.WorkflowID, "error", err)
		errorMsg := fmt.Sprintf("failed to get workflow: %v", err)
		response := getWorkflowConductorResponse{
			baseResponse: baseResponse{
				baseMessage: baseMessage{
					Type:      getWorkflowMessage,
					RequestID: requestID,
				},
				ErrorMessage: &errorMsg,
			},
			Output: nil,
		}
		return c.sendResponse(response, "get workflow response")
	}

	// Format the workflow for response
	var formattedWorkflow *listWorkflowsConductorResponseBody
	if len(workflows) > 0 {
		formatted := formatListWorkflowsResponseBody(workflows[0])
		formattedWorkflow = &formatted
	}

	response := getWorkflowConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      getWorkflowMessage,
				RequestID: requestID,
			},
		},
		Output: formattedWorkflow,
	}

	return c.sendResponse(response, "get workflow response")
}

// handleForkWorkflowRequest handles fork workflow requests from the conductor
func (c *Conductor) handleForkWorkflowRequest(data []byte, requestID string) error {
	var req forkWorkflowConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse fork workflow request", "error", err)
		return fmt.Errorf("failed to parse fork workflow request: %w", err)
	}
	c.logger.Debug("Handling fork workflow request", "request", req)

	// Build ForkWorkflowInput from the request
	// Validate StartStep to prevent integer overflow
	if req.Body.StartStep < 0 {
		return fmt.Errorf("invalid StartStep: cannot be negative")
	}
	input := ForkWorkflowInput{
		OriginalWorkflowID: req.Body.WorkflowID,
		StartStep:          uint(req.Body.StartStep), // #nosec G115 -- validated above
	}

	// Set optional fields
	if req.Body.NewWorkflowID != nil {
		input.ForkedWorkflowID = *req.Body.NewWorkflowID
	}
	if req.Body.ApplicationVersion != nil {
		input.ApplicationVersion = *req.Body.ApplicationVersion
	}

	// Execute the fork workflow
	handle, err := c.dbosCtx.ForkWorkflow(c.dbosCtx, input)
	var newWorkflowID *string
	var errorMsg *string

	if err != nil {
		c.logger.Error("Failed to fork workflow", "original_workflow_id", req.Body.WorkflowID, "error", err)
		errStr := fmt.Sprintf("failed to fork workflow: %v", err)
		errorMsg = &errStr
	} else {
		workflowID := handle.GetWorkflowID()
		newWorkflowID = &workflowID
		c.logger.Info("Successfully forked workflow", "original_workflow_id", req.Body.WorkflowID, "new_workflow_id", workflowID)
	}

	response := forkWorkflowConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      forkWorkflowMessage,
				RequestID: requestID,
			},
			ErrorMessage: errorMsg,
		},
		NewWorkflowID: newWorkflowID,
	}

	return c.sendResponse(response, "fork workflow response")
}

// handleExistPendingWorkflowsRequest handles exist pending workflows requests from the conductor
func (c *Conductor) handleExistPendingWorkflowsRequest(data []byte, requestID string) error {
	var req existPendingWorkflowsConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse exist pending workflows request", "error", err)
		return fmt.Errorf("failed to parse exist pending workflows request: %w", err)
	}
	c.logger.Debug("Handling exist pending workflows request", "executor_id", req.ExecutorID, "application_version", req.ApplicationVersion)

	// Use ListWorkflows to check for pending workflows with the specified executor ID and application version
	// Filter for pending status workflows
	opts := []ListWorkflowsOption{
		WithStatus([]WorkflowStatusType{WorkflowStatusPending}),
		WithLimit(1), // We only need to know if any exist, so limit to 1 for efficiency
		WithExecutorIDs([]string{req.ExecutorID}),
		WithAppVersion(req.ApplicationVersion),
	}

	workflows, err := c.dbosCtx.ListWorkflows(c.dbosCtx, opts...)
	var errorMsg *string
	if err != nil {
		c.logger.Error("Failed to check for pending workflows", "executor_id", req.ExecutorID, "application_version", req.ApplicationVersion, "error", err)
		errStr := fmt.Sprintf("failed to check for pending workflows: %v", err)
		errorMsg = &errStr
	}

	response := existPendingWorkflowsConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      existPendingWorkflowsMessage,
				RequestID: requestID,
			},
			ErrorMessage: errorMsg,
		},
		Exist: len(workflows) > 0,
	}

	return c.sendResponse(response, "exist pending workflows response")
}

// handleUnknownMessageType sends an error response for unknown message types
func (c *Conductor) handleUnknownMessageType(requestID string, msgType messageType, errorMsg string) error {
	if c.conn == nil {
		return fmt.Errorf("no connection")
	}

	response := baseResponse{
		baseMessage: baseMessage{
			Type:      msgType,
			RequestID: requestID,
		},
		ErrorMessage: &errorMsg,
	}

	data, err := json.Marshal(response)
	if err != nil {
		c.logger.Error("Failed to marshal error response", "error", err)
		return fmt.Errorf("failed to marshal error response: %w", err)
	}

	c.logger.Debug("Sending error response", "data", response)

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if err := c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		c.logger.Warn("Failed to set write deadline for error response", "error", err)
	}
	if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		c.logger.Error("Failed to send error response", "error", err)
		return fmt.Errorf("failed to send message: %w", err)
	}
	if err := c.conn.SetWriteDeadline(time.Time{}); err != nil {
		c.logger.Warn("Failed to clear write deadline for error response", "error", err)
	}

	return nil
}
