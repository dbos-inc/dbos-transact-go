// Package dbos provides a Go SDK for building durable applications with DBOS Transact.
//
// DBOS Transact enables developers to write resilient distributed applications using workflows
// and steps backed by PostgreSQL. All application state is automatically persisted, providing
// exactly-once execution guarantees and automatic recovery from failures.
package dbos

import (
	"context"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

const (
	_DEFAULT_ADMIN_SERVER_PORT = 3001
	_DBOS_DOMAIN               = "cloud.dbos.dev"
)

// Config holds configuration parameters for initializing a DBOS context.
// DatabaseURL and AppName are required.
type Config struct {
	DatabaseURL     string       // PostgreSQL connection string (required)
	AppName         string       // Application name for identification (required)
	Logger          *slog.Logger // Custom logger instance (defaults to a new slog logger)
	AdminServer     bool         // Enable Transact admin HTTP server (disabled by default)
	ConductorURL    string       // DBOS conductor service URL (optional)
	ConductorAPIKey string       // DBOS conductor API key (optional)
}

// processConfig enforces mandatory fields and applies defaults.
func processConfig(inputConfig *Config) (*Config, error) {
	// First check required fields
	if len(inputConfig.DatabaseURL) == 0 {
		return nil, fmt.Errorf("missing required config field: databaseURL")
	}
	if len(inputConfig.AppName) == 0 {
		return nil, fmt.Errorf("missing required config field: appName")
	}

	dbosConfig := &Config{
		DatabaseURL:     inputConfig.DatabaseURL,
		AppName:         inputConfig.AppName,
		Logger:          inputConfig.Logger,
		AdminServer:     inputConfig.AdminServer,
		ConductorURL:    inputConfig.ConductorURL,
		ConductorAPIKey: inputConfig.ConductorAPIKey,
	}

	// Load defaults
	if dbosConfig.Logger == nil {
		dbosConfig.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}

	return dbosConfig, nil
}

// DBOSContext represents a DBOS execution context that provides workflow orchestration capabilities.
// It extends the standard Go context.Context and adds methods for running workflows and steps,
// inter-workflow communication, and state management.
//
// The context manages the lifecycle of workflows, provides durability guarantees, and enables
// recovery of interrupted workflows.
type DBOSContext interface {
	context.Context

	// Context Lifecycle
	Launch() error                  // Launch the DBOS runtime including system database, queues, admin server, and workflow recovery
	Shutdown(timeout time.Duration) // Gracefully shutdown all DBOS runtime components with ordered cleanup sequence

	// Workflow operations
	RunAsStep(_ DBOSContext, fn StepFunc, opts ...StepOption) (any, error)                                        // Execute a function as a durable step within a workflow
	RunAsWorkflow(_ DBOSContext, fn WorkflowFunc, input any, opts ...WorkflowOption) (WorkflowHandle[any], error) // Start a new workflow execution
	Send(_ DBOSContext, destinationID string, message any, topic string) error                                    // Send a message to another workflow
	Recv(_ DBOSContext, topic string, timeout time.Duration) (any, error)                                         // Receive a message sent to this workflow
	SetEvent(_ DBOSContext, key string, message any) error                                                        // Set a key-value event for this workflow
	GetEvent(_ DBOSContext, targetWorkflowID string, key string, timeout time.Duration) (any, error)              // Get a key-value event from a target workflow
	Sleep(_ DBOSContext, duration time.Duration) (time.Duration, error)                                           // Durable sleep that survives workflow recovery
	GetWorkflowID() (string, error)                                                                               // Get the current workflow ID (only available within workflows)
	GetStepID() (int, error)                                                                                      // Get the current step ID (only available within workflows)

	// Workflow management
	RetrieveWorkflow(_ DBOSContext, workflowID string) (WorkflowHandle[any], error)                                       // Get a handle to an existing workflow
	Enqueue(_ DBOSContext, queueName, workflowName string, input any, opts ...EnqueueOption) (WorkflowHandle[any], error) // Enqueue a workflow to a named queue
	CancelWorkflow(_ DBOSContext, workflowID string) error                                                                // Cancel a workflow by setting its status to CANCELLED
	ResumeWorkflow(_ DBOSContext, workflowID string) (WorkflowHandle[any], error)                                         // Resume a cancelled workflow
	ForkWorkflow(_ DBOSContext, input ForkWorkflowInput) (WorkflowHandle[any], error)                                     // Fork a workflow from a specific step
	ListWorkflows(_ DBOSContext, opts ...ListWorkflowsOption) ([]WorkflowStatus, error)                                   // List workflows based on filtering criteria

	// Accessors
	GetApplicationVersion() string // Get the application version for this context
	GetExecutorID() string         // Get the executor ID for this context
	GetApplicationID() string      // Get the application ID for this context
}

type dbosContext struct {
	ctx           context.Context
	ctxCancelFunc context.CancelCauseFunc

	launched atomic.Bool

	systemDB    systemDatabase
	adminServer *adminServer
	config      *Config

	// Queue runner
	queueRunner *queueRunner

	// Conductor client
	conductor *Conductor

	// Application metadata
	applicationVersion string
	applicationID      string
	executorID         string

	// Wait group for workflow goroutines
	workflowsWg *sync.WaitGroup

	// Workflow registry - read-mostly sync.Map since registration happens only before launch
	workflowRegistry        *sync.Map // map[string]workflowRegistryEntry
	workflowCustomNametoFQN *sync.Map // Maps fully qualified workflow names to custom names. Usefor when client enqueues a workflow by name because registry is indexed by FQN.

	// Workflow scheduler
	workflowScheduler *cron.Cron

	// logger
	logger *slog.Logger
}

// Implement contex.Context interface methods
func (c *dbosContext) Deadline() (deadline time.Time, ok bool) {
	return c.ctx.Deadline()
}

func (c *dbosContext) Done() <-chan struct{} {
	return c.ctx.Done()
}

func (c *dbosContext) Err() error {
	return c.ctx.Err()
}

func (c *dbosContext) Value(key any) any {
	return c.ctx.Value(key)
}

// WithValue returns a copy of the DBOS context with the given key-value pair.
// This is similar to context.WithValue but maintains DBOS context capabilities.
// No-op if the provided context is not a concrete dbos.dbosContext.
func WithValue(ctx DBOSContext, key, val any) DBOSContext {
	if ctx == nil {
		return nil
	}
	// Will do nothing if the concrete type is not dbosContext
	if dbosCtx, ok := ctx.(*dbosContext); ok {
		return &dbosContext{
			ctx:                     context.WithValue(dbosCtx.ctx, key, val), // Spawn a new child context with the value set
			logger:                  dbosCtx.logger,
			systemDB:                dbosCtx.systemDB,
			workflowsWg:             dbosCtx.workflowsWg,
			workflowRegistry:        dbosCtx.workflowRegistry,
			workflowCustomNametoFQN: dbosCtx.workflowCustomNametoFQN,
			applicationVersion:      dbosCtx.applicationVersion,
			executorID:              dbosCtx.executorID,
			applicationID:           dbosCtx.applicationID,
		}
	}
	return nil
}

// WithoutCancel returns a copy of the DBOS context that is not canceled when the parent context is canceled.
// This is useful for operations that should continue even after a workflow is cancelled.
// No-op if the provided context is not a concrete dbos.dbosContext.
func WithoutCancel(ctx DBOSContext) DBOSContext {
	if ctx == nil {
		return nil
	}
	if dbosCtx, ok := ctx.(*dbosContext); ok {
		return &dbosContext{
			ctx:                     context.WithoutCancel(dbosCtx.ctx),
			logger:                  dbosCtx.logger,
			systemDB:                dbosCtx.systemDB,
			workflowsWg:             dbosCtx.workflowsWg,
			workflowRegistry:        dbosCtx.workflowRegistry,
			workflowCustomNametoFQN: dbosCtx.workflowCustomNametoFQN,
			applicationVersion:      dbosCtx.applicationVersion,
			executorID:              dbosCtx.executorID,
			applicationID:           dbosCtx.applicationID,
		}
	}
	return nil
}

// WithTimeout returns a copy of the DBOS context with a timeout.
// The returned context will be canceled after the specified duration.
// No-op if the provided context is not a concrete dbos.dbosContext.
func WithTimeout(ctx DBOSContext, timeout time.Duration) (DBOSContext, context.CancelFunc) {
	if ctx == nil {
		return nil, func() {}
	}
	if dbosCtx, ok := ctx.(*dbosContext); ok {
		newCtx, cancelFunc := context.WithTimeoutCause(dbosCtx.ctx, timeout, errors.New("DBOS context timeout"))
		return &dbosContext{
			ctx:                     newCtx,
			logger:                  dbosCtx.logger,
			systemDB:                dbosCtx.systemDB,
			workflowsWg:             dbosCtx.workflowsWg,
			workflowRegistry:        dbosCtx.workflowRegistry,
			workflowCustomNametoFQN: dbosCtx.workflowCustomNametoFQN,
			applicationVersion:      dbosCtx.applicationVersion,
			executorID:              dbosCtx.executorID,
			applicationID:           dbosCtx.applicationID,
		}, cancelFunc
	}
	return nil, func() {}
}

func (c *dbosContext) getWorkflowScheduler() *cron.Cron {
	if c.workflowScheduler == nil {
		c.workflowScheduler = cron.New(cron.WithSeconds())
	}
	return c.workflowScheduler
}

func (c *dbosContext) GetApplicationVersion() string {
	return c.applicationVersion
}

func (c *dbosContext) GetExecutorID() string {
	return c.executorID
}

func (c *dbosContext) GetApplicationID() string {
	return c.applicationID
}

// NewDBOSContext creates a new DBOS context with the provided configuration.
// The context must be launched with Launch() before use and should be shut down with Shutdown().
// This function initializes the DBOS system database, sets up the queue sub-system,
// and prepares the workflow registry.
//
// Example:
//
//	config := dbos.Config{
//	    DatabaseURL: "postgres://user:pass@localhost:5432/dbname",
//	    AppName:     "my-app",
//	}
//	ctx, err := dbos.NewDBOSContext(config)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer ctx.Shutdown(30*time.Second)
//
//	if err := ctx.Launch(); err != nil {
//	    log.Fatal(err)
//	}
func NewDBOSContext(inputConfig Config) (DBOSContext, error) {
	ctx, cancelFunc := context.WithCancelCause(context.Background())
	initExecutor := &dbosContext{
		workflowsWg:             &sync.WaitGroup{},
		ctx:                     ctx,
		ctxCancelFunc:           cancelFunc,
		workflowRegistry:        &sync.Map{},
		workflowCustomNametoFQN: &sync.Map{},
	}

	// Load and process the configuration
	config, err := processConfig(&inputConfig)
	if err != nil {
		return nil, newInitializationError(err.Error())
	}
	initExecutor.config = config

	// Set global logger
	initExecutor.logger = config.Logger
	initExecutor.logger.Info("Initializing DBOS context", "app_name", config.AppName)

	// Register types we serialize with gob
	var t time.Time
	gob.Register(t)

	// Initialize global variables with environment variables, providing defaults if not set
	initExecutor.applicationVersion = os.Getenv("DBOS__APPVERSION")
	if initExecutor.applicationVersion == "" {
		initExecutor.applicationVersion = computeApplicationVersion()
	}

	initExecutor.executorID = os.Getenv("DBOS__VMID")
	if initExecutor.executorID == "" {
		initExecutor.executorID = "local"
	}

	initExecutor.applicationID = os.Getenv("DBOS__APPID")

	// Create the system database
	systemDB, err := newSystemDatabase(initExecutor, config.DatabaseURL, initExecutor.logger)
	if err != nil {
		return nil, newInitializationError(fmt.Sprintf("failed to create system database: %v", err))
	}
	initExecutor.systemDB = systemDB
	initExecutor.logger.Info("System database initialized")

	// Initialize the queue runner and register DBOS internal queue
	initExecutor.queueRunner = newQueueRunner(initExecutor.logger)
	NewWorkflowQueue(initExecutor, _DBOS_INTERNAL_QUEUE_NAME)

	// Initialize conductor if API key is provided
	if config.ConductorAPIKey != "" {
		if initExecutor.executorID == "local" {
			initExecutor.executorID = uuid.NewString()
		}

		if config.ConductorURL == "" {
			dbosDomain := os.Getenv("DBOS_DOMAIN")
			if dbosDomain == "" {
				dbosDomain = _DBOS_DOMAIN
			}
			config.ConductorURL = fmt.Sprintf("wss://%s/conductor/v1alpha1", dbosDomain)
		}
		conductorConfig := ConductorConfig{
			url:     config.ConductorURL,
			apiKey:  config.ConductorAPIKey,
			appName: config.AppName,
		}
		conductor, err := NewConductor(conductorConfig, initExecutor)
		if err != nil {
			return nil, newInitializationError(fmt.Sprintf("failed to initialize conductor: %v", err))
		} else {
			initExecutor.conductor = conductor
			initExecutor.logger.Info("Conductor initialized")
		}
	}

	return initExecutor, nil
}

// Launch initializes and starts the DBOS runtime components including the system database,
// admin server (if configured), queue runner, workflow scheduler, and performs recovery
// of any pending workflows on this executor. This method must be called before using the DBOS context
// for workflow execution and should only be called once.
//
// Returns an error if the context is already launched or if any component fails to start.
func (c *dbosContext) Launch() error {
	if c.launched.Load() {
		return newInitializationError("DBOS is already launched")
	}

	// Start the system database
	c.systemDB.launch(c)

	// Start the admin server if configured
	if c.config.AdminServer {
		adminServer := newAdminServer(c, _DEFAULT_ADMIN_SERVER_PORT)
		err := adminServer.Start()
		if err != nil {
			c.logger.Error("Failed to start admin server", "error", err)
			return newInitializationError(fmt.Sprintf("failed to start admin server: %v", err))
		}
		c.logger.Info("Admin server started", "port", _DEFAULT_ADMIN_SERVER_PORT)
		c.adminServer = adminServer
	}

	// Start the queue runner in a goroutine
	go func() {
		c.queueRunner.run(c)
	}()
	c.logger.Info("Queue runner started")

	// Start the workflow scheduler if it has been initialized
	if c.workflowScheduler != nil {
		c.workflowScheduler.Start()
		c.logger.Info("Workflow scheduler started")
	}

	// Start the conductor if it has been initialized
	if c.conductor != nil {
		c.conductor.Launch()
		c.logger.Info("Conductor started")
	}

	// Run a round of recovery on the local executor
	recoveryHandles, err := recoverPendingWorkflows(c, []string{c.executorID})
	if err != nil {
		return newInitializationError(fmt.Sprintf("failed to recover pending workflows during launch: %v", err))
	}
	if len(recoveryHandles) > 0 {
		c.logger.Info("Recovered pending workflows", "count", len(recoveryHandles))
	} else {
		c.logger.Info("No pending workflows to recover")
	}

	c.logger.Info("DBOS initialized", "app_version", c.applicationVersion, "executor_id", c.executorID)
	c.launched.Store(true)
	return nil
}

// Shutdown gracefully shuts down the DBOS runtime by performing a complete, ordered cleanup
// of all system components. The shutdown sequence includes:
//
// 1. Calls Cancel to stop workflows and cancel the context
// 2. Waits for the queue runner to complete processing
// 3. Stops the workflow scheduler and waits for scheduled jobs to finish
// 4. Shuts down the system database connection pool and notification listener
// 5. Shuts down conductor
// 6. Shuts down the admin server
// 7. Marks the context as not launched
//
// Each step respects the provided timeout. If any component doesn't shut down within the timeout,
// a warning is logged and the shutdown continues to the next component.
//
// Shutdown is a permanent operation and should be called when the application is terminating.
func (c *dbosContext) Shutdown(timeout time.Duration) {
	c.logger.Info("Shutting down DBOS context")

	// Cancel the context to signal all resources to stop
	c.ctxCancelFunc(errors.New("DBOS cancellation initiated"))

	// Wait for all workflows to finish
	c.logger.Info("Waiting for all workflows to finish")
	done := make(chan struct{})
	go func() {
		c.workflowsWg.Wait()
		close(done)
	}()
	select {
	case <-done:
		c.logger.Info("All workflows completed")
	case <-time.After(timeout):
		// For now just log a warning: eventually we might want Cancel to return an error.
		c.logger.Warn("Timeout waiting for workflows to complete", "timeout", timeout)
	}

	// Wait for queue runner to finish
	if c.queueRunner != nil && c.launched.Load() {
		c.logger.Info("Waiting for queue runner to complete")
		select {
		case <-c.queueRunner.completionChan:
			c.logger.Info("Queue runner completed")
		case <-time.After(timeout):
			c.logger.Warn("Timeout waiting for queue runner to complete", "timeout", timeout)
		}
	}

	// Stop the workflow scheduler and wait until all scheduled workflows are done
	if c.workflowScheduler != nil && c.launched.Load() {
		c.logger.Info("Stopping workflow scheduler")
		ctx := c.workflowScheduler.Stop()

		select {
		case <-ctx.Done():
			c.logger.Info("All scheduled jobs completed")
			c.workflowScheduler = nil
		case <-time.After(timeout):
			c.logger.Warn("Timeout waiting for jobs to complete. Moving on", "timeout", timeout)
		}
	}

	// Shutdown the conductor
	if c.conductor != nil {
		c.logger.Info("Shutting down conductor")
		c.conductor.Shutdown(timeout)
	}

	// Shutdown the admin server
	if c.adminServer != nil && c.launched.Load() {
		c.logger.Info("Shutting down admin server")
		err := c.adminServer.Shutdown(timeout)
		if err != nil {
			c.logger.Error("Failed to shutdown admin server", "error", err)
		} else {
			c.logger.Info("Admin server shutdown complete")
		}
	}

	// Close the system database
	if c.systemDB != nil {
		c.logger.Info("Shutting down system database")
		c.systemDB.shutdown(c, timeout)
	}

	c.launched.Store(false)
}

// getBinaryHash computes and returns the SHA-256 hash of the current executable.
// This is used for application versioning to ensure workflow compatibility across deployments.
// Returns the hexadecimal representation of the hash or an error if the executable cannot be read.
func getBinaryHash() (string, error) {
	execPath, err := os.Executable()
	if err != nil {
		return "", err
	}

	execPath, err = filepath.EvalSymlinks(execPath)
	if err != nil {
		return "", fmt.Errorf("resolve self path: %w", err)
	}

	fi, err := os.Lstat(execPath)
	if err != nil {
		return "", err
	}
	if !fi.Mode().IsRegular() {
		return "", fmt.Errorf("executable is not a regular file")
	}

	file, err := os.Open(execPath) // #nosec G304 -- opening our own executable, not user-supplied
	if err != nil {
		return "", err
	}
	defer file.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func computeApplicationVersion() string {
	hash, err := getBinaryHash()
	if err != nil {
		fmt.Printf("DBOS: Failed to compute binary hash: %v\n", err)
		return ""
	}
	return hash
}
