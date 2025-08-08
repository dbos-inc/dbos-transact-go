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
	"sync"
	"sync/atomic"
	"time"

	"github.com/robfig/cron/v3"
)

const (
	_DEFAULT_ADMIN_SERVER_PORT = 3001
)

type Config struct {
	DatabaseURL string
	AppName     string
	Logger      *slog.Logger
	AdminServer bool
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
		DatabaseURL: inputConfig.DatabaseURL,
		AppName:     inputConfig.AppName,
		Logger:      inputConfig.Logger,
		AdminServer: inputConfig.AdminServer,
	}

	// Load defaults
	if dbosConfig.Logger == nil {
		dbosConfig.Logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}

	return dbosConfig, nil
}

type DBOSContext interface {
	context.Context

	// Context Lifecycle
	Launch() error
	Cancel()

	// Workflow operations
	RunAsStep(_ DBOSContext, fn func(cxt context.Context) (any, error)) (any, error)
	RunAsWorkflow(_ DBOSContext, fn WorkflowFunc, input any, opts ...WorkflowOption) (WorkflowHandle[any], error)
	Send(_ DBOSContext, input WorkflowSendInputInternal) error
	Recv(_ DBOSContext, input WorkflowRecvInput) (any, error)
	SetEvent(_ DBOSContext, input WorkflowSetEventInput) error
	GetEvent(_ DBOSContext, input WorkflowGetEventInput) (any, error)
	Sleep(duration time.Duration) (time.Duration, error)
	GetWorkflowID() (string, error)

	// Workflow management
	RetrieveWorkflow(_ DBOSContext, workflowID string) (WorkflowHandle[any], error)

	// Accessors
	GetApplicationVersion() string
	GetExecutorID() string
	GetApplicationID() string
}

type dbosContext struct {
	ctx           context.Context
	ctxCancelFunc context.CancelCauseFunc

	launched atomic.Bool

	systemDB    SystemDatabase
	adminServer *adminServer
	config      *Config

	// Queue runner
	queueRunner *queueRunner

	// Application metadata
	applicationVersion string
	applicationID      string
	executorID         string

	// Wait group for workflow goroutines
	workflowsWg *sync.WaitGroup

	// Workflow registry
	workflowRegistry map[string]workflowRegistryEntry
	workflowRegMutex *sync.RWMutex

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

func WithValue(ctx DBOSContext, key, val any) DBOSContext {
	if ctx == nil {
		return nil
	}
	// Will do nothing if the concrete type is not dbosContext
	if dbosCtx, ok := ctx.(*dbosContext); ok {
		return &dbosContext{
			ctx:                context.WithValue(dbosCtx.ctx, key, val), // Spawn a new child context with the value set
			logger:             dbosCtx.logger,
			systemDB:           dbosCtx.systemDB,
			workflowsWg:        dbosCtx.workflowsWg,
			workflowRegistry:   dbosCtx.workflowRegistry,
			workflowRegMutex:   dbosCtx.workflowRegMutex,
			applicationVersion: dbosCtx.applicationVersion,
			executorID:         dbosCtx.executorID,
			applicationID:      dbosCtx.applicationID,
		}
	}
	return nil
}

func WithoutCancel(ctx DBOSContext) DBOSContext {
	if ctx == nil {
		return nil
	}
	if dbosCtx, ok := ctx.(*dbosContext); ok {
		return &dbosContext{
			ctx:                context.WithoutCancel(dbosCtx.ctx),
			logger:             dbosCtx.logger,
			systemDB:           dbosCtx.systemDB,
			workflowsWg:        dbosCtx.workflowsWg,
			workflowRegistry:   dbosCtx.workflowRegistry,
			workflowRegMutex:   dbosCtx.workflowRegMutex,
			applicationVersion: dbosCtx.applicationVersion,
			executorID:         dbosCtx.executorID,
			applicationID:      dbosCtx.applicationID,
		}
	}
	return nil
}

func WithTimeout(ctx DBOSContext, timeout time.Duration) (DBOSContext, context.CancelFunc) {
	if ctx == nil {
		return nil, func() {}
	}
	if dbosCtx, ok := ctx.(*dbosContext); ok {
		newCtx, cancelFunc := context.WithTimeoutCause(dbosCtx.ctx, timeout, errors.New("DBOS context timeout"))
		return &dbosContext{
			ctx:                newCtx,
			logger:             dbosCtx.logger,
			systemDB:           dbosCtx.systemDB,
			workflowsWg:        dbosCtx.workflowsWg,
			workflowRegistry:   dbosCtx.workflowRegistry,
			workflowRegMutex:   dbosCtx.workflowRegMutex,
			applicationVersion: dbosCtx.applicationVersion,
			executorID:         dbosCtx.executorID,
			applicationID:      dbosCtx.applicationID,
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

func NewDBOSContext(inputConfig Config) (DBOSContext, error) {
	ctx, cancelFunc := context.WithCancelCause(context.Background())
	initExecutor := &dbosContext{
		workflowsWg:      &sync.WaitGroup{},
		ctx:              ctx,
		ctxCancelFunc:    cancelFunc,
		workflowRegistry: make(map[string]workflowRegistryEntry),
		workflowRegMutex: &sync.RWMutex{},
	}

	// Load and process the configuration
	config, err := processConfig(&inputConfig)
	if err != nil {
		return nil, newInitializationError(err.Error())
	}
	initExecutor.config = config

	// Set global logger
	initExecutor.logger = config.Logger

	// Register types we serialize with gob
	var t time.Time
	gob.Register(t)

	// Initialize global variables with environment variables, providing defaults if not set
	initExecutor.applicationVersion = os.Getenv("DBOS__APPVERSION")
	if initExecutor.applicationVersion == "" {
		initExecutor.applicationVersion = computeApplicationVersion()
		initExecutor.logger.Info("DBOS__APPVERSION not set, using computed hash")
	}

	initExecutor.executorID = os.Getenv("DBOS__VMID")
	if initExecutor.executorID == "" {
		initExecutor.executorID = "local"
		initExecutor.logger.Info("DBOS__VMID not set, using default", "executor_id", initExecutor.executorID)
	}

	initExecutor.applicationID = os.Getenv("DBOS__APPID")

	initExecutor.logger = initExecutor.logger.With(
		//"app_version", initExecutor.applicationVersion, // This is really verbose...
		"executor_id", initExecutor.executorID,
		//"app_id", initExecutor.applicationID, // This should stay internal
	)

	// Create the system database
	systemDB, err := newSystemDatabase(initExecutor, config.DatabaseURL, initExecutor.logger)
	if err != nil {
		return nil, newInitializationError(fmt.Sprintf("failed to create system database: %v", err))
	}
	initExecutor.systemDB = systemDB
	initExecutor.logger.Info("System database initialized")

	// Initialize the queue runner and register DBOS internal queue
	initExecutor.queueRunner = newQueueRunner()
	NewWorkflowQueue(initExecutor, _DBOS_INTERNAL_QUEUE_NAME)

	return initExecutor, nil
}

func (c *dbosContext) Launch() error {
	if c.launched.Load() {
		return newInitializationError("DBOS is already launched")
	}

	// Start the system database
	c.systemDB.Launch(c)

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

	// Run a round of recovery on the local executor
	recoveryHandles, err := recoverPendingWorkflows(c, []string{c.executorID})
	if err != nil {
		return newInitializationError(fmt.Sprintf("failed to recover pending workflows during launch: %v", err))
	}
	if len(recoveryHandles) > 0 {
		c.logger.Info("Recovered pending workflows", "count", len(recoveryHandles))
	}

	c.logger.Info("DBOS initialized", "app_version", c.applicationVersion, "executor_id", c.executorID)
	c.launched.Store(true)
	return nil
}

// TODO: shutdown should really have a timeout and return an error if it wasn't able to shutdown everything
func (c *dbosContext) Cancel() {
	c.logger.Info("Shutting down DBOS context")

	// Cancel the context to signal all resources to stop
	c.ctxCancelFunc(errors.New("DBOS shutdown initiated"))

	// Wait for all workflows to finish
	c.logger.Info("Waiting for all workflows to finish")
	c.workflowsWg.Wait()
	c.logger.Info("All workflows completed")

	// Close the pool and the notification listener if started
	if c.systemDB != nil {
		c.logger.Info("Shutting down system database")
		c.systemDB.Shutdown(c)
		c.systemDB = nil
	}

	if c.launched.Load() {
		// Wait for queue runner to finish
		<-c.queueRunner.completionChan
		c.logger.Info("Queue runner completed")

		if c.workflowScheduler != nil {
			c.logger.Info("Stopping workflow scheduler")
			ctx := c.workflowScheduler.Stop()
			// Wait for all running jobs to complete with 5-second timeout
			timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			select {
			case <-ctx.Done():
				c.logger.Info("All scheduled jobs completed")
			case <-timeoutCtx.Done():
				c.logger.Warn("Timeout waiting for jobs to complete. Moving on", "timeout", "5s")
			}
		}

		if c.adminServer != nil {
			c.logger.Info("Shutting down admin server")
			err := c.adminServer.Shutdown(c)
			if err != nil {
				c.logger.Error("Failed to shutdown admin server", "error", err)
			} else {
				c.logger.Info("Admin server shutdown complete")
			}
			c.adminServer = nil
		}
	}
	c.launched.Store(false)
}

func GetBinaryHash() (string, error) {
	execPath, err := os.Executable()
	if err != nil {
		return "", err
	}

	file, err := os.Open(execPath)
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
	hash, err := GetBinaryHash()
	if err != nil {
		fmt.Printf("DBOS: Failed to compute binary hash: %v\n", err)
		return ""
	}
	return hash
}
