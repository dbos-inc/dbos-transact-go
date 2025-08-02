package dbos

import (
	"context"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

var (
	_DEFAULT_ADMIN_SERVER_PORT = 3001
)

var logger *slog.Logger // Global because accessed everywhere inside the library

func getLogger() *slog.Logger {
	if logger == nil {
		return slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	return logger
}

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
	Shutdown()

	// Workflow operations
	RunAsStep(_ DBOSContext, fn StepFunc, input any) (any, error)
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
	ctx context.Context

	launched bool

	systemDB    SystemDatabase
	adminServer *adminServer
	config      *Config

	// Queue runner context and cancel function
	queueRunnerCtx        context.Context
	queueRunnerCancelFunc context.CancelFunc
	queueRunnerDone       chan struct{}

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

// Create a new context
// This is intended for workflow contexts and step contexts
// Hence we only set the relevant fields
func WithValue(ctx DBOSContext, key, val any) DBOSContext {
	if ctx == nil {
		return nil
	}
	// Will do nothing if the concrete type is not dbosContext
	if dbosCtx, ok := ctx.(*dbosContext); ok {
		return &dbosContext{
			ctx:                context.WithValue(dbosCtx.ctx, key, val),
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
	initExecutor := &dbosContext{
		workflowsWg:      &sync.WaitGroup{},
		ctx:              context.Background(),
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
	logger = config.Logger

	// Register types we serialize with gob
	var t time.Time
	gob.Register(t)

	// Initialize global variables with environment variables, providing defaults if not set
	initExecutor.applicationVersion = os.Getenv("DBOS__APPVERSION")
	if initExecutor.applicationVersion == "" {
		initExecutor.applicationVersion = computeApplicationVersion()
		logger.Info("DBOS__APPVERSION not set, using computed hash")
	}

	initExecutor.executorID = os.Getenv("DBOS__VMID")
	if initExecutor.executorID == "" {
		initExecutor.executorID = "local"
		logger.Info("DBOS__VMID not set, using default", "executor_id", initExecutor.executorID)
	}

	initExecutor.applicationID = os.Getenv("DBOS__APPID")

	// Create the system database
	systemDB, err := NewSystemDatabase(config.DatabaseURL)
	if err != nil {
		return nil, newInitializationError(fmt.Sprintf("failed to create system database: %v", err))
	}
	initExecutor.systemDB = systemDB
	logger.Info("System database initialized")

	return initExecutor, nil
}

func (c *dbosContext) Launch() error {
	if c.launched {
		return newInitializationError("DBOS is already launched")
	}

	// Start the system database
	c.systemDB.Launch(context.Background())

	// Start the admin server if configured
	if c.config.AdminServer {
		adminServer := newAdminServer(c, _DEFAULT_ADMIN_SERVER_PORT)
		err := adminServer.Start()
		if err != nil {
			logger.Error("Failed to start admin server", "error", err)
			return newInitializationError(fmt.Sprintf("failed to start admin server: %v", err))
		}
		logger.Info("Admin server started", "port", _DEFAULT_ADMIN_SERVER_PORT)
		c.adminServer = adminServer
	}

	// Create context with cancel function for queue runner
	// FIXME: cancellation now has to go through the DBOSContext
	ctx, cancel := context.WithCancel(c.ctx)
	c.queueRunnerCtx = ctx
	c.queueRunnerCancelFunc = cancel
	c.queueRunnerDone = make(chan struct{})

	// Start the queue runner in a goroutine
	go func() {
		defer close(c.queueRunnerDone)
		queueRunner(c)
	}()
	logger.Info("Queue runner started")

	// Start the workflow scheduler if it has been initialized
	if c.workflowScheduler != nil {
		c.workflowScheduler.Start()
		logger.Info("Workflow scheduler started")
	}

	// Run a round of recovery on the local executor
	recoveryHandles, err := recoverPendingWorkflows(c, []string{c.executorID})
	if err != nil {
		return newInitializationError(fmt.Sprintf("failed to recover pending workflows during launch: %v", err))
	}
	if len(recoveryHandles) > 0 {
		logger.Info("Recovered pending workflows", "count", len(recoveryHandles))
	}

	logger.Info("DBOS initialized", "app_version", c.applicationVersion, "executor_id", c.executorID)
	c.launched = true
	return nil
}

func (c *dbosContext) Shutdown() {
	if !c.launched {
		logger.Warn("DBOS is not launched, nothing to shutdown")
		return
	}

	// Wait for all workflows to finish
	getLogger().Info("Waiting for all workflows to finish")
	c.workflowsWg.Wait()

	// Cancel the context to stop the queue runner
	if c.queueRunnerCancelFunc != nil {
		getLogger().Info("Stopping queue runner")
		c.queueRunnerCancelFunc()
		// Wait for queue runner to finish
		<-c.queueRunnerDone
		getLogger().Info("Queue runner stopped")
	}

	if c.workflowScheduler != nil {
		getLogger().Info("Stopping workflow scheduler")
		ctx := c.workflowScheduler.Stop()
		// Wait for all running jobs to complete with 5-second timeout
		timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		select {
		case <-ctx.Done():
			getLogger().Info("All scheduled jobs completed")
		case <-timeoutCtx.Done():
			getLogger().Warn("Timeout waiting for jobs to complete", "timeout", "5s")
		}
	}

	if c.systemDB != nil {
		getLogger().Info("Shutting down system database")
		c.systemDB.Shutdown()
		c.systemDB = nil
	}

	if c.adminServer != nil {
		getLogger().Info("Shutting down admin server")
		err := c.adminServer.Shutdown()
		if err != nil {
			getLogger().Error("Failed to shutdown admin server", "error", err)
		} else {
			getLogger().Info("Admin server shutdown complete")
		}
		c.adminServer = nil
	}

	if logger != nil {
		logger = nil
	}
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
