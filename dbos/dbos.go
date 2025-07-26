package dbos

import (
	"context"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"time"

	"github.com/robfig/cron/v3"
)

var (
	_APP_VERSION               string
	_EXECUTOR_ID               string
	_APP_ID                    string
	_DEFAULT_ADMIN_SERVER_PORT = 3001
)

var workflowScheduler *cron.Cron // Global because accessed during workflow registration before the dbos singleton is initialized

var logger *slog.Logger // Global because accessed everywhere inside the library

func getLogger() *slog.Logger {
	if dbos == nil || logger == nil {
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

// processConfig merges configuration from two sources in order of precedence:
// 1. programmatic configuration
// 2. environment variables
// Finally, it applies default values if needed.
func processConfig(inputConfig *Config) (*Config, error) {
	// First check required fields
	if len(inputConfig.DatabaseURL) == 0 {
		return nil, fmt.Errorf("missing required config field: databaseURL")
	}
	if len(inputConfig.AppName) == 0 {
		return nil, fmt.Errorf("missing required config field: appName")
	}

	dbosConfig := &Config{}

	// Start with environment variables (lowest precedence)
	if dbURL := os.Getenv("DBOS_SYSTEM_DATABASE_URL"); dbURL != "" {
		dbosConfig.DatabaseURL = dbURL
	}

	// Override with programmatic configuration (highest precedence)
	if len(inputConfig.DatabaseURL) > 0 {
		dbosConfig.DatabaseURL = inputConfig.DatabaseURL
	}
	if len(inputConfig.AppName) > 0 {
		dbosConfig.AppName = inputConfig.AppName
	}
	// Copy over parameters that can only be set programmatically
	dbosConfig.Logger = inputConfig.Logger
	dbosConfig.AdminServer = inputConfig.AdminServer

	// Load defaults
	if dbosConfig.Logger == nil {
		dbosConfig.Logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	if len(dbosConfig.DatabaseURL) == 0 {
		dbosConfig.Logger.Info("Using default database URL: postgres://postgres:${PGPASSWORD}@localhost:5432/dbos?sslmode=disable")
		password := url.QueryEscape(os.Getenv("PGPASSWORD"))
		dbosConfig.DatabaseURL = fmt.Sprintf("postgres://postgres:%s@localhost:5432/dbos?sslmode=disable", password)
	}

	return dbosConfig, nil
}

var dbos *executor // DBOS singleton instance

type executor struct {
	systemDB              SystemDatabase
	queueRunnerCtx        context.Context
	queueRunnerCancelFunc context.CancelFunc
	queueRunnerDone       chan struct{}
	adminServer           *adminServer
	config                *Config
}

func Initialize(inputConfig Config) error {
	if dbos != nil {
		fmt.Println("warning: DBOS instance already initialized, skipping re-initialization")
		return newInitializationError("DBOS already initialized")
	}

	// Load & process the configuration
	config, err := processConfig(&inputConfig)
	if err != nil {
		return newInitializationError(err.Error())
	}

	// Set global logger
	logger = config.Logger

	// Register types we serialize with gob
	var t time.Time
	gob.Register(t)

	// Initialize global variables with environment variables, providing defaults if not set
	_APP_VERSION = os.Getenv("DBOS__APPVERSION")
	if _APP_VERSION == "" {
		_APP_VERSION = computeApplicationVersion()
		logger.Info("DBOS__APPVERSION not set, using computed hash")
	}

	_EXECUTOR_ID = os.Getenv("DBOS__VMID")
	if _EXECUTOR_ID == "" {
		_EXECUTOR_ID = "local"
		logger.Info("DBOS__VMID not set, using default", "executor_id", _EXECUTOR_ID)
	}

	_APP_ID = os.Getenv("DBOS__APPID")

	// Create the system database
	systemDB, err := NewSystemDatabase(config.DatabaseURL)
	if err != nil {
		return newInitializationError(fmt.Sprintf("failed to create system database: %v", err))
	}
	logger.Info("System database initialized")

	// Set the global dbos instance
	dbos = &executor{
		config:   config,
		systemDB: systemDB,
	}

	return nil
}

func Launch() error {
	if dbos == nil {
		return newInitializationError("DBOS instance not initialized, call Initialize first")
	}
	// Start the system database
	dbos.systemDB.Launch(context.Background())

	// Start the admin server if configured
	if dbos.config.AdminServer {
		adminServer := newAdminServer(_DEFAULT_ADMIN_SERVER_PORT)
		err := adminServer.Start()
		if err != nil {
			logger.Error("Failed to start admin server", "error", err)
			return newInitializationError(fmt.Sprintf("failed to start admin server: %v", err))
		}
		logger.Info("Admin server started", "port", _DEFAULT_ADMIN_SERVER_PORT)
		dbos.adminServer = adminServer
	}

	// Create context with cancel function for queue runner
	ctx, cancel := context.WithCancel(context.Background())
	dbos.queueRunnerCtx = ctx
	dbos.queueRunnerCancelFunc = cancel
	dbos.queueRunnerDone = make(chan struct{})

	// Start the queue runner in a goroutine
	go func() {
		defer close(dbos.queueRunnerDone)
		queueRunner(ctx)
	}()
	logger.Info("Queue runner started")

	// Start the workflow scheduler if it has been initialized
	if workflowScheduler != nil {
		workflowScheduler.Start()
		logger.Info("Workflow scheduler started")
	}

	// Run a round of recovery on the local executor
	recoveryHandles, err := recoverPendingWorkflows(context.Background(), []string{_EXECUTOR_ID}) // XXX maybe use the queue runner context here to allow Shutdown to cancel it?
	if err != nil {
		return newInitializationError(fmt.Sprintf("failed to recover pending workflows during launch: %v", err))
	}
	if len(recoveryHandles) > 0 {
		logger.Info("Recovered pending workflows", "count", len(recoveryHandles))
	}

	logger.Info("DBOS initialized", "app_version", _APP_VERSION, "executor_id", _EXECUTOR_ID)
	return nil
}

func Shutdown() {
	if dbos == nil {
		fmt.Println("DBOS instance is nil, cannot shutdown")
		return
	}

	// XXX is there a way to ensure all workflows goroutine are done before closing?

	// Cancel the context to stop the queue runner
	if dbos.queueRunnerCancelFunc != nil {
		dbos.queueRunnerCancelFunc()
		// Wait for queue runner to finish
		<-dbos.queueRunnerDone
		getLogger().Info("Queue runner stopped")
	}

	if workflowScheduler != nil {
		ctx := workflowScheduler.Stop()
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

	if dbos.systemDB != nil {
		dbos.systemDB.Shutdown()
		dbos.systemDB = nil
	}

	if dbos.adminServer != nil {
		err := dbos.adminServer.Shutdown()
		if err != nil {
			getLogger().Error("Failed to shutdown admin server", "error", err)
		} else {
			getLogger().Info("Admin server shutdown complete")
		}
		dbos.adminServer = nil
	}

	if logger != nil {
		logger = nil
	}
	dbos = nil
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
