package dbos

import (
	"testing"
)

func TestConfigValidationErrorTypes(t *testing.T) {
	databaseURL := getDatabaseURL()

	t.Run("CreatesDBOSContext", func(t *testing.T) {
		t.Setenv("DBOS__APPVERSION", "v1.0.0")
		t.Setenv("DBOS__APPID", "test-app-id")
		t.Setenv("DBOS__VMID", "test-executor-id")
		ctx, err := NewDBOSContext(Config{
			DatabaseURL: databaseURL,
			AppName:     "test-initialize",
		})
		if err != nil {
			t.Fatalf("Failed to initialize DBOS: %v", err)
		}
		defer func() {
			if ctx != nil {
				ctx.Shutdown()
			}
		}() // Clean up executor

		if ctx == nil {
			t.Fatal("Initialize returned nil executor")
		}

		// Test that executor implements DBOSContext interface
		var _ DBOSContext = ctx

		// Test that we can call methods on the executor
		appVersion := ctx.GetApplicationVersion()
		if appVersion != "v1.0.0" {
			t.Fatal("GetApplicationVersion returned empty string")
		}
		executorID := ctx.GetExecutorID()
		if executorID != "test-executor-id" {
			t.Fatal("GetExecutorID returned empty string")
		}
		appID := ctx.GetApplicationID()
		if appID != "test-app-id" {
			t.Fatal("GetApplicationID returned empty string")
		}
	})

	t.Run("FailsWithoutAppName", func(t *testing.T) {
		config := Config{
			DatabaseURL: databaseURL,
		}

		_, err := NewDBOSContext(config)
		if err == nil {
			t.Fatal("expected error when app name is missing, but got none")
		}

		dbosErr, ok := err.(*DBOSError)
		if !ok {
			t.Fatalf("expected DBOSError, got %T", err)
		}

		if dbosErr.Code != InitializationError {
			t.Fatalf("expected InitializationError code, got %v", dbosErr.Code)
		}

		expectedMsg := "Error initializing DBOS Transact: missing required config field: appName"
		if dbosErr.Message != expectedMsg {
			t.Fatalf("expected error message '%s', got '%s'", expectedMsg, dbosErr.Message)
		}
	})

	t.Run("FailsWithoutDatabaseURL", func(t *testing.T) {
		config := Config{
			AppName: "test-app",
		}

		_, err := NewDBOSContext(config)
		if err == nil {
			t.Fatal("expected error when database URL is missing, but got none")
		}

		dbosErr, ok := err.(*DBOSError)
		if !ok {
			t.Fatalf("expected DBOSError, got %T", err)
		}

		if dbosErr.Code != InitializationError {
			t.Fatalf("expected InitializationError code, got %v", dbosErr.Code)
		}

		expectedMsg := "Error initializing DBOS Transact: missing required config field: databaseURL"
		if dbosErr.Message != expectedMsg {
			t.Fatalf("expected error message '%s', got '%s'", expectedMsg, dbosErr.Message)
		}
	})
}
