package dbos

import (
	"testing"
)

func TestConfigValidationErrorTypes(t *testing.T) {
	databaseURL := getDatabaseURL(t)

	t.Run("FailsWithoutAppName", func(t *testing.T) {
		config := Config{
			DatabaseURL: databaseURL,
		}

		err := Initialize(config)
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

		err := Initialize(config)
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
