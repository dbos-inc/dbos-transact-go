package dbos

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		require.NoError(t, err)
		defer func() {
			if ctx != nil {
				ctx.Shutdown(1 * time.Minute)
			}
		}() // Clean up executor

		require.NotNil(t, ctx)

		// Test that executor implements DBOSContext interface
		var _ DBOSContext = ctx

		// Test that we can call methods on the executor
		appVersion := ctx.GetApplicationVersion()
		assert.Equal(t, "v1.0.0", appVersion)
		executorID := ctx.GetExecutorID()
		assert.Equal(t, "test-executor-id", executorID)
		appID := ctx.GetApplicationID()
		assert.Equal(t, "test-app-id", appID)
	})

	t.Run("FailsWithoutAppName", func(t *testing.T) {
		config := Config{
			DatabaseURL: databaseURL,
		}

		_, err := NewDBOSContext(config)
		require.Error(t, err)

		dbosErr, ok := err.(*DBOSError)
		require.True(t, ok, "expected DBOSError, got %T", err)

		assert.Equal(t, InitializationError, dbosErr.Code)

		expectedMsg := "Error initializing DBOS Transact: missing required config field: appName"
		assert.Equal(t, expectedMsg, dbosErr.Message)
	})

	t.Run("FailsWithoutDatabaseURL", func(t *testing.T) {
		config := Config{
			AppName: "test-app",
		}

		_, err := NewDBOSContext(config)
		require.Error(t, err)

		dbosErr, ok := err.(*DBOSError)
		require.True(t, ok, "expected DBOSError, got %T", err)

		assert.Equal(t, InitializationError, dbosErr.Code)

		expectedMsg := "Error initializing DBOS Transact: missing required config field: databaseURL"
		assert.Equal(t, expectedMsg, dbosErr.Message)
	})
}
