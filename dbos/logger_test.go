package dbos

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestLogger(t *testing.T) {
	databaseURL := getDatabaseURL()

	t.Run("Default logger", func(t *testing.T) {
		dbosCtx, err := NewDBOSContext(Config{
			DatabaseURL: databaseURL,
			AppName:     "test-app",
		}) // Create executor with default logger
		if err != nil {
			t.Fatalf("Failed to create executor with default logger: %v", err)
		}
		err = dbosCtx.Launch()
		if err != nil {
			t.Fatalf("Failed to launch with default logger: %v", err)
		}
		t.Cleanup(func() {
			if dbosCtx != nil {
				dbosCtx.Shutdown()
			}
		})

		if logger == nil {
			t.Fatal("Logger is nil")
		}

		// Test logger access
		logger.Info("Test message from default logger")

	})

	t.Run("Custom logger", func(t *testing.T) {
		// Test with custom slog logger
		var buf bytes.Buffer
		slogLogger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))

		// Add some context to the slog logger
		slogLogger = slogLogger.With("service", "dbos-test", "environment", "test")

		dbosCtx, err := NewDBOSContext(Config{
			DatabaseURL: databaseURL,
			AppName:     "test-app",
			Logger:      slogLogger,
		})
		if err != nil {
			t.Fatalf("Failed to create executor with custom logger: %v", err)
		}
		err = dbosCtx.Launch()
		if err != nil {
			t.Fatalf("Failed to launch with custom logger: %v", err)
		}
		t.Cleanup(func() {
			if dbosCtx != nil {
				dbosCtx.Shutdown()
			}
		})

		if logger == nil {
			t.Fatal("Logger is nil")
		}

		// Test that we can use the logger and it maintains context
		logger.Info("Test message from custom logger", "test_key", "test_value")

		// Check that our custom logger was used and captured the output
		logOutput := buf.String()
		if !strings.Contains(logOutput, "service=dbos-test") {
			t.Errorf("Expected log output to contain service=dbos-test, got: %s", logOutput)
		}
		if !strings.Contains(logOutput, "environment=test") {
			t.Errorf("Expected log output to contain environment=test, got: %s", logOutput)
		}
		if !strings.Contains(logOutput, "test_key=test_value") {
			t.Errorf("Expected log output to contain test_key=test_value, got: %s", logOutput)
		}
	})
}
