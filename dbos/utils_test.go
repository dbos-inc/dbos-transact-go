package dbos

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func getDatabaseURL() string {
	databaseURL := os.Getenv("DBOS_SYSTEM_DATABASE_URL")
	if databaseURL == "" {
		password := os.Getenv("PGPASSWORD")
		if password == "" {
			password = "dbos"
		}
		databaseURL = fmt.Sprintf("postgres://postgres:%s@localhost:5432/dbos?sslmode=disable", url.QueryEscape(password))
	}
	return databaseURL
}

/* Test database setup */
func setupDBOS(t *testing.T) DBOSContext {
	t.Helper()

	databaseURL := getDatabaseURL()

	// Clean up the test database
	parsedURL, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("failed to parse database URL: %v", err)
	}

	dbName := parsedURL.Database
	if dbName == "" {
		t.Skip("DBOS_SYSTEM_DATABASE_URL does not specify a database name, skipping integration test")
	}

	postgresURL := parsedURL.Copy()
	postgresURL.Database = "postgres"
	conn, err := pgx.ConnectConfig(context.Background(), postgresURL)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+dbName+" WITH (FORCE)")
	if err != nil {
		t.Fatalf("failed to drop test database: %v", err)
	}

	dbosContext, err := NewDBOSContext(Config{
		DatabaseURL: databaseURL,
		AppName:     "test-app",
	})
	if err != nil {
		t.Fatalf("failed to create DBOS instance: %v", err)
	}
	if dbosContext == nil {
		t.Fatal("expected DBOS instance but got nil")
	}

	// Register cleanup to run after test completes
	t.Cleanup(func() {
		fmt.Println("Cleaning up DBOS instance...")
		if dbosContext != nil {
			dbosContext.Shutdown()
		}
	})

	return dbosContext
}

/* Event struct provides a simple synchronization primitive that can be used to signal between goroutines. */
type Event struct {
	mu    sync.Mutex
	cond  *sync.Cond
	IsSet bool
}

func NewEvent() *Event {
	e := &Event{}
	e.cond = sync.NewCond(&e.mu)
	return e
}

func (e *Event) Wait() {
	e.mu.Lock()
	defer e.mu.Unlock()
	for !e.IsSet {
		e.cond.Wait()
	}
}

func (e *Event) Set() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.IsSet = true
	e.cond.Broadcast()
}

func (e *Event) Clear() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.IsSet = false
}

/* Helpers */

// stopQueueRunner stops the queue runner for testing purposes
func stopQueueRunner(ctx DBOSContext) {
	if ctx != nil {
		exec := ctx.(*dbosContext)
		if exec.queueRunnerCancelFunc != nil {
			exec.queueRunnerCancelFunc()
			// Wait for queue runner to finish
			<-exec.queueRunnerDone
		}
	}
}

// restartQueueRunner restarts the queue runner for testing purposes
func restartQueueRunner(ctx DBOSContext) {
	if ctx != nil {
		exec := ctx.(*dbosContext)
		// Create new context and cancel function
		// FIXME: cancellation now has to go through the DBOSContext
		ctx, cancel := context.WithCancel(context.Background())
		exec.queueRunnerCtx = ctx
		exec.queueRunnerCancelFunc = cancel
		exec.queueRunnerDone = make(chan struct{})

		// Start the queue runner in a goroutine
		go func() {
			defer close(exec.queueRunnerDone)
			queueRunner(exec)
		}()
	}
}

func equal(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func queueEntriesAreCleanedUp(ctx DBOSContext) bool {
	maxTries := 10
	success := false
	for range maxTries {
		// Begin transaction
		exec := ctx.(*dbosContext)
		tx, err := exec.systemDB.(*systemDatabase).pool.Begin(context.Background())
		if err != nil {
			return false
		}

		query := `SELECT COUNT(*)
				  FROM dbos.workflow_status
				  WHERE queue_name IS NOT NULL
					AND queue_name != $1
					AND status IN ('ENQUEUED', 'PENDING')`

		var count int
		err = tx.QueryRow(context.Background(), query, _DBOS_INTERNAL_QUEUE_NAME).Scan(&count)
		tx.Rollback(context.Background()) // Clean up transaction

		if err != nil {
			return false
		}

		if count == 0 {
			success = true
			break
		}

		time.Sleep(1 * time.Second)
	}

	return success
}
