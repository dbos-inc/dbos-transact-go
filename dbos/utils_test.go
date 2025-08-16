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
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
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

/* Test database reset */
func resetTestDatabase(t *testing.T, databaseURL string) {
	t.Helper()

	// Clean up the test database
	parsedURL, err := pgx.ParseConfig(databaseURL)
	require.NoError(t, err)

	dbName := parsedURL.Database
	if dbName == "" {
		t.Skip("DBOS_SYSTEM_DATABASE_URL does not specify a database name, skipping integration test")
	}

	postgresURL := parsedURL.Copy()
	postgresURL.Database = "postgres"
	conn, err := pgx.ConnectConfig(context.Background(), postgresURL)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+dbName+" WITH (FORCE)")
	require.NoError(t, err)
}

/* Test database setup */
func setupDBOS(t *testing.T, dropDB bool, checkLeaks bool) DBOSContext {
	t.Helper()

	databaseURL := getDatabaseURL()
	if dropDB {
		resetTestDatabase(t, databaseURL)
	}

	dbosCtx, err := NewDBOSContext(Config{
		DatabaseURL: databaseURL,
		AppName:     "test-app",
	})
	require.NoError(t, err)
	require.NotNil(t, dbosCtx)

	// Register cleanup to run after test completes
	t.Cleanup(func() {
		fmt.Println("Cleaning up DBOS instance...")
		dbosCtx.(*dbosContext).logger.Info("Cleaning up DBOS instance...")
		if dbosCtx != nil {
			dbosCtx.Cancel()
		}
		dbosCtx = nil
		if checkLeaks {
			goleak.VerifyNone(t)
		}
	})

	return dbosCtx
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
		tx, err := exec.systemDB.(*sysDB).pool.Begin(ctx)
		if err != nil {
			return false
		}

		query := `SELECT COUNT(*)
				  FROM dbos.workflow_status
				  WHERE queue_name IS NOT NULL
					AND queue_name != $1
					AND status IN ('ENQUEUED', 'PENDING')`

		var count int
		err = tx.QueryRow(ctx, query, _DBOS_INTERNAL_QUEUE_NAME).Scan(&count)
		tx.Rollback(ctx) // Clean up transaction

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
