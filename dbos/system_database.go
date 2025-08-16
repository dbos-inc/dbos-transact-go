package dbos

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

/*******************************/
/******* INTERFACE ********/
/*******************************/

type systemDatabase interface {
	// SysDB management
	launch(ctx context.Context)
	shutdown(ctx context.Context)
	resetSystemDB(ctx context.Context) error

	// Workflows
	insertWorkflowStatus(ctx context.Context, input insertWorkflowStatusDBInput) (*insertWorkflowResult, error)
	listWorkflows(ctx context.Context, input listWorkflowsDBInput) ([]WorkflowStatus, error)
	updateWorkflowOutcome(ctx context.Context, input updateWorkflowOutcomeDBInput) error
	awaitWorkflowResult(ctx context.Context, workflowID string) (any, error)
	cancelWorkflow(ctx context.Context, workflowID string) error
	resumeWorkflow(ctx context.Context, workflowID string) error
	forkWorkflow(ctx context.Context, input forkWorkflowDBInput) error

	// Child workflows
	recordChildWorkflow(ctx context.Context, input recordChildWorkflowDBInput) error
	checkChildWorkflow(ctx context.Context, workflowUUID string, functionID int) (*string, error)
	recordChildGetResult(ctx context.Context, input recordChildGetResultDBInput) error

	// Steps
	recordOperationResult(ctx context.Context, input recordOperationResultDBInput) error
	checkOperationExecution(ctx context.Context, input checkOperationExecutionDBInput) (*recordedResult, error)
	getWorkflowSteps(ctx context.Context, workflowID string) ([]stepInfo, error)

	// Communication (special steps)
	send(ctx context.Context, input WorkflowSendInput) error
	recv(ctx context.Context, input WorkflowRecvInput) (any, error)
	setEvent(ctx context.Context, input WorkflowSetEventInput) error
	getEvent(ctx context.Context, input WorkflowGetEventInput) (any, error)

	// Timers (special steps)
	sleep(ctx context.Context, duration time.Duration) (time.Duration, error)

	// Queues
	dequeueWorkflows(ctx context.Context, input dequeueWorkflowsInput) ([]dequeuedWorkflow, error)
	clearQueueAssignment(ctx context.Context, workflowID string) (bool, error)
}

type sysDB struct {
	pool                           *pgxpool.Pool
	notificationListenerConnection *pgconn.PgConn
	notificationLoopDone           chan struct{}
	notificationsMap               *sync.Map
	logger                         *slog.Logger
	launched                       bool
}

/*******************************/
/******* INITIALIZATION ********/
/*******************************/

// createDatabaseIfNotExists creates the database if it doesn't exist
func createDatabaseIfNotExists(ctx context.Context, databaseURL string, logger *slog.Logger) error {
	// Connect to the postgres database
	parsedURL, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		return newInitializationError(fmt.Sprintf("failed to parse database URL: %v", err))
	}

	dbName := parsedURL.Database
	if dbName == "" {
		return newInitializationError("database name not found in URL")
	}

	serverURL := parsedURL.Copy()
	serverURL.Database = "postgres"
	conn, err := pgx.ConnectConfig(ctx, serverURL)
	if err != nil {
		return newInitializationError(fmt.Sprintf("failed to connect to PostgreSQL server: %v", err))
	}
	defer conn.Close(ctx)

	// Create the system database if it doesn't exist
	var exists bool
	err = conn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)", dbName).Scan(&exists)
	if err != nil {
		return newInitializationError(fmt.Sprintf("failed to check if database exists: %v", err))
	}
	if !exists {
		createSQL := fmt.Sprintf("CREATE DATABASE %s", pgx.Identifier{dbName}.Sanitize())
		_, err = conn.Exec(ctx, createSQL)
		if err != nil {
			return newInitializationError(fmt.Sprintf("failed to create database %s: %v", dbName, err))
		}
		logger.Info("Database created", "name", dbName)
	}

	return nil
}

//go:embed migrations/*.sql
var migrationFiles embed.FS

const (
	_DBOS_MIGRATION_TABLE = "dbos_schema_migrations"

	// PostgreSQL error codes
	_PG_ERROR_UNIQUE_VIOLATION      = "23505"
	_PG_ERROR_FOREIGN_KEY_VIOLATION = "23503"

	// Notification channels
	_DBOS_NOTIFICATIONS_CHANNEL   = "dbos_notifications_channel"
	_DBOS_WORKFLOW_EVENTS_CHANNEL = "dbos_workflow_events_channel"

	// Database retry timeouts
	_DB_CONNECTION_RETRY_DELAY = 500 * time.Millisecond
	_DB_RETRY_INTERVAL         = 1 * time.Second
)

func runMigrations(databaseURL string) error {
	// Change the driver to pgx5
	databaseURL = "pgx5://" + strings.TrimPrefix(databaseURL, "postgres://")

	// Create migration source from embedded files
	d, err := iofs.New(migrationFiles, "migrations")
	if err != nil {
		return newInitializationError(fmt.Sprintf("failed to create migration source: %v", err))
	}

	// Add custom migration table name to avoid conflicts with user migrations
	// Parse the URL to properly determine where to add the query parameter
	parsedURL, err := url.Parse(databaseURL)
	if err != nil {
		return newInitializationError(fmt.Sprintf("failed to parse database URL: %v", err))
	}

	// Check if query parameters already exist
	separator := "?"
	if parsedURL.RawQuery != "" {
		separator = "&"
	}
	databaseURL += separator + "x-migrations-table=" + _DBOS_MIGRATION_TABLE

	// Create migrator
	m, err := migrate.NewWithSourceInstance("iofs", d, databaseURL)
	if err != nil {
		return newInitializationError(fmt.Sprintf("failed to create migrator: %v", err))
	}
	defer m.Close()

	// Run migrations
	// FIXME: tolerate errors when the migration is bcz we run an older version of transact
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return newInitializationError(fmt.Sprintf("failed to run migrations: %v", err))
	}

	return nil
}

// New creates a new SystemDatabase instance and runs migrations
func newSystemDatabase(ctx context.Context, databaseURL string, logger *slog.Logger) (systemDatabase, error) {
	// Create the database if it doesn't exist
	if err := createDatabaseIfNotExists(ctx, databaseURL, logger); err != nil {
		return nil, fmt.Errorf("failed to create database: %v", err)
	}

	// Run migrations first
	if err := runMigrations(databaseURL); err != nil {
		return nil, fmt.Errorf("failed to run migrations: %v", err)
	}

	// Create pgx pool
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %v", err)
	}

	// Test the connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %v", err)
	}

	// Create a map of notification payloads to channels
	notificationsMap := &sync.Map{}

	// Create a connection to listen on notifications
	config, err := pgconn.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database URL: %v", err)
	}
	config.OnNotification = func(c *pgconn.PgConn, n *pgconn.Notification) {
		if n.Channel == _DBOS_NOTIFICATIONS_CHANNEL || n.Channel == _DBOS_WORKFLOW_EVENTS_CHANNEL {
			// Check if an entry exists in the map, indexed by the payload
			// If yes, broadcast on the condition variable so listeners can wake up
			if cond, exists := notificationsMap.Load(n.Payload); exists {
				cond.(*sync.Cond).Broadcast()
			}
		}
	}
	notificationListenerConnection, err := pgconn.ConnectConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect notification listener to database: %v", err)
	}

	return &sysDB{
		pool:                           pool,
		notificationListenerConnection: notificationListenerConnection,
		notificationsMap:               notificationsMap,
		notificationLoopDone:           make(chan struct{}),
		logger:                         logger,
	}, nil
}

func (s *sysDB) launch(ctx context.Context) {
	// Start the notification listener loop
	go s.notificationListenerLoop(ctx)
	s.launched = true
}

func (s *sysDB) shutdown(ctx context.Context) {
	s.logger.Info("DBOS: Closing system database connection pool")
	if s.pool != nil {
		s.pool.Close()
	}

	// Context wasn't cancelled, let's manually close
	if !errors.Is(ctx.Err(), context.Canceled) {
		err := s.notificationListenerConnection.Close(ctx)
		if err != nil {
			s.logger.Error("Failed to close notification listener connection", "error", err)
		}
	}

	if s.launched {
		// Wait for the notification loop to exit
		<-s.notificationLoopDone
	}

	s.notificationsMap.Clear()
	// Allow pgx health checks to complete
	// https://github.com/jackc/pgx/blob/15bca4a4e14e0049777c1245dba4c16300fe4fd0/pgxpool/pool.go#L417
	// These trigger go-leak alerts
	time.Sleep(_DB_CONNECTION_RETRY_DELAY)

	s.launched = false
}

/*******************************/
/******* WORKFLOWS ********/
/*******************************/

type insertWorkflowResult struct {
	attempts         int
	status           WorkflowStatusType
	name             string
	queueName        *string
	timeout          time.Duration
	workflowDeadline time.Time
}

type insertWorkflowStatusDBInput struct {
	status     WorkflowStatus
	maxRetries int
	tx         pgx.Tx
}

func (s *sysDB) insertWorkflowStatus(ctx context.Context, input insertWorkflowStatusDBInput) (*insertWorkflowResult, error) {
	if input.tx == nil {
		return nil, errors.New("transaction is required for InsertWorkflowStatus")
	}

	// Set default values
	attempts := 1
	if input.status.Status == WorkflowStatusEnqueued {
		attempts = 0
	}

	updatedAt := time.Now()
	if !input.status.UpdatedAt.IsZero() {
		updatedAt = input.status.UpdatedAt
	}

	var deadline *int64 = nil
	if !input.status.Deadline.IsZero() {
		millis := input.status.Deadline.UnixMilli()
		deadline = &millis
	}

	var timeoutMs *int64 = nil
	if input.status.Timeout > 0 {
		millis := input.status.Timeout.Round(time.Millisecond).Milliseconds()
		timeoutMs = &millis
	}

	inputString, err := serialize(input.status.Input)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize input: %w", err)
	}

	// Our DB works with NULL values
	var applicationVersion *string
	if len(input.status.ApplicationVersion) > 0 {
		applicationVersion = &input.status.ApplicationVersion
	}

	var deduplicationID *string
	if len(input.status.DeduplicationID) > 0 {
		deduplicationID = &input.status.DeduplicationID
	}

	query := `INSERT INTO dbos.workflow_status (
        workflow_uuid,
        status,
        name,
        queue_name,
        authenticated_user,
        assumed_role,
        authenticated_roles,
        executor_id,
        application_version,
        application_id,
        created_at,
        recovery_attempts,
        updated_at,
        workflow_timeout_ms,
        workflow_deadline_epoch_ms,
        inputs,
        deduplication_id,
        priority
    ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
    ON CONFLICT (workflow_uuid)
        DO UPDATE SET
			recovery_attempts = CASE
                WHEN EXCLUDED.status != $19 THEN workflow_status.recovery_attempts + 1
                ELSE workflow_status.recovery_attempts
            END,
            updated_at = EXCLUDED.updated_at,
            executor_id = CASE
                WHEN EXCLUDED.status = $20 THEN workflow_status.executor_id
                ELSE EXCLUDED.executor_id
            END
        RETURNING recovery_attempts, status, name, queue_name, workflow_timeout_ms, workflow_deadline_epoch_ms`

	var result insertWorkflowResult
	var timeoutMSResult *int64
	var workflowDeadlineEpochMS *int64
	err = input.tx.QueryRow(ctx, query,
		input.status.ID,
		input.status.Status,
		input.status.Name,
		input.status.QueueName,
		input.status.AuthenticatedUser,
		input.status.AssumedRole,
		input.status.AuthenticatedRoles,
		input.status.ExecutorID,
		applicationVersion,
		input.status.ApplicationID,
		input.status.CreatedAt.UnixMilli(),
		attempts,
		updatedAt.UnixMilli(),
		timeoutMs,
		deadline,
		inputString,
		deduplicationID,
		input.status.Priority,
		WorkflowStatusEnqueued,
		WorkflowStatusEnqueued,
	).Scan(
		&result.attempts,
		&result.status,
		&result.name,
		&result.queueName,
		&timeoutMSResult,
		&workflowDeadlineEpochMS,
	)
	if err != nil {
		// Handle unique constraint violation for the deduplication ID (this should be the only case for a 23505)
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == _PG_ERROR_UNIQUE_VIOLATION {
			return nil, newQueueDeduplicatedError(
				input.status.ID,
				input.status.QueueName,
				input.status.DeduplicationID,
			)
		}
		return nil, fmt.Errorf("failed to insert workflow status: %w", err)
	}

	// Convert timeout milliseconds to time.Duration
	if timeoutMSResult != nil && *timeoutMSResult > 0 {
		result.timeout = time.Duration(*timeoutMSResult) * time.Millisecond
	}

	// Convert deadline milliseconds to time.Time
	if workflowDeadlineEpochMS != nil {
		result.workflowDeadline = time.Unix(0, *workflowDeadlineEpochMS*int64(time.Millisecond))
	}

	if len(input.status.Name) > 0 && result.name != input.status.Name {
		return nil, newConflictingWorkflowError(input.status.ID, fmt.Sprintf("Workflow already exists with a different name: %s, but the provided name is: %s", result.name, input.status.Name))
	}
	if len(input.status.QueueName) > 0 && result.queueName != nil && input.status.QueueName != *result.queueName {
		return nil, newConflictingWorkflowError(input.status.ID, fmt.Sprintf("Workflow already exists in a different queue: %s, but the provided queue is: %s", *result.queueName, input.status.QueueName))
	}

	// Every time we start executing a workflow (and thus attempt to insert its status), we increment `recovery_attempts` by 1.
	// When this number becomes equal to `maxRetries + 1`, we mark the workflow as `RETRIES_EXCEEDED`.
	if result.status != WorkflowStatusSuccess && result.status != WorkflowStatusError &&
		input.maxRetries > 0 && result.attempts > input.maxRetries+1 {

		// Update workflow status to RETRIES_EXCEEDED and clear queue-related fields
		dlqQuery := `UPDATE dbos.workflow_status
					 SET status = $1, deduplication_id = NULL, started_at_epoch_ms = NULL, queue_name = NULL
					 WHERE workflow_uuid = $2 AND status = $3`

		_, err = input.tx.Exec(ctx, dlqQuery,
			WorkflowStatusRetriesExceeded,
			input.status.ID,
			WorkflowStatusPending)

		if err != nil {
			return nil, fmt.Errorf("failed to update workflow to RETRIES_EXCEEDED: %w", err)
		}

		// Commit the transaction before throwing the error
		if err := input.tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("failed to commit transaction after marking workflow as RETRIES_EXCEEDED: %w", err)
		}

		return nil, newDeadLetterQueueError(input.status.ID, input.maxRetries)
	}

	return &result, nil
}

// ListWorkflowsInput represents the input parameters for listing workflows
type listWorkflowsDBInput struct {
	workflowName       string
	queueName          string
	workflowIDPrefix   string
	workflowIDs        []string
	authenticatedUser  string
	startTime          time.Time
	endTime            time.Time
	status             []WorkflowStatusType
	applicationVersion string
	executorIDs        []string
	limit              *int
	offset             *int
	sortDesc           bool
	loadInput          bool
	loadOutput         bool
	tx                 pgx.Tx
}

// ListWorkflows retrieves a list of workflows based on the provided filters
func (s *sysDB) listWorkflows(ctx context.Context, input listWorkflowsDBInput) ([]WorkflowStatus, error) {
	qb := newQueryBuilder()

	// Build the base query with conditional column selection
	loadColumns := []string{
		"workflow_uuid", "status", "name", "authenticated_user", "assumed_role", "authenticated_roles",
		"executor_id", "created_at", "updated_at", "application_version", "application_id",
		"recovery_attempts", "queue_name", "workflow_timeout_ms", "workflow_deadline_epoch_ms", "started_at_epoch_ms",
		"deduplication_id", "priority",
	}

	if input.loadOutput {
		loadColumns = append(loadColumns, "output", "error")
	}
	if input.loadInput {
		loadColumns = append(loadColumns, "inputs")
	}

	baseQuery := fmt.Sprintf("SELECT %s FROM dbos.workflow_status", strings.Join(loadColumns, ", "))

	// Add filters using query builder
	if input.workflowName != "" {
		qb.addWhere("name", input.workflowName)
	}
	if input.queueName != "" {
		qb.addWhere("queue_name", input.queueName)
	}
	if input.workflowIDPrefix != "" {
		qb.addWhereLike("workflow_uuid", input.workflowIDPrefix+"%")
	}
	if len(input.workflowIDs) > 0 {
		qb.addWhereAny("workflow_uuid", input.workflowIDs)
	}
	if input.authenticatedUser != "" {
		qb.addWhere("authenticated_user", input.authenticatedUser)
	}
	if !input.startTime.IsZero() {
		qb.addWhereGreaterEqual("created_at", input.startTime.UnixMilli())
	}
	if !input.endTime.IsZero() {
		qb.addWhereLessEqual("created_at", input.endTime.UnixMilli())
	}
	if len(input.status) > 0 {
		qb.addWhereAny("status", input.status)
	}
	if input.applicationVersion != "" {
		qb.addWhere("application_version", input.applicationVersion)
	}
	if len(input.executorIDs) > 0 {
		qb.addWhereAny("executor_id", input.executorIDs)
	}

	// Build complete query
	var query string
	if len(qb.whereClauses) > 0 {
		query = fmt.Sprintf("%s WHERE %s", baseQuery, strings.Join(qb.whereClauses, " AND "))
	} else {
		query = baseQuery
	}

	// Add sorting
	if input.sortDesc {
		query += " ORDER BY created_at DESC"
	} else {
		query += " ORDER BY created_at ASC"
	}

	// Add limit and offset
	if input.limit != nil {
		qb.argCounter++
		query += fmt.Sprintf(" LIMIT $%d", qb.argCounter)
		qb.args = append(qb.args, *input.limit)
	}

	if input.offset != nil {
		qb.argCounter++
		query += fmt.Sprintf(" OFFSET $%d", qb.argCounter)
		qb.args = append(qb.args, *input.offset)
	}

	// Execute the query
	var rows pgx.Rows
	var err error

	if input.tx != nil {
		rows, err = input.tx.Query(ctx, query, qb.args...)
	} else {
		rows, err = s.pool.Query(ctx, query, qb.args...)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to execute ListWorkflows query: %w", err)
	}
	defer rows.Close()

	var workflows []WorkflowStatus
	for rows.Next() {
		var wf WorkflowStatus
		var queueName *string
		var createdAtMs, updatedAtMs int64
		var timeoutMs *int64
		var deadlineMs, startedAtMs *int64
		var outputString, inputString *string
		var errorStr *string
		var deduplicationID *string
		var applicationVersion *string
		var executorID *string

		// Build scan arguments dynamically based on loaded columns
		scanArgs := []interface{}{
			&wf.ID, &wf.Status, &wf.Name, &wf.AuthenticatedUser, &wf.AssumedRole,
			&wf.AuthenticatedRoles, &executorID, &createdAtMs,
			&updatedAtMs, &applicationVersion, &wf.ApplicationID,
			&wf.Attempts, &queueName, &timeoutMs,
			&deadlineMs, &startedAtMs, &deduplicationID, &wf.Priority,
		}

		if input.loadOutput {
			scanArgs = append(scanArgs, &outputString, &errorStr)
		}
		if input.loadInput {
			scanArgs = append(scanArgs, &inputString)
		}

		err := rows.Scan(scanArgs...)
		if err != nil {
			return nil, fmt.Errorf("failed to scan workflow row: %w", err)
		}

		if queueName != nil && len(*queueName) > 0 {
			wf.QueueName = *queueName
		}

		if executorID != nil && len(*executorID) > 0 {
			wf.ExecutorID = *executorID
		}

		if applicationVersion != nil && len(*applicationVersion) > 0 {
			wf.ApplicationVersion = *applicationVersion
		}

		if deduplicationID != nil && len(*deduplicationID) > 0 {
			wf.DeduplicationID = *deduplicationID
		}

		// Convert milliseconds to time.Time
		wf.CreatedAt = time.Unix(0, createdAtMs*int64(time.Millisecond))
		wf.UpdatedAt = time.Unix(0, updatedAtMs*int64(time.Millisecond))

		// Convert timeout milliseconds to time.Duration
		if timeoutMs != nil && *timeoutMs > 0 {
			wf.Timeout = time.Duration(*timeoutMs) * time.Millisecond
		}

		// Convert deadline milliseconds to time.Time
		if deadlineMs != nil {
			wf.Deadline = time.Unix(0, *deadlineMs*int64(time.Millisecond))
		}

		// Convert started at milliseconds to time.Time
		if startedAtMs != nil {
			wf.StartedAt = time.Unix(0, *startedAtMs*int64(time.Millisecond))
		}

		// Handle output and error only if loadOutput is true
		if input.loadOutput {
			// Convert error string to error type if present
			if errorStr != nil && *errorStr != "" {
				wf.Error = errors.New(*errorStr)
			}

			// XXX maybe set wf.Output to outputString and run deserialize out of system DB
			wf.Output, err = deserialize(outputString)
			if err != nil {
				return nil, fmt.Errorf("failed to deserialize output: %w", err)
			}
		}

		// Handle input only if loadInput is true
		if input.loadInput {
			wf.Input, err = deserialize(inputString)
			if err != nil {
				return nil, fmt.Errorf("failed to deserialize input: %w", err)
			}
		}

		workflows = append(workflows, wf)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over workflow rows: %w", err)
	}

	return workflows, nil
}

type updateWorkflowOutcomeDBInput struct {
	workflowID string
	status     WorkflowStatusType
	output     any
	err        error
	tx         pgx.Tx
}

// Will evolve as we serialize all output and error types
func (s *sysDB) updateWorkflowOutcome(ctx context.Context, input updateWorkflowOutcomeDBInput) error {
	query := `UPDATE dbos.workflow_status
			  SET status = $1, output = $2, error = $3, updated_at = $4, deduplication_id = NULL
			  WHERE workflow_uuid = $5`

	outputString, err := serialize(input.output)
	if err != nil {
		return fmt.Errorf("failed to serialize output: %w", err)
	}

	var errorStr string
	if input.err != nil {
		errorStr = input.err.Error()
	}

	if input.tx != nil {
		_, err = input.tx.Exec(ctx, query, input.status, outputString, errorStr, time.Now().UnixMilli(), input.workflowID)
	} else {
		_, err = s.pool.Exec(ctx, query, input.status, outputString, errorStr, time.Now().UnixMilli(), input.workflowID)
	}

	if err != nil {
		return fmt.Errorf("failed to update workflow status: %w", err)
	}
	return nil
}

func (s *sysDB) cancelWorkflow(ctx context.Context, workflowID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Check if workflow exists
	listInput := listWorkflowsDBInput{
		workflowIDs: []string{workflowID},
		loadInput:   true,
		loadOutput:  true,
		tx:          tx,
	}
	wfs, err := s.listWorkflows(ctx, listInput)
	if err != nil {
		return err
	}
	if len(wfs) == 0 {
		return newNonExistentWorkflowError(workflowID)
	}

	wf := wfs[0]
	switch wf.Status {
	case WorkflowStatusSuccess, WorkflowStatusError, WorkflowStatusCancelled:
		// Workflow is already in a terminal state, rollback and return
		if err := tx.Rollback(ctx); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
		return nil
	}

	// Set the workflow's status to CANCELLED and started_at_epoch_ms to NULL (so it does not block the queue, if any)
	updateStatusQuery := `UPDATE dbos.workflow_status
						  SET status = $1, updated_at = $2, started_at_epoch_ms = NULL
						  WHERE workflow_uuid = $3`

	_, err = tx.Exec(ctx, updateStatusQuery, WorkflowStatusCancelled, time.Now().UnixMilli(), workflowID)
	if err != nil {
		return fmt.Errorf("failed to update workflow status to CANCELLED: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (s *sysDB) resumeWorkflow(ctx context.Context, workflowID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Execute with snapshot isolation in case of concurrent calls on the same workflow
	_, err = tx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
	if err != nil {
		return fmt.Errorf("failed to set transaction isolation level: %w", err)
	}

	// Check the status of the workflow. If it is complete, do nothing.
	listInput := listWorkflowsDBInput{
		workflowIDs: []string{workflowID},
		loadInput:   true,
		loadOutput:  true,
		tx:          tx,
	}
	wfs, err := s.listWorkflows(ctx, listInput)
	if err != nil {
		s.logger.Error("ResumeWorkflow: failed to list workflows", "error", err)
		return err
	}
	if len(wfs) == 0 {
		return newNonExistentWorkflowError(workflowID)
	}

	wf := wfs[0]
	if wf.Status == WorkflowStatusSuccess || wf.Status == WorkflowStatusError {
		return nil // Workflow is complete, do nothing
	}

	// Set the workflow's status to ENQUEUED and clear its recovery attempts, set new deadline
	updateStatusQuery := `UPDATE dbos.workflow_status
						  SET status = $1, queue_name = $2, recovery_attempts = $3, 
						      workflow_deadline_epoch_ms = NULL, deduplication_id = NULL,
						      started_at_epoch_ms = NULL, updated_at = $4
						  WHERE workflow_uuid = $5`

	_, err = tx.Exec(ctx, updateStatusQuery,
		WorkflowStatusEnqueued,
		_DBOS_INTERNAL_QUEUE_NAME,
		0,
		time.Now().UnixMilli(),
		workflowID)
	if err != nil {
		return fmt.Errorf("failed to update workflow status to ENQUEUED: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

type forkWorkflowDBInput struct {
	originalWorkflowID string
	forkedWorkflowID   string
	startStep          int
	applicationVersion string
}

func (s *sysDB) forkWorkflow(ctx context.Context, input forkWorkflowDBInput) error {
	// Validate startStep
	if input.startStep < 0 {
		return fmt.Errorf("startStep must be >= 0, got %d", input.startStep)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Get the original workflow status
	listInput := listWorkflowsDBInput{
		workflowIDs: []string{input.originalWorkflowID},
		loadInput:   true,
		tx:          tx,
	}
	wfs, err := s.listWorkflows(ctx, listInput)
	if err != nil {
		return fmt.Errorf("failed to list workflows: %w", err)
	}
	if len(wfs) == 0 {
		return newNonExistentWorkflowError(input.originalWorkflowID)
	}

	originalWorkflow := wfs[0]

	// Determine the application version to use
	appVersion := originalWorkflow.ApplicationVersion
	if input.applicationVersion != "" {
		appVersion = input.applicationVersion
	}

	// Create an entry for the forked workflow with the same initial values as the original
	insertQuery := `INSERT INTO dbos.workflow_status (
		workflow_uuid,
		status,
		name,
		authenticated_user,
		assumed_role,
		authenticated_roles,
		application_version,
		application_id,
		queue_name,
		inputs,
		created_at,
		updated_at,
		recovery_attempts
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`

	inputString, err := serialize(originalWorkflow.Input)
	if err != nil {
		return fmt.Errorf("failed to serialize input: %w", err)
	}

	_, err = tx.Exec(ctx, insertQuery,
		input.forkedWorkflowID,
		WorkflowStatusEnqueued,
		originalWorkflow.Name,
		originalWorkflow.AuthenticatedUser,
		originalWorkflow.AssumedRole,
		originalWorkflow.AuthenticatedRoles,
		&appVersion,
		originalWorkflow.ApplicationID,
		_DBOS_INTERNAL_QUEUE_NAME,
		inputString,
		time.Now().UnixMilli(),
		time.Now().UnixMilli(),
		0)

	if err != nil {
		return fmt.Errorf("failed to insert forked workflow status: %w", err)
	}

	// If startStep > 0, copy the original workflow's outputs into the forked workflow
	if input.startStep > 0 {
		copyOutputsQuery := `INSERT INTO dbos.operation_outputs
			(workflow_uuid, function_id, output, error, function_name, child_workflow_id)
			SELECT $1, function_id, output, error, function_name, child_workflow_id
			FROM dbos.operation_outputs
			WHERE workflow_uuid = $2 AND function_id < $3`

		_, err = tx.Exec(ctx, copyOutputsQuery, input.forkedWorkflowID, input.originalWorkflowID, input.startStep)
		if err != nil {
			return fmt.Errorf("failed to copy operation outputs: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (s *sysDB) awaitWorkflowResult(ctx context.Context, workflowID string) (any, error) {
	query := `SELECT status, output, error FROM dbos.workflow_status WHERE workflow_uuid = $1`
	var status WorkflowStatusType
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		row := s.pool.QueryRow(ctx, query, workflowID)
		var outputString *string
		var errorStr *string
		err := row.Scan(&status, &outputString, &errorStr)
		if err != nil {
			if err == pgx.ErrNoRows {
				time.Sleep(_DB_RETRY_INTERVAL)
				continue
			}
			return nil, fmt.Errorf("failed to query workflow status: %w", err)
		}

		// Deserialize output from TEXT to bytes then from bytes to R using gob
		output, err := deserialize(outputString)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize output: %w", err)
		}

		switch status {
		case WorkflowStatusSuccess, WorkflowStatusError:
			if errorStr == nil || len(*errorStr) == 0 {
				return output, nil
			}
			return output, errors.New(*errorStr)
		case WorkflowStatusCancelled:
			return output, newAwaitedWorkflowCancelledError(workflowID)
		default:
			time.Sleep(_DB_RETRY_INTERVAL)
		}
	}
}

type recordOperationResultDBInput struct {
	workflowID string
	stepID     int
	stepName   string
	output     any
	err        error
	tx         pgx.Tx
}

func (s *sysDB) recordOperationResult(ctx context.Context, input recordOperationResultDBInput) error {
	query := `INSERT INTO dbos.operation_outputs
            (workflow_uuid, function_id, output, error, function_name)
            VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT DO NOTHING`

	var errorString *string
	if input.err != nil {
		e := input.err.Error()
		errorString = &e
	}

	outputString, err := serialize(input.output)
	if err != nil {
		return fmt.Errorf("failed to serialize output: %w", err)
	}

	var commandTag pgconn.CommandTag
	if input.tx != nil {
		commandTag, err = input.tx.Exec(ctx, query,
			input.workflowID,
			input.stepID,
			outputString,
			errorString,
			input.stepName,
		)
	} else {
		commandTag, err = s.pool.Exec(ctx, query,
			input.workflowID,
			input.stepID,
			outputString,
			errorString,
			input.stepName,
		)
	}

	/*
		s.logger.Debug("RecordOperationResult CommandTag", "command_tag", commandTag)
		s.logger.Debug("RecordOperationResult Rows affected", "rows_affected", commandTag.RowsAffected())
		s.logger.Debug("RecordOperationResult SQL", "sql", commandTag.String())
	*/

	if err != nil {
		s.logger.Error("RecordOperationResult Error occurred", "error", err)
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == _PG_ERROR_UNIQUE_VIOLATION {
			return newWorkflowConflictIDError(input.workflowID)
		}
		return err
	}

	if commandTag.RowsAffected() == 0 {
		s.logger.Warn("RecordOperationResult No rows were affected by the insert")
	}

	return nil
}

/*******************************/
/******* CHILD WORKFLOWS ********/
/*******************************/

type recordChildWorkflowDBInput struct {
	parentWorkflowID string
	childWorkflowID  string
	stepID           int
	stepName         string
	tx               pgx.Tx
}

func (s *sysDB) recordChildWorkflow(ctx context.Context, input recordChildWorkflowDBInput) error {
	query := `INSERT INTO dbos.operation_outputs
            (workflow_uuid, function_id, function_name, child_workflow_id)
            VALUES ($1, $2, $3, $4)`

	var commandTag pgconn.CommandTag
	var err error

	if input.tx != nil {
		commandTag, err = input.tx.Exec(ctx, query,
			input.parentWorkflowID,
			input.stepID,
			input.stepName,
			input.childWorkflowID,
		)
	} else {
		commandTag, err = s.pool.Exec(ctx, query,
			input.parentWorkflowID,
			input.stepID,
			input.stepName,
			input.childWorkflowID,
		)
	}

	if err != nil {
		// Check for unique constraint violation (conflict ID error)
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == _PG_ERROR_UNIQUE_VIOLATION {
			return fmt.Errorf(
				"child workflow %s already registered for parent workflow %s (operation ID: %d)",
				input.childWorkflowID, input.parentWorkflowID, input.stepID)
		}
		return fmt.Errorf("failed to record child workflow: %w", err)
	}

	if commandTag.RowsAffected() == 0 {
		s.logger.Warn("RecordChildWorkflow No rows were affected by the insert")
	}

	return nil
}

func (s *sysDB) checkChildWorkflow(ctx context.Context, workflowID string, functionID int) (*string, error) {
	query := `SELECT child_workflow_id
              FROM dbos.operation_outputs
              WHERE workflow_uuid = $1 AND function_id = $2`

	var childWorkflowID *string
	err := s.pool.QueryRow(ctx, query, workflowID, functionID).Scan(&childWorkflowID)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to check child workflow: %w", err)
	}

	return childWorkflowID, nil
}

type recordChildGetResultDBInput struct {
	parentWorkflowID string
	childWorkflowID  string
	stepID           int
	output           string
	err              error
}

func (s *sysDB) recordChildGetResult(ctx context.Context, input recordChildGetResultDBInput) error {
	query := `INSERT INTO dbos.operation_outputs
            (workflow_uuid, function_id, function_name, output, error, child_workflow_id)
            VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT DO NOTHING`

	var errorString *string
	if input.err != nil {
		e := input.err.Error()
		errorString = &e
	}

	_, err := s.pool.Exec(ctx, query,
		input.parentWorkflowID,
		input.stepID,
		"DBOS.getResult",
		input.output,
		errorString,
		input.childWorkflowID,
	)
	if err != nil {
		return fmt.Errorf("failed to record get result: %w", err)
	}
	return nil
}

/*******************************/
/******* STEPS ********/
/*******************************/

type recordedResult struct {
	output any
	err    error
}

type checkOperationExecutionDBInput struct {
	workflowID string
	stepID     int
	stepName   string
	tx         pgx.Tx
}

func (s *sysDB) checkOperationExecution(ctx context.Context, input checkOperationExecutionDBInput) (*recordedResult, error) {
	var tx pgx.Tx
	var err error

	// Use provided transaction or create a new one
	if input.tx != nil {
		tx = input.tx
	} else {
		tx, err = s.pool.Begin(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to begin transaction: %w", err)
		}
		defer tx.Rollback(ctx) // We don't need to commit this transaction -- it is just useful for having READ COMMITTED across the reads
	}

	// First query: Retrieve the workflow status
	workflowStatusQuery := `SELECT status FROM dbos.workflow_status WHERE workflow_uuid = $1`

	// Second query: Retrieve operation outputs if they exist
	stepOutputQuery := `SELECT output, error, function_name
							 FROM dbos.operation_outputs
							 WHERE workflow_uuid = $1 AND function_id = $2`

	var workflowStatus WorkflowStatusType

	// Execute first query to get workflow status
	err = tx.QueryRow(ctx, workflowStatusQuery, input.workflowID).Scan(&workflowStatus)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, newNonExistentWorkflowError(input.workflowID)
		}
		return nil, fmt.Errorf("failed to get workflow status: %w", err)
	}

	// If the workflow is cancelled, raise the exception
	if workflowStatus == WorkflowStatusCancelled {
		return nil, newWorkflowCancelledError(input.workflowID)
	}

	// Execute second query to get operation outputs
	var outputString *string
	var errorStr *string
	var recordedFunctionName string

	err = tx.QueryRow(ctx, stepOutputQuery, input.workflowID, input.stepID).Scan(&outputString, &errorStr, &recordedFunctionName)

	// If there are no operation outputs, return nil
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get operation outputs: %w", err)
	}

	// If the provided and recorded function name are different, throw an exception
	if input.stepName != recordedFunctionName {
		return nil, newUnexpectedStepError(input.workflowID, input.stepID, input.stepName, recordedFunctionName)
	}

	output, err := deserialize(outputString)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize output: %w", err)
	}

	var recordedError error
	if errorStr != nil && *errorStr != "" {
		recordedError = errors.New(*errorStr)
	}
	result := &recordedResult{
		output: output,
		err:    recordedError,
	}
	return result, nil
}

type stepInfo struct {
	StepID          int
	StepName        string
	Output          any
	Error           error
	ChildWorkflowID string
}

func (s *sysDB) getWorkflowSteps(ctx context.Context, workflowID string) ([]stepInfo, error) {
	query := `SELECT function_id, function_name, output, error, child_workflow_id
			  FROM dbos.operation_outputs
			  WHERE workflow_uuid = $1
			  ORDER BY function_id ASC`

	rows, err := s.pool.Query(ctx, query, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to query workflow steps: %w", err)
	}
	defer rows.Close()

	var steps []stepInfo
	for rows.Next() {
		var step stepInfo
		var outputString *string
		var errorString *string
		var childWorkflowID *string

		err := rows.Scan(&step.StepID, &step.StepName, &outputString, &errorString, &childWorkflowID)
		if err != nil {
			return nil, fmt.Errorf("failed to scan step row: %w", err)
		}

		// Deserialize output if present
		if outputString != nil {
			output, err := deserialize(outputString)
			if err != nil {
				return nil, fmt.Errorf("failed to deserialize output: %w", err)
			}
			step.Output = output
		}

		// Convert error string to error if present
		if errorString != nil && *errorString != "" {
			step.Error = errors.New(*errorString)
		}

		// Set child workflow ID if present
		if childWorkflowID != nil {
			step.ChildWorkflowID = *childWorkflowID
		}

		steps = append(steps, step)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over step rows: %w", err)
	}

	return steps, nil
}

// Sleep is a special type of step that sleeps for a specified duration
// A wakeup time is computed and recorded in the database
// If we sleep is re-executed, it will only sleep for the remaining duration until the wakeup time
func (s *sysDB) sleep(ctx context.Context, duration time.Duration) (time.Duration, error) {
	functionName := "DBOS.sleep"

	// Get workflow state from context
	wfState, ok := ctx.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return 0, newStepExecutionError("", functionName, "workflow state not found in context: are you running this step within a workflow?")
	}

	if wfState.isWithinStep {
		return 0, newStepExecutionError(wfState.workflowID, functionName, "cannot call Sleep within a step")
	}

	stepID := wfState.NextStepID()

	// Check if operation was already executed
	checkInput := checkOperationExecutionDBInput{
		workflowID: wfState.workflowID,
		stepID:     stepID,
		stepName:   functionName,
	}
	recordedResult, err := s.checkOperationExecution(ctx, checkInput)
	if err != nil {
		return 0, fmt.Errorf("failed to check operation execution: %w", err)
	}

	var endTime time.Time

	if recordedResult != nil {
		if recordedResult.output == nil { // This should never happen
			return 0, fmt.Errorf("no recorded end time for recorded sleep operation")
		}

		// The output should be a time.Time representing the end time
		endTimeInterface, ok := recordedResult.output.(time.Time)
		if !ok {
			return 0, fmt.Errorf("recorded output is not a time.Time: %T", recordedResult.output)
		}
		endTime = endTimeInterface

		if recordedResult.err != nil { // This should never happen
			return 0, recordedResult.err
		}
	} else {
		// First execution: calculate and record the end time
		s.logger.Debug("Durable sleep", "stepID", stepID, "duration", duration)

		endTime = time.Now().Add(duration)

		// Record the operation result with the calculated end time
		recordInput := recordOperationResultDBInput{
			workflowID: wfState.workflowID,
			stepID:     stepID,
			stepName:   functionName,
			output:     endTime,
			err:        nil,
		}

		err = s.recordOperationResult(ctx, recordInput)
		if err != nil {
			// Check if this is a ConflictingWorkflowError (operation already recorded by another process)
			if dbosErr, ok := err.(*DBOSError); ok && dbosErr.Code == ConflictingIDError {
			} else {
				return 0, fmt.Errorf("failed to record sleep operation result: %w", err)
			}
		}
	}

	// Calculate remaining duration until wake up time
	remainingDuration := max(0, time.Until(endTime))

	// Actually sleep for the remaining duration
	time.Sleep(remainingDuration)

	return remainingDuration, nil
}

/****************************************/
/******* WORKFLOW COMMUNICATIONS ********/
/****************************************/

func (s *sysDB) notificationListenerLoop(ctx context.Context) {
	defer func() {
		s.notificationLoopDone <- struct{}{}
	}()

	s.logger.Info("DBOS: Starting notification listener loop")
	mrr := s.notificationListenerConnection.Exec(ctx, fmt.Sprintf("LISTEN %s; LISTEN %s", _DBOS_NOTIFICATIONS_CHANNEL, _DBOS_WORKFLOW_EVENTS_CHANNEL))
	results, err := mrr.ReadAll()
	if err != nil {
		s.logger.Error("Failed to listen on notification channels", "error", err)
		return
	}
	err = mrr.Close()
	if err != nil {
		s.logger.Error("Failed to close connection after setting notification listeners", "error", err)
		return
	}

	for _, result := range results {
		if result.Err != nil {
			s.logger.Error("Error listening on notification channels", "error", result.Err)
			return
		}
	}

	for {
		// Block until a notification is received. OnNotification will be called when a notification is received.
		// WaitForNotification handles context cancellation: https://github.com/jackc/pgx/blob/15bca4a4e14e0049777c1245dba4c16300fe4fd0/pgconn/pgconn.go#L1050
		err := s.notificationListenerConnection.WaitForNotification(ctx)
		if err != nil {
			// Context cancellation
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				s.logger.Info("Notification listener loop exiting due to context cancellation", "cause", context.Cause(ctx), "error", err)
				return
			}

			// Connection closed (during shutdown) - exit gracefully
			if s.notificationListenerConnection.IsClosed() {
				s.logger.Info("Notification listener loop exiting due to connection closure")
				return
			}

			// Other errors - log and retry.
			// TODO add exponential backoff + jitter
			s.logger.Error("Error waiting for notification", "error", err)
			time.Sleep(_DB_CONNECTION_RETRY_DELAY)
			continue
		}
	}
}

const _DBOS_NULL_TOPIC = "__null__topic__"

type WorkflowSendInput struct {
	DestinationID string
	Message       any
	Topic         string
}

// Send is a special type of step that sends a message to another workflow.
// Can be called both within a workflow (as a step) or outside a workflow (directly).
// When called within a workflow: durability and the function run in the same transaction, and we forbid nested step execution
func (s *sysDB) send(ctx context.Context, input WorkflowSendInput) error {
	functionName := "DBOS.send"

	// Get workflow state from context (optional for Send as we can send from outside a workflow)
	wfState, ok := ctx.Value(workflowStateKey).(*workflowState)
	var stepID int
	var isInWorkflow bool

	if ok && wfState != nil {
		isInWorkflow = true
		if wfState.isWithinStep {
			return newStepExecutionError(wfState.workflowID, functionName, "cannot call Send within a step")
		}
		stepID = wfState.NextStepID()
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Check if operation was already executed and do nothing if so (only if in workflow)
	if isInWorkflow {
		checkInput := checkOperationExecutionDBInput{
			workflowID: wfState.workflowID,
			stepID:     stepID,
			stepName:   functionName,
			tx:         tx,
		}
		recordedResult, err := s.checkOperationExecution(ctx, checkInput)
		if err != nil {
			return err
		}
		if recordedResult != nil {
			// when hitting this case, recordedResult will be &{<nil> <nil>}
			return nil
		}
	}

	// Set default topic if not provided
	topic := _DBOS_NULL_TOPIC
	if len(input.Topic) > 0 {
		topic = input.Topic
	}

	// Serialize the message. It must have been registered with encoding/gob by the user if not a basic type.
	messageString, err := serialize(input.Message)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %w", err)
	}

	insertQuery := `INSERT INTO dbos.notifications (destination_uuid, topic, message) VALUES ($1, $2, $3)`
	_, err = tx.Exec(ctx, insertQuery, input.DestinationID, topic, messageString)
	if err != nil {
		// Check for foreign key violation (destination workflow doesn't exist)
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == _PG_ERROR_FOREIGN_KEY_VIOLATION {
			return newNonExistentWorkflowError(input.DestinationID)
		}
		return fmt.Errorf("failed to insert notification: %w", err)
	}

	// Record the operation result if this is called within a workflow
	if isInWorkflow {
		recordInput := recordOperationResultDBInput{
			workflowID: wfState.workflowID,
			stepID:     stepID,
			stepName:   functionName,
			output:     nil,
			err:        nil,
			tx:         tx,
		}

		err = s.recordOperationResult(ctx, recordInput)
		if err != nil {
			return fmt.Errorf("failed to record operation result: %w", err)
		}
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Recv is a special type of step that receives a message destined for a given workflow
func (s *sysDB) recv(ctx context.Context, input WorkflowRecvInput) (any, error) {
	functionName := "DBOS.recv"

	// Get workflow state from context
	wfState, ok := ctx.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return nil, newStepExecutionError("", functionName, "workflow state not found in context: are you running this step within a workflow?")
	}

	if wfState.isWithinStep {
		return nil, newStepExecutionError(wfState.workflowID, functionName, "cannot call Recv within a step")
	}

	stepID := wfState.NextStepID()
	destinationID := wfState.workflowID

	// Set default topic if not provided
	topic := _DBOS_NULL_TOPIC
	if len(input.Topic) > 0 {
		topic = input.Topic
	}

	// Check if operation was already executed
	checkInput := checkOperationExecutionDBInput{
		workflowID: destinationID,
		stepID:     stepID,
		stepName:   functionName,
	}
	recordedResult, err := s.checkOperationExecution(ctx, checkInput)
	if err != nil {
		return nil, fmt.Errorf("failed to check operation execution: %w", err)
	}
	if recordedResult != nil {
		if recordedResult.output != nil {
			return recordedResult.output, nil
		}
		return nil, fmt.Errorf("no output recorded in the last recv")
	}

	// First check if there's already a receiver for this workflow/topic to avoid unnecessary database load
	payload := fmt.Sprintf("%s::%s", destinationID, topic)
	cond := sync.NewCond(&sync.Mutex{})
	_, loaded := s.notificationsMap.LoadOrStore(payload, cond)
	if loaded {
		s.logger.Error("Receive already called for workflow", "destination_id", destinationID)
		return nil, newWorkflowConflictIDError(destinationID)
	}
	defer func() {
		// Clean up the condition variable after we're done and broadcast to wake up any waiting goroutines
		cond.Broadcast()
		s.notificationsMap.Delete(payload)
	}()

	// Now check if there is already a message available in the database.
	// If not, we'll wait for a notification and timeout
	var exists bool
	query := `SELECT EXISTS (SELECT 1 FROM dbos.notifications WHERE destination_uuid = $1 AND topic = $2)`
	err = s.pool.QueryRow(ctx, query, destinationID, topic).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check message: %w", err)
	}
	if !exists {
		// Wait for notifications using condition variable with timeout pattern
		s.logger.Debug("Waiting for notification on condition variable", "payload", payload)

		done := make(chan struct{})
		go func() {
			cond.L.Lock()
			defer cond.L.Unlock()
			cond.Wait()
			close(done)
		}()

		select {
		case <-done:
			s.logger.Debug("Received notification on condition variable", "payload", payload)
		case <-time.After(input.Timeout):
			s.logger.Warn("Recv() timeout reached", "payload", payload, "timeout", input.Timeout)
		}
	}

	// Find the oldest message and delete it atomically
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	query = `
        WITH oldest_entry AS (
            SELECT destination_uuid, topic, message, created_at_epoch_ms
            FROM dbos.notifications
            WHERE destination_uuid = $1 AND topic = $2
            ORDER BY created_at_epoch_ms ASC
            LIMIT 1
        )
        DELETE FROM dbos.notifications
        WHERE destination_uuid = (SELECT destination_uuid FROM oldest_entry)
          AND topic = (SELECT topic FROM oldest_entry)
          AND created_at_epoch_ms = (SELECT created_at_epoch_ms FROM oldest_entry)
        RETURNING message`

	var messageString *string
	err = tx.QueryRow(ctx, query, destinationID, topic).Scan(&messageString)
	if err != nil {
		if err == pgx.ErrNoRows {
			// No message found, record nil result
			messageString = nil
		} else {
			return nil, fmt.Errorf("failed to consume message: %w", err)
		}
	}

	// Deserialize the message
	var message any
	if messageString != nil { // nil message should never happen because they'd cause an error on the send() path
		message, err = deserialize(messageString)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize message: %w", err)
		}
	}

	// Record the operation result
	recordInput := recordOperationResultDBInput{
		workflowID: destinationID,
		stepID:     stepID,
		stepName:   functionName,
		output:     message,
		tx:         tx,
	}
	err = s.recordOperationResult(ctx, recordInput)
	if err != nil {
		return nil, fmt.Errorf("failed to record operation result: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return message, nil
}

type WorkflowSetEventInput struct {
	Key     string
	Message any
}

func (s *sysDB) setEvent(ctx context.Context, input WorkflowSetEventInput) error {
	functionName := "DBOS.setEvent"

	// Get workflow state from context
	wfState, ok := ctx.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return newStepExecutionError("", functionName, "workflow state not found in context: are you running this step within a workflow?")
	}

	if wfState.isWithinStep {
		return newStepExecutionError(wfState.workflowID, functionName, "cannot call SetEvent within a step")
	}

	stepID := wfState.NextStepID()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Check if operation was already executed and do nothing if so
	checkInput := checkOperationExecutionDBInput{
		workflowID: wfState.workflowID,
		stepID:     stepID,
		stepName:   functionName,
		tx:         tx,
	}
	recordedResult, err := s.checkOperationExecution(ctx, checkInput)
	if err != nil {
		return err
	}
	if recordedResult != nil {
		// when hitting this case, recordedResult will be &{<nil> <nil>}
		return nil
	}

	// Serialize the message. It must have been registered with encoding/gob by the user if not a basic type.
	messageString, err := serialize(input.Message)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %w", err)
	}

	// Insert or update the event using UPSERT
	insertQuery := `INSERT INTO dbos.workflow_events (workflow_uuid, key, value)
					VALUES ($1, $2, $3)
					ON CONFLICT (workflow_uuid, key)
					DO UPDATE SET value = EXCLUDED.value`

	_, err = tx.Exec(ctx, insertQuery, wfState.workflowID, input.Key, messageString)
	if err != nil {
		return fmt.Errorf("failed to insert/update workflow event: %w", err)
	}

	// Record the operation result
	recordInput := recordOperationResultDBInput{
		workflowID: wfState.workflowID,
		stepID:     stepID,
		stepName:   functionName,
		output:     nil,
		err:        nil,
		tx:         tx,
	}

	err = s.recordOperationResult(ctx, recordInput)
	if err != nil {
		return fmt.Errorf("failed to record operation result: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (s *sysDB) getEvent(ctx context.Context, input WorkflowGetEventInput) (any, error) {
	functionName := "DBOS.getEvent"

	// Get workflow state from context (optional for GetEvent as we can get an event from outside a workflow)
	wfState, ok := ctx.Value(workflowStateKey).(*workflowState)
	var stepID int
	var isInWorkflow bool

	if ok && wfState != nil {
		isInWorkflow = true
		if wfState.isWithinStep {
			return nil, newStepExecutionError(wfState.workflowID, functionName, "cannot call GetEvent within a step")
		}
		stepID = wfState.NextStepID()

		// Check if operation was already executed (only if in workflow)
		checkInput := checkOperationExecutionDBInput{
			workflowID: wfState.workflowID,
			stepID:     stepID,
			stepName:   functionName,
		}
		recordedResult, err := s.checkOperationExecution(ctx, checkInput)
		if err != nil {
			return nil, err
		}
		if recordedResult != nil {
			return recordedResult.output, recordedResult.err
		}
	}

	// Create notification payload and condition variable
	payload := fmt.Sprintf("%s::%s", input.TargetWorkflowID, input.Key)
	cond := sync.NewCond(&sync.Mutex{})
	existingCond, loaded := s.notificationsMap.LoadOrStore(payload, cond)
	if loaded {
		// Reuse the existing condition variable
		cond = existingCond.(*sync.Cond)
	}

	// Defer broadcast to ensure any waiting goroutines eventually unlock
	defer func() {
		cond.Broadcast()
		// Clean up the condition variable after we're done, if we created it
		if !loaded {
			s.notificationsMap.Delete(payload)
		}
	}()

	// Check if the event already exists in the database
	query := `SELECT value FROM dbos.workflow_events WHERE workflow_uuid = $1 AND key = $2`
	var value any
	var valueString *string

	row := s.pool.QueryRow(ctx, query, input.TargetWorkflowID, input.Key)
	err := row.Scan(&valueString)
	if err != nil && err != pgx.ErrNoRows {
		return nil, fmt.Errorf("failed to query workflow event: %w", err)
	}

	if err == pgx.ErrNoRows || valueString == nil { // valueString should never be `nil`
		// Wait for notification with timeout using condition variable
		done := make(chan struct{})
		go func() {
			cond.L.Lock()
			defer cond.L.Unlock()
			cond.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Received notification
		case <-time.After(input.Timeout):
			// Timeout reached
			s.logger.Warn("GetEvent() timeout reached", "target_workflow_id", input.TargetWorkflowID, "key", input.Key, "timeout", input.Timeout)
		case <-ctx.Done():
			return nil, fmt.Errorf("context cancelled while waiting for event: %w", ctx.Err())
		}

		// Query the database again after waiting
		row = s.pool.QueryRow(ctx, query, input.TargetWorkflowID, input.Key)
		err = row.Scan(&valueString)
		if err != nil {
			if err == pgx.ErrNoRows {
				value = nil // Event still doesn't exist
			} else {
				return nil, fmt.Errorf("failed to query workflow event after wait: %w", err)
			}
		}
	}

	// Deserialize the value if it exists
	if valueString != nil {
		value, err = deserialize(valueString)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize event value: %w", err)
		}
	}

	// Record the operation result if this is called within a workflow
	if isInWorkflow {
		recordInput := recordOperationResultDBInput{
			workflowID: wfState.workflowID,
			stepID:     stepID,
			stepName:   functionName,
			output:     value,
			err:        nil,
		}

		err = s.recordOperationResult(ctx, recordInput)
		if err != nil {
			return nil, fmt.Errorf("failed to record operation result: %w", err)
		}
	}

	return value, nil
}

/*******************************/
/******* QUEUES ********/
/*******************************/

type dequeuedWorkflow struct {
	id    string
	name  string
	input string
}

type dequeueWorkflowsInput struct {
	queue              WorkflowQueue
	executorID         string
	applicationVersion string
}

func (s *sysDB) dequeueWorkflows(ctx context.Context, input dequeueWorkflowsInput) ([]dequeuedWorkflow, error) {
	// Begin transaction with snapshot isolation
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Set transaction isolation level to repeatable read (similar to snapshot isolation)
	_, err = tx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
	if err != nil {
		return nil, fmt.Errorf("failed to set transaction isolation level: %w", err)
	}

	// First check the rate limiter
	startTimeMs := time.Now().UnixMilli()
	var numRecentQueries int
	if input.queue.RateLimit != nil {
		limiterPeriod := time.Duration(input.queue.RateLimit.Period * float64(time.Second))

		// Calculate the cutoff time: current time minus limiter period
		cutoffTimeMs := time.Now().Add(-limiterPeriod).UnixMilli()

		// Count workflows that have started in the limiter period
		limiterQuery := `
		SELECT COUNT(*)
		FROM dbos.workflow_status
		WHERE queue_name = $1
		  AND status != $2
		  AND started_at_epoch_ms > $3`

		err := tx.QueryRow(ctx, limiterQuery,
			input.queue.Name,
			WorkflowStatusEnqueued,
			cutoffTimeMs).Scan(&numRecentQueries)
		if err != nil {
			return nil, fmt.Errorf("failed to query rate limiter: %w", err)
		}

		if numRecentQueries >= input.queue.RateLimit.Limit {
			return []dequeuedWorkflow{}, nil
		}
	}

	// Calculate max_tasks based on concurrency limits
	maxTasks := input.queue.MaxTasksPerIteration

	if input.queue.WorkerConcurrency != nil || input.queue.GlobalConcurrency != nil {
		// Count pending workflows by executor
		pendingQuery := `
			SELECT executor_id, COUNT(*) as task_count
			FROM dbos.workflow_status
			WHERE queue_name = $1 AND status = $2
			GROUP BY executor_id`

		rows, err := tx.Query(ctx, pendingQuery, input.queue.Name, WorkflowStatusPending)
		if err != nil {
			return nil, fmt.Errorf("failed to query pending workflows: %w", err)
		}
		defer rows.Close()

		pendingWorkflowsDict := make(map[string]int)
		for rows.Next() {
			var executorIDRow string
			var taskCount int
			if err := rows.Scan(&executorIDRow, &taskCount); err != nil {
				return nil, fmt.Errorf("failed to scan pending workflow row: %w", err)
			}
			pendingWorkflowsDict[executorIDRow] = taskCount
		}

		localPendingWorkflows := pendingWorkflowsDict[input.executorID]

		// Check worker concurrency limit
		if input.queue.WorkerConcurrency != nil {
			workerConcurrency := *input.queue.WorkerConcurrency
			if localPendingWorkflows > workerConcurrency {
				s.logger.Warn("Local pending workflows on queue exceeds worker concurrency limit", "local_pending", localPendingWorkflows, "queue_name", input.queue.Name, "concurrency_limit", workerConcurrency)
			}
			availableWorkerTasks := max(workerConcurrency-localPendingWorkflows, 0)
			maxTasks = availableWorkerTasks
		}

		// Check global concurrency limit
		if input.queue.GlobalConcurrency != nil {
			globalPendingWorkflows := 0
			for _, count := range pendingWorkflowsDict {
				globalPendingWorkflows += count
			}

			concurrency := *input.queue.GlobalConcurrency
			if globalPendingWorkflows > concurrency {
				s.logger.Warn("Total pending workflows on queue exceeds global concurrency limit", "total_pending", globalPendingWorkflows, "queue_name", input.queue.Name, "concurrency_limit", concurrency)
			}
			availableTasks := max(concurrency-globalPendingWorkflows, 0)
			if availableTasks < maxTasks {
				maxTasks = availableTasks
			}
		}
	}

	// Build the query to select workflows for dequeueing
	// Use SKIP LOCKED when no global concurrency is set to avoid blocking,
	// otherwise use NOWAIT to ensure consistent view across processes
	skipLocks := input.queue.GlobalConcurrency == nil
	var lockClause string
	if skipLocks {
		lockClause = "FOR UPDATE SKIP LOCKED"
	} else {
		lockClause = "FOR UPDATE NOWAIT"
	}

	var query string
	if input.queue.PriorityEnabled {
		query = fmt.Sprintf(`
			SELECT workflow_uuid
			FROM dbos.workflow_status
			WHERE queue_name = $1
			  AND status = $2
			  AND (application_version = $3 OR application_version IS NULL)
			ORDER BY priority ASC, created_at ASC
			%s`, lockClause)
	} else {
		query = fmt.Sprintf(`
			SELECT workflow_uuid
			FROM dbos.workflow_status
			WHERE queue_name = $1
			  AND status = $2
			  AND (application_version = $3 OR application_version IS NULL)
			ORDER BY created_at ASC
			%s`, lockClause)
	}

	if maxTasks >= 0 {
		query += fmt.Sprintf(" LIMIT %d", int(maxTasks))
	}

	// Execute the query to get workflow IDs
	rows, err := tx.Query(ctx, query, input.queue.Name, WorkflowStatusEnqueued, input.applicationVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to query enqueued workflows: %w", err)
	}
	defer rows.Close()

	var dequeuedIDs []string
	for rows.Next() {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			s.logger.Warn("DequeueWorkflows context cancelled while reading dequeue results", "cause", context.Cause(ctx))
			return nil, ctx.Err()
		default:
		}
		var workflowID string
		if err := rows.Scan(&workflowID); err != nil {
			return nil, fmt.Errorf("failed to scan workflow ID: %w", err)
		}
		dequeuedIDs = append(dequeuedIDs, workflowID)
	}

	if len(dequeuedIDs) > 0 {
		s.logger.Debug("attempting to dequeue task(s)", "queueName", input.queue.Name, "numTasks", len(dequeuedIDs))
	}

	// Update workflows to PENDING status and get their details
	var retWorkflows []dequeuedWorkflow
	for _, id := range dequeuedIDs {
		// If we have a limiter, stop dequeueing workflows when the number of workflows started this period exceeds the limit.
		if input.queue.RateLimit != nil {
			if len(retWorkflows)+numRecentQueries >= input.queue.RateLimit.Limit {
				break
			}
		}
		retWorkflow := dequeuedWorkflow{
			id: id,
		}

		// Update workflow status to PENDING and return name and inputs
		updateQuery := `
			UPDATE dbos.workflow_status
			SET status = $1,
			    application_version = $2,
			    executor_id = $3,
			    started_at_epoch_ms = $4,
			    workflow_deadline_epoch_ms = CASE
			        WHEN workflow_timeout_ms IS NOT NULL AND workflow_deadline_epoch_ms IS NULL
			        THEN EXTRACT(epoch FROM NOW()) * 1000 + workflow_timeout_ms
			        ELSE workflow_deadline_epoch_ms
			    END
			WHERE workflow_uuid = $5
			RETURNING name, inputs`

		var inputString *string
		err := tx.QueryRow(ctx, updateQuery,
			WorkflowStatusPending,
			input.applicationVersion,
			input.executorID,
			startTimeMs,
			id).Scan(&retWorkflow.name, &inputString)

		if inputString != nil && len(*inputString) > 0 {
			retWorkflow.input = *inputString
		}

		if err != nil {
			return nil, fmt.Errorf("failed to update workflow %s during dequeue: %w", id, err)
		}
		retWorkflows = append(retWorkflows, retWorkflow)
	}

	// Commit only if workflows were dequeued. Avoids WAL bloat and XID advancement.
	if len(retWorkflows) > 0 {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("failed to commit transaction: %w", err)
		}
	}

	return retWorkflows, nil
}

func (s *sysDB) clearQueueAssignment(ctx context.Context, workflowID string) (bool, error) {
	query := `UPDATE dbos.workflow_status
			  SET status = $1, started_at_epoch_ms = NULL
			  WHERE workflow_uuid = $2
			    AND queue_name IS NOT NULL
			    AND status = $3`

	commandTag, err := s.pool.Exec(ctx, query,
		WorkflowStatusEnqueued,
		workflowID,
		WorkflowStatusPending)

	if err != nil {
		return false, fmt.Errorf("failed to clear queue assignment for workflow %s: %w", workflowID, err)
	}

	// If no rows were affected, the workflow is not anymore in the queue or was already completed
	return commandTag.RowsAffected() > 0, nil
}

/*******************************/
/******* UTILS ********/
/*******************************/

func (s *sysDB) resetSystemDB(ctx context.Context) error {
	// Get the current database configuration from the pool
	config := s.pool.Config()
	if config == nil || config.ConnConfig == nil {
		return fmt.Errorf("failed to get pool configuration")
	}

	// Extract the database name before closing the pool
	dbName := config.ConnConfig.Database
	if dbName == "" {
		return fmt.Errorf("database name not found in pool configuration")
	}

	// Close the current pool before dropping the database
	s.pool.Close()

	// Create a new connection configuration pointing to the postgres database
	postgresConfig := config.ConnConfig.Copy()
	postgresConfig.Database = "postgres"

	// Connect to the postgres database
	conn, err := pgx.ConnectConfig(ctx, postgresConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL server: %w", err)
	}
	defer conn.Close(ctx)

	// Drop the database
	dropSQL := fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", pgx.Identifier{dbName}.Sanitize())
	_, err = conn.Exec(ctx, dropSQL)
	if err != nil {
		return fmt.Errorf("failed to drop database %s: %w", dbName, err)
	}

	return nil
}

type queryBuilder struct {
	setClauses   []string
	whereClauses []string
	args         []any
	argCounter   int
}

func newQueryBuilder() *queryBuilder {
	return &queryBuilder{
		setClauses:   make([]string, 0),
		whereClauses: make([]string, 0),
		args:         make([]any, 0),
		argCounter:   0,
	}
}

func (qb *queryBuilder) addSet(column string, value any) {
	qb.argCounter++
	qb.setClauses = append(qb.setClauses, fmt.Sprintf("%s=$%d", column, qb.argCounter))
	qb.args = append(qb.args, value)
}

func (qb *queryBuilder) addSetRaw(clause string) {
	qb.setClauses = append(qb.setClauses, clause)
}

func (qb *queryBuilder) addWhere(column string, value any) {
	qb.argCounter++
	qb.whereClauses = append(qb.whereClauses, fmt.Sprintf("%s=$%d", column, qb.argCounter))
	qb.args = append(qb.args, value)
}

func (qb *queryBuilder) addWhereLike(column string, value any) {
	qb.argCounter++
	qb.whereClauses = append(qb.whereClauses, fmt.Sprintf("%s LIKE $%d", column, qb.argCounter))
	qb.args = append(qb.args, value)
}

func (qb *queryBuilder) addWhereAny(column string, values any) {
	qb.argCounter++
	qb.whereClauses = append(qb.whereClauses, fmt.Sprintf("%s = ANY($%d)", column, qb.argCounter))
	qb.args = append(qb.args, values)
}

func (qb *queryBuilder) addWhereGreaterEqual(column string, value any) {
	qb.argCounter++
	qb.whereClauses = append(qb.whereClauses, fmt.Sprintf("%s >= $%d", column, qb.argCounter))
	qb.args = append(qb.args, value)
}

func (qb *queryBuilder) addWhereLessEqual(column string, value any) {
	qb.argCounter++
	qb.whereClauses = append(qb.whereClauses, fmt.Sprintf("%s <= $%d", column, qb.argCounter))
	qb.args = append(qb.args, value)
}
