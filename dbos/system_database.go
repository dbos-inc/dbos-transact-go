package dbos

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"net/url"
	"os"
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

type SystemDatabase interface {
	Launch(ctx context.Context)
	Shutdown()
	ResetSystemDB(ctx context.Context) error
	InsertWorkflowStatus(ctx context.Context, input InsertWorkflowStatusDBInput) (*InsertWorkflowResult, error)
	RecordOperationResult(ctx context.Context, input recordOperationResultDBInput) error
	RecordChildWorkflow(ctx context.Context, input recordChildWorkflowDBInput) error
	CheckChildWorkflow(ctx context.Context, workflowUUID string, functionID int) (*string, error)
	ListWorkflows(ctx context.Context, input ListWorkflowsDBInput) ([]WorkflowStatus, error)
	UpdateWorkflowOutcome(ctx context.Context, input UpdateWorkflowOutcomeDBInput) error
	AwaitWorkflowResult(ctx context.Context, workflowID string) (any, error)
	DequeueWorkflows(ctx context.Context, queue WorkflowQueue) ([]dequeuedWorkflow, error)
	ClearQueueAssignment(ctx context.Context, workflowID string) (bool, error)
	CheckOperationExecution(ctx context.Context, input CheckOperationExecutionDBInput) (*RecordedResult, error)
	RecordChildGetResult(ctx context.Context, input recordChildGetResultDBInput) error
	GetWorkflowSteps(ctx context.Context, workflowID string) ([]StepInfo, error)
	Send(ctx context.Context, input WorkflowSendInput) error
	Recv(ctx context.Context, input WorkflowRecvInput) (any, error)
}

type systemDatabase struct {
	pool                           *pgxpool.Pool
	notificationListenerConnection *pgconn.PgConn
	notificationListenerLoopCancel context.CancelFunc
	notificationsMap               *sync.Map
}

/*******************************/
/******* INITIALIZATION ********/
/*******************************/

// createDatabaseIfNotExists creates the database if it doesn't exist
func createDatabaseIfNotExists(databaseURL string) error {
	// Connect to the postgres database
	parsedURL, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		return NewInitializationError(fmt.Sprintf("failed to parse database URL: %v", err))
	}

	dbName := parsedURL.Database
	if dbName == "" {
		return NewInitializationError("database name not found in URL")
	}

	serverURL := parsedURL.Copy()
	serverURL.Database = "postgres"
	conn, err := pgx.ConnectConfig(context.Background(), serverURL)
	if err != nil {
		return NewInitializationError(fmt.Sprintf("failed to connect to PostgreSQL server: %v", err))
	}
	defer conn.Close(context.Background())

	// Create the system database if it doesn't exist
	var exists bool
	err = conn.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)", dbName).Scan(&exists)
	if err != nil {
		return NewInitializationError(fmt.Sprintf("failed to check if database exists: %v", err))
	}
	if !exists {
		// TODO: validate db name
		createSQL := fmt.Sprintf("CREATE DATABASE %s", pgx.Identifier{dbName}.Sanitize())
		_, err = conn.Exec(context.Background(), createSQL)
		if err != nil {
			return NewInitializationError(fmt.Sprintf("failed to create database %s: %v", dbName, err))
		}
	}

	return nil
}

//go:embed migrations/*.sql
var migrationFiles embed.FS

// TODO: must use the systemdb name
func runMigrations(databaseURL string) error {
	// Change the driver to pgx5
	databaseURL = "pgx5://" + strings.TrimPrefix(databaseURL, "postgres://")

	// Create migration source from embedded files
	d, err := iofs.New(migrationFiles, "migrations")
	if err != nil {
		return NewInitializationError(fmt.Sprintf("failed to create migration source: %v", err))
	}

	// Create migrator
	m, err := migrate.NewWithSourceInstance("iofs", d, databaseURL)
	if err != nil {
		return NewInitializationError(fmt.Sprintf("failed to create migrator: %v", err))
	}
	defer m.Close()

	// Run migrations
	// FIXME: tolerate errors when the migration is bcz we run an older version of transact
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return NewInitializationError(fmt.Sprintf("failed to run migrations: %v", err))
	}

	return nil
}

// New creates a new SystemDatabase instance and runs migrations
func NewSystemDatabase() (SystemDatabase, error) {
	// TODO: pass proper config
	databaseURL := os.Getenv("DBOS_DATABASE_URL")
	if databaseURL == "" {
		fmt.Println("DBOS_DATABASE_URL not set, using default: postgres://postgres:${PGPASSWORD}@localhost:5432/dbos?sslmode=disable")
		password := url.QueryEscape(os.Getenv("PGPASSWORD"))
		databaseURL = fmt.Sprintf("postgres://postgres:%s@localhost:5432/dbos?sslmode=disable", password)
	}

	// Create the database if it doesn't exist
	if err := createDatabaseIfNotExists(databaseURL); err != nil {
		return nil, NewInitializationError(fmt.Sprintf("failed to create database: %v", err))
	}

	// Run migrations first
	if err := runMigrations(databaseURL); err != nil {
		return nil, NewInitializationError(fmt.Sprintf("failed to run migrations: %v", err))
	}

	// Create pgx pool
	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		return nil, NewInitializationError(fmt.Sprintf("failed to create connection pool: %v", err))
	}

	// Test the connection
	// FIXME: remove this
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		return nil, NewInitializationError(fmt.Sprintf("failed to ping database: %v", err))
	}

	// Create a map of notification payloads to channels
	notificationsMap := &sync.Map{}

	// Create a connection to listen on notifications
	config, err := pgconn.ParseConfig(databaseURL)
	if err != nil {
		return nil, NewInitializationError(fmt.Sprintf("failed to parse database URL: %v", err))
	}
	config.OnNotification = func(c *pgconn.PgConn, n *pgconn.Notification) {
		if n.Channel == "dbos_notifications_channel" {
			// Check if an entry exists in the map, indexed by the payload
			// If yes, send a signal to the channel so the listener can wake up
			if ch, exists := notificationsMap.Load(n.Payload); exists {
				select {
				case ch.(chan bool) <- true: // Send a signal to wake up the listener
				default:
					getLogger().Warn("notification channel for payload is full, skipping", "payload", n.Payload)
				}
			}
		}
	}
	notificationListenerConnection, err := pgconn.ConnectConfig(context.Background(), config)
	if err != nil {
		return nil, NewInitializationError(fmt.Sprintf("failed to connect notification listener to database: %v", err))
	}

	return &systemDatabase{
		pool:                           pool,
		notificationListenerConnection: notificationListenerConnection,
		notificationsMap:               notificationsMap,
	}, nil
}

func (s *systemDatabase) Launch(ctx context.Context) {
	// Start the notification listener loop
	var notificationListenerLoopContext context.Context
	notificationListenerLoopContext, s.notificationListenerLoopCancel = context.WithCancel(ctx)
	go s.notificationListenerLoop(notificationListenerLoopContext)
	// FIXME: logger not availableyet
	fmt.Println("DBOS: Started notification listener loop")
}

func (s *systemDatabase) Shutdown() {
	getLogger().Info("DBOS: Closing system database connection pool")
	if s.pool != nil {
		s.pool.Close()
	}
	if s.notificationListenerLoopCancel != nil {
		s.notificationListenerLoopCancel()
	}
	s.notificationsMap.Clear()
}

/*******************************/
/******* WORKFLOWS ********/
/*******************************/

type InsertWorkflowResult struct {
	Attempts                int                `json:"attempts"`
	Status                  WorkflowStatusType `json:"status"`
	Name                    string             `json:"name"`
	QueueName               *string            `json:"queue_name"`
	WorkflowDeadlineEpochMs *int64             `json:"workflow_deadline_epoch_ms"`
}

type InsertWorkflowStatusDBInput struct {
	status     WorkflowStatus
	maxRetries int
	tx         pgx.Tx
}

func (s *systemDatabase) InsertWorkflowStatus(ctx context.Context, input InsertWorkflowStatusDBInput) (*InsertWorkflowResult, error) {
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

	var timeoutMs int64 = 0
	if input.status.Timeout > 0 {
		timeoutMs = input.status.Timeout.Milliseconds()
	}

	inputString, err := serialize(input.status.Input)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize input: %w", err)
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
        RETURNING recovery_attempts, status, name, queue_name, workflow_deadline_epoch_ms`

	var result InsertWorkflowResult
	err = input.tx.QueryRow(ctx, query,
		input.status.ID,
		input.status.Status,
		input.status.Name,
		input.status.QueueName,
		input.status.AuthenticatedUser,
		input.status.AssumedRole,
		input.status.AuthenticatedRoles,
		input.status.ExecutorID,
		input.status.ApplicationVersion,
		input.status.ApplicationID,
		input.status.CreatedAt.UnixMilli(),
		attempts,
		updatedAt.UnixMilli(),
		timeoutMs,
		deadline,
		inputString,
		input.status.DeduplicationID,
		input.status.Priority,
		WorkflowStatusEnqueued,
		WorkflowStatusEnqueued,
	).Scan(
		&result.Attempts,
		&result.Status,
		&result.Name,
		&result.QueueName,
		&result.WorkflowDeadlineEpochMs,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to insert workflow status: %w", err)
	}

	if len(input.status.Name) > 0 && result.Name != input.status.Name {
		return nil, NewConflictingWorkflowError(input.status.ID, fmt.Sprintf("Workflow already exists with a different name: %s, but the provided name is: %s", result.Name, input.status.Name))
	}
	if len(input.status.QueueName) > 0 && result.QueueName != nil && input.status.QueueName != *result.QueueName {
		getLogger().Warn("Queue name conflict for workflow", "workflow_id", input.status.ID, "result_queue", *result.QueueName, "status_queue", input.status.QueueName)
	}

	// Every time we start executing a workflow (and thus attempt to insert its status), we increment `recovery_attempts` by 1.
	// When this number becomes equal to `maxRetries + 1`, we mark the workflow as `RETRIES_EXCEEDED`.
	if result.Status != WorkflowStatusSuccess && result.Status != WorkflowStatusError &&
		input.maxRetries > 0 && result.Attempts > input.maxRetries+1 {

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

		return nil, NewDeadLetterQueueError(input.status.ID, input.maxRetries)
	}

	return &result, nil
}

// ListWorkflowsInput represents the input parameters for listing workflows
type ListWorkflowsDBInput struct {
	WorkflowName       string
	QueueName          string
	WorkflowIDPrefix   string
	WorkflowIDs        []string
	AuthenticatedUser  string
	StartTime          time.Time
	EndTime            time.Time
	Status             []WorkflowStatusType
	ApplicationVersion string
	ExecutorIDs        []string
	Limit              *int
	Offset             *int
	SortDesc           bool
	Tx                 pgx.Tx
}

// ListWorkflows retrieves a list of workflows based on the provided filters
func (s *systemDatabase) ListWorkflows(ctx context.Context, input ListWorkflowsDBInput) ([]WorkflowStatus, error) {
	qb := newQueryBuilder()

	// Build the base query
	baseQuery := `SELECT workflow_uuid, status, name, authenticated_user, assumed_role, authenticated_roles,
	                 output, error, executor_id, created_at, updated_at, application_version, application_id,
	                 recovery_attempts, queue_name, workflow_timeout_ms, workflow_deadline_epoch_ms, started_at_epoch_ms,
					 deduplication_id, inputs, priority
	          FROM dbos.workflow_status`

	// Add filters using query builder
	if input.WorkflowName != "" {
		qb.addWhere("name", input.WorkflowName)
	}
	if input.QueueName != "" {
		qb.addWhere("queue_name", input.QueueName)
	}
	if input.WorkflowIDPrefix != "" {
		qb.addWhereLike("workflow_uuid", input.WorkflowIDPrefix+"%")
	}
	if len(input.WorkflowIDs) > 0 {
		qb.addWhereAny("workflow_uuid", input.WorkflowIDs)
	}
	if input.AuthenticatedUser != "" {
		qb.addWhere("authenticated_user", input.AuthenticatedUser)
	}
	if !input.StartTime.IsZero() {
		qb.addWhereGreaterEqual("created_at", input.StartTime.UnixMilli())
	}
	if !input.EndTime.IsZero() {
		qb.addWhereLessEqual("created_at", input.EndTime.UnixMilli())
	}
	if len(input.Status) > 0 {
		qb.addWhereAny("status", input.Status)
	}
	if input.ApplicationVersion != "" {
		qb.addWhere("application_version", input.ApplicationVersion)
	}
	if len(input.ExecutorIDs) > 0 {
		qb.addWhereAny("executor_id", input.ExecutorIDs)
	}

	// Build complete query
	var query string
	if len(qb.whereClauses) > 0 {
		query = fmt.Sprintf("%s WHERE %s", baseQuery, strings.Join(qb.whereClauses, " AND "))
	} else {
		query = baseQuery
	}

	// Add sorting
	if input.SortDesc {
		query += " ORDER BY created_at DESC"
	} else {
		query += " ORDER BY created_at ASC"
	}

	// Add limit and offset
	if input.Limit != nil {
		qb.argCounter++
		query += fmt.Sprintf(" LIMIT $%d", qb.argCounter)
		qb.args = append(qb.args, *input.Limit)
	}

	if input.Offset != nil {
		qb.argCounter++
		query += fmt.Sprintf(" OFFSET $%d", qb.argCounter)
		qb.args = append(qb.args, *input.Offset)
	}

	// Execute the query
	var rows pgx.Rows
	var err error

	if input.Tx != nil {
		rows, err = input.Tx.Query(ctx, query, qb.args...)
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

		err := rows.Scan(
			&wf.ID, &wf.Status, &wf.Name, &wf.AuthenticatedUser, &wf.AssumedRole,
			&wf.AuthenticatedRoles, &outputString, &errorStr, &wf.ExecutorID, &createdAtMs,
			&updatedAtMs, &wf.ApplicationVersion, &wf.ApplicationID,
			&wf.Attempts, &queueName, &timeoutMs,
			&deadlineMs, &startedAtMs, &wf.DeduplicationID,
			&inputString, &wf.Priority,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan workflow row: %w", err)
		}

		if queueName != nil && len(*queueName) > 0 {
			wf.QueueName = *queueName
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

		// Convert error string to error type if present
		if errorStr != nil && *errorStr != "" {
			wf.Error = errors.New(*errorStr)
		}

		// XXX maybe set wf.Output to outputString and run deserialize out of system DB
		wf.Output, err = deserialize(outputString)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize output: %w", err)
		}

		wf.Input, err = deserialize(inputString)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize input: %w", err)
		}

		workflows = append(workflows, wf)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over workflow rows: %w", err)
	}

	return workflows, nil
}

type UpdateWorkflowOutcomeDBInput struct {
	workflowID string
	status     WorkflowStatusType
	output     any
	err        error
	tx         pgx.Tx
}

// Will evolve as we serialize all output and error types
func (s *systemDatabase) UpdateWorkflowOutcome(ctx context.Context, input UpdateWorkflowOutcomeDBInput) error {
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

func (s *systemDatabase) CancelWorkflow(ctx context.Context, workflowID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) // Rollback if not committed

	// Check if workflow exists
	listInput := ListWorkflowsDBInput{
		WorkflowIDs: []string{workflowID},
		Tx:          tx,
	}
	wfs, err := s.ListWorkflows(ctx, listInput)
	if err != nil {
		return err
	}
	if len(wfs) == 0 {
		return NewNonExistentWorkflowError(workflowID)
	}

	wf := wfs[0]
	switch wf.Status {
	case "SUCCESS", "ERROR", "CANCELLED":
		// Workflow is already in a terminal state, rollback and return
		if err := tx.Rollback(ctx); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
		return nil
	}

	//Set the workflow's status to CANCELLED
	updateStatusQuery := `UPDATE dbos.workflow_status
						  SET status = 'CANCELLED', updated_at = $1
						  WHERE workflow_uuid = $2`

	_, err = tx.Exec(ctx, updateStatusQuery, time.Now().UnixMilli(), workflowID)
	if err != nil {
		return fmt.Errorf("failed to update workflow status to CANCELLED: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (s *systemDatabase) AwaitWorkflowResult(ctx context.Context, workflowID string) (any, error) {
	query := `SELECT status, output, error FROM dbos.workflow_status WHERE workflow_uuid = $1`
	var status WorkflowStatusType
	for {
		row := s.pool.QueryRow(ctx, query, workflowID)
		var outputString *string
		var errorStr *string
		err := row.Scan(&status, &outputString, &errorStr)
		if err != nil {
			if err == pgx.ErrNoRows {
				time.Sleep(1 * time.Second)
				continue
			}
			return nil, fmt.Errorf("failed to query workflow status: %w", err)
		}

		switch status {
		case WorkflowStatusSuccess, WorkflowStatusError:
			// Deserialize output from TEXT to bytes then from bytes to R using gob
			output, err := deserialize(outputString)
			if err != nil {
				return nil, fmt.Errorf("failed to deserialize output: %w", err)
			}
			if errorStr == nil || len(*errorStr) == 0 {
				return output, nil
			}
			return output, errors.New(*errorStr)
		case WorkflowStatusCancelled:
			return nil, NewAwaitedWorkflowCancelledError(workflowID)
		default:
			time.Sleep(1 * time.Second) // Wait before checking again
		}
	}
}

type recordOperationResultDBInput struct {
	workflowID    string
	operationID   int
	operationName string
	output        any
	err           error
	tx            pgx.Tx
}

func (s *systemDatabase) RecordOperationResult(ctx context.Context, input recordOperationResultDBInput) error {
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
			input.operationID,
			outputString,
			errorString,
			input.operationName,
		)
	} else {
		commandTag, err = s.pool.Exec(ctx, query,
			input.workflowID,
			input.operationID,
			outputString,
			errorString,
			input.operationName,
		)
	}

	/*
		getLogger().Debug("RecordOperationResult CommandTag", "command_tag", commandTag)
		getLogger().Debug("RecordOperationResult Rows affected", "rows_affected", commandTag.RowsAffected())
		getLogger().Debug("RecordOperationResult SQL", "sql", commandTag.String())
	*/

	// TODO return DBOSWorkflowConflictIDError(result["workflow_uuid"]) on 23505 conflict ID error
	if err != nil {
		getLogger().Error("RecordOperationResult Error occurred", "error", err)
		return fmt.Errorf("failed to record operation result: %w", err)
	}

	if commandTag.RowsAffected() == 0 {
		getLogger().Warn("RecordOperationResult No rows were affected by the insert")
	}

	return nil
}

/*******************************/
/******* CHILD WORKFLOWS ********/
/*******************************/

type recordChildWorkflowDBInput struct {
	parentWorkflowID string
	childWorkflowID  string
	functionID       int
	functionName     string
	tx               pgx.Tx
}

func (s *systemDatabase) RecordChildWorkflow(ctx context.Context, input recordChildWorkflowDBInput) error {
	query := `INSERT INTO dbos.operation_outputs
            (workflow_uuid, function_id, function_name, child_workflow_id)
            VALUES ($1, $2, $3, $4)`

	var commandTag pgconn.CommandTag
	var err error

	if input.tx != nil {
		commandTag, err = input.tx.Exec(ctx, query,
			input.parentWorkflowID,
			input.functionID,
			input.functionName,
			input.childWorkflowID,
		)
	} else {
		commandTag, err = s.pool.Exec(ctx, query,
			input.parentWorkflowID,
			input.functionID,
			input.functionName,
			input.childWorkflowID,
		)
	}

	if err != nil {
		// Check for unique constraint violation (conflict ID error)
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "23505" {
			return fmt.Errorf(
				"child workflow %s already registered for parent workflow %s (operation ID: %d)",
				input.childWorkflowID, input.parentWorkflowID, input.functionID)
		}
		return fmt.Errorf("failed to record child workflow: %w", err)
	}

	if commandTag.RowsAffected() == 0 {
		getLogger().Warn("RecordChildWorkflow No rows were affected by the insert")
	}

	return nil
}

func (s *systemDatabase) CheckChildWorkflow(ctx context.Context, workflowID string, functionID int) (*string, error) {
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
	operationID      int
	output           string
	err              error
}

func (s *systemDatabase) RecordChildGetResult(ctx context.Context, input recordChildGetResultDBInput) error {
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
		input.operationID,
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

type RecordedResult struct {
	output any
	err    error
}

type CheckOperationExecutionDBInput struct {
	workflowID   string
	operationID  int
	functionName string
	tx           pgx.Tx
}

func (s *systemDatabase) CheckOperationExecution(ctx context.Context, input CheckOperationExecutionDBInput) (*RecordedResult, error) {
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
	operationOutputQuery := `SELECT output, error, function_name
							 FROM dbos.operation_outputs
							 WHERE workflow_uuid = $1 AND function_id = $2`

	var workflowStatus WorkflowStatusType

	// Execute first query to get workflow status
	err = tx.QueryRow(ctx, workflowStatusQuery, input.workflowID).Scan(&workflowStatus)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, NewNonExistentWorkflowError(input.workflowID)
		}
		return nil, fmt.Errorf("failed to get workflow status: %w", err)
	}

	// If the workflow is cancelled, raise the exception
	if workflowStatus == WorkflowStatusCancelled {
		return nil, NewWorkflowCancelledError(input.workflowID)
	}

	// Execute second query to get operation outputs
	var outputString *string
	var errorStr *string
	var recordedFunctionName string

	err = tx.QueryRow(ctx, operationOutputQuery, input.workflowID, input.operationID).Scan(&outputString, &errorStr, &recordedFunctionName)

	// If there are no operation outputs, return nil
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get operation outputs: %w", err)
	}

	// If the provided and recorded function name are different, throw an exception
	if input.functionName != recordedFunctionName {
		return nil, NewUnexpectedStepError(input.workflowID, input.operationID, input.functionName, recordedFunctionName)
	}

	output, err := deserialize(outputString)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize output: %w", err)
	}

	var recordedError error
	if errorStr != nil && *errorStr != "" {
		recordedError = errors.New(*errorStr)
	}
	result := &RecordedResult{
		output: output,
		err:    recordedError,
	}
	return result, nil
}

type StepInfo struct {
	FunctionID      int
	FunctionName    string
	Output          any
	Error           error
	ChildWorkflowID string
}

func (s *systemDatabase) GetWorkflowSteps(ctx context.Context, workflowID string) ([]StepInfo, error) {
	query := `SELECT function_id, function_name, output, error, child_workflow_id
			  FROM dbos.operation_outputs
			  WHERE workflow_uuid = $1`

	rows, err := s.pool.Query(ctx, query, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to query workflow steps: %w", err)
	}
	defer rows.Close()

	var steps []StepInfo
	for rows.Next() {
		var step StepInfo
		var outputString *string
		var errorString *string
		var childWorkflowID *string

		err := rows.Scan(&step.FunctionID, &step.FunctionName, &outputString, &errorString, &childWorkflowID)
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

/****************************************/
/******* WORKFLOW COMMUNICATIONS ********/
/****************************************/

func (s *systemDatabase) notificationListenerLoop(ctx context.Context) {
	mrr := s.notificationListenerConnection.Exec(ctx, "LISTEN dbos_notifications_channel")
	results, err := mrr.ReadAll()
	if err != nil {
		getLogger().Error("Failed to listen on dbos_notifications_channel", "error", err)
		return
	}
	mrr.Close()

	for _, result := range results {
		if result.Err != nil {
			getLogger().Error("Error listening on dbos_notifications_channel", "error", result.Err)
			return
		}
	}

	for {
		// Block until a notification is received. OnNotification will be called when a notification is received.
		// WaitForNotification handles context cancellation: https://github.com/jackc/pgx/blob/15bca4a4e14e0049777c1245dba4c16300fe4fd0/pgconn/pgconn.go#L1050
		err := s.notificationListenerConnection.WaitForNotification(ctx)
		if err != nil {
			if err == context.Canceled {
				getLogger().Info("Notification listener loop exiting due to context cancellation", "error", ctx.Err())
				return
			}
			getLogger().Error("Error waiting for notification", "error", err)
			time.Sleep(1 * time.Second)
			continue
		}

	}
}

const DBOS_NULL_TOPIC = "__null__topic__"

// Send is a special type of step that sends a message to another workflow.
// Three differences with a normal steps: durability and the function run in the same transaction, and we forbid nested step execution
func (s *systemDatabase) Send(ctx context.Context, input WorkflowSendInput) error {
	functionName := "DBOS.send"

	// Get workflow state from context
	workflowState, ok := ctx.Value(WorkflowStateKey).(*WorkflowState)
	if !ok || workflowState == nil {
		return NewStepExecutionError("", functionName, "workflow state not found in context: are you running this step within a workflow?")
	}

	if workflowState.isWithinStep {
		return NewStepExecutionError(workflowState.WorkflowID, functionName, "cannot call Send within a step")
	}

	stepID := workflowState.NextStepID()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Check if operation was already executed and do nothing if so
	checkInput := CheckOperationExecutionDBInput{
		workflowID:   workflowState.WorkflowID,
		operationID:  stepID,
		functionName: functionName,
		tx:           tx,
	}
	recordedResult, err := s.CheckOperationExecution(ctx, checkInput)
	if err != nil {
		return err
	}
	if recordedResult != nil {
		return nil
	}

	// Set default topic if not provided
	topic := DBOS_NULL_TOPIC
	if len(input.Topic) > 0 {
		topic = input.Topic
	}

	// Serialize the message. It must have been registered with encoding/gob by the user if not a basic type.
	messageString, err := serialize(input.Message)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %w", err)
	}

	insertQuery := `INSERT INTO dbos.notifications (destination_uuid, topic, message)
					VALUES ($1, $2, $3)`

	_, err = tx.Exec(ctx, insertQuery, input.DestinationID, topic, messageString)
	if err != nil {
		// Check for foreign key violation (destination workflow doesn't exist)
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "23503" {
			return NewNonExistentWorkflowError(input.DestinationID)
		}
		return fmt.Errorf("failed to insert notification: %w", err)
	}

	// Record the operation result
	recordInput := recordOperationResultDBInput{
		workflowID:    workflowState.WorkflowID,
		operationID:   stepID,
		operationName: functionName,
		output:        nil,
		err:           nil,
		tx:            tx,
	}

	err = s.RecordOperationResult(ctx, recordInput)
	if err != nil {
		return fmt.Errorf("failed to record operation result: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Recv is a special type of step that receives a message destined for a given workflow
func (s *systemDatabase) Recv(ctx context.Context, input WorkflowRecvInput) (any, error) {
	functionName := "DBOS.recv"

	// Get workflow state from context
	// XXX these checks might be better suited for outside of the system db code. We'll see when we implement the client.
	workflowState, ok := ctx.Value(WorkflowStateKey).(*WorkflowState)
	if !ok || workflowState == nil {
		return nil, NewStepExecutionError("", functionName, "workflow state not found in context: are you running this step within a workflow?")
	}

	if workflowState.isWithinStep {
		return nil, NewStepExecutionError(workflowState.WorkflowID, functionName, "cannot call Recv within a step")
	}

	stepID := workflowState.NextStepID()
	destinationID := workflowState.WorkflowID

	// Set default topic if not provided
	topic := DBOS_NULL_TOPIC
	if len(input.Topic) > 0 {
		topic = input.Topic
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Check if operation was already executed
	// XXX this might not need to be in the transaction
	checkInput := CheckOperationExecutionDBInput{
		workflowID:   destinationID,
		operationID:  stepID,
		functionName: functionName,
		tx:           tx,
	}
	recordedResult, err := s.CheckOperationExecution(ctx, checkInput)
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
	c := make(chan bool, 1) // Make it buffered to allow the notification listener to post a signal even if the receiver has not reached its select statement yet
	_, loaded := s.notificationsMap.LoadOrStore(payload, c)
	if loaded {
		close(c)
		getLogger().Error("Receive already called for workflow", "destination_id", destinationID)
		return nil, NewWorkflowConflictIDError(destinationID)
	}
	defer func() {
		// Clean up the channel after we're done
		// XXX We should handle panics in this function and make sure we call this. Not a problem for now as panic will crash the importing package.
		s.notificationsMap.Delete(payload)
		close(c)
	}()

	// Now check if there is already a message available in the database.
	// If not, we'll wait for a notification and timeout
	var exists bool
	query := `SELECT EXISTS (SELECT 1 FROM dbos.notifications WHERE destination_uuid = $1 AND topic = $2)`
	err = tx.QueryRow(ctx, query, destinationID, topic).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check message: %w", err)
	}
	if !exists {
		// Listen for notifications on the channel
		// XXX should we prevent zero or negative timeouts?
		getLogger().Debug("Waiting for notification on channel", "payload", payload)
		select {
		case <-c:
			getLogger().Debug("Received notification on channel", "payload", payload)
		case <-time.After(input.Timeout):
			// If we reach the timeout, we can check if there is a message in the database, and if not return nil.
			getLogger().Warn("Timeout reached for channel", "payload", payload, "timeout", input.Timeout)
		}
	}

	// Find the oldest message and delete it atomically
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
		workflowID:    destinationID,
		operationID:   stepID,
		operationName: functionName,
		output:        message,
		tx:            tx,
	}
	err = s.RecordOperationResult(ctx, recordInput)
	if err != nil {
		return nil, fmt.Errorf("failed to record operation result: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return message, nil
}

/*******************************/
/******* QUEUES ********/
/*******************************/

type dequeuedWorkflow struct {
	id    string
	name  string
	input string
}

func (s *systemDatabase) DequeueWorkflows(ctx context.Context, queue WorkflowQueue) ([]dequeuedWorkflow, error) {
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
	if queue.Limiter != nil {
		limiterPeriod := time.Duration(queue.Limiter.Period * float64(time.Second))

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
			queue.Name,
			WorkflowStatusEnqueued,
			cutoffTimeMs).Scan(&numRecentQueries)
		if err != nil {
			return nil, fmt.Errorf("failed to query rate limiter: %w", err)
		}

		if numRecentQueries >= queue.Limiter.Limit {
			return []dequeuedWorkflow{}, nil
		}
	}

	// Calculate max_tasks based on concurrency limits
	maxTasks := queue.MaxTasksPerIteration

	if queue.WorkerConcurrency != nil || queue.GlobalConcurrency != nil {
		// Count pending workflows by executor
		pendingQuery := `
			SELECT executor_id, COUNT(*) as task_count
			FROM dbos.workflow_status
			WHERE queue_name = $1 AND status = $2
			GROUP BY executor_id`

		rows, err := tx.Query(ctx, pendingQuery, queue.Name, WorkflowStatusPending)
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

		localPendingWorkflows := pendingWorkflowsDict[EXECUTOR_ID]

		// Check worker concurrency limit
		if queue.WorkerConcurrency != nil {
			workerConcurrency := *queue.WorkerConcurrency
			if localPendingWorkflows > workerConcurrency {
				getLogger().Warn("Local pending workflows on queue exceeds worker concurrency limit", "local_pending", localPendingWorkflows, "queue_name", queue.Name, "concurrency_limit", workerConcurrency)
			}
			availableWorkerTasks := max(workerConcurrency-localPendingWorkflows, 0)
			maxTasks = uint(availableWorkerTasks)
		}

		// Check global concurrency limit
		if queue.GlobalConcurrency != nil {
			globalPendingWorkflows := 0
			for _, count := range pendingWorkflowsDict {
				globalPendingWorkflows += count
			}

			concurrency := *queue.GlobalConcurrency
			if globalPendingWorkflows > concurrency {
				getLogger().Warn("Total pending workflows on queue exceeds global concurrency limit", "total_pending", globalPendingWorkflows, "queue_name", queue.Name, "concurrency_limit", concurrency)
			}
			availableTasks := max(concurrency-globalPendingWorkflows, 0)
			if uint(availableTasks) < maxTasks {
				maxTasks = uint(availableTasks)
			}
		}
	}

	// Build the query to select workflows for dequeueing
	// Use SKIP LOCKED when no global concurrency is set to avoid blocking,
	// otherwise use NOWAIT to ensure consistent view across processes
	skipLocks := queue.GlobalConcurrency == nil
	var lockClause string
	if skipLocks {
		lockClause = "FOR UPDATE SKIP LOCKED"
	} else {
		lockClause = "FOR UPDATE NOWAIT"
	}

	var query string
	if queue.PriorityEnabled {
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

	// Add limit if maxTasks is finite
	if maxTasks > 0 {
		query += fmt.Sprintf(" LIMIT %d", int(maxTasks))
	}

	// Execute the query to get workflow IDs
	rows, err := tx.Query(ctx, query, queue.Name, WorkflowStatusEnqueued, APP_VERSION)
	if err != nil {
		return nil, fmt.Errorf("failed to query enqueued workflows: %w", err)
	}
	defer rows.Close()

	var dequeuedIDs []string
	for rows.Next() {
		var workflowID string
		if err := rows.Scan(&workflowID); err != nil {
			return nil, fmt.Errorf("failed to scan workflow ID: %w", err)
		}
		dequeuedIDs = append(dequeuedIDs, workflowID)
	}

	if len(dequeuedIDs) > 0 {
		// fmt.Printf("[%s] attempting to dequeue %d task(s)\n", queue.Name, len(dequeuedIDs))
	}

	// Update workflows to PENDING status and get their details
	var retWorkflows []dequeuedWorkflow
	for _, id := range dequeuedIDs {
		// If we have a limiter, stop dequeueing workflows when the number of workflows started this period exceeds the limit.
		if queue.Limiter != nil {
			if len(retWorkflows)+numRecentQueries >= queue.Limiter.Limit {
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
			APP_VERSION,
			EXECUTOR_ID,
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

func (s *systemDatabase) ClearQueueAssignment(ctx context.Context, workflowID string) (bool, error) {
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

func (s *systemDatabase) ResetSystemDB(ctx context.Context) error {
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
