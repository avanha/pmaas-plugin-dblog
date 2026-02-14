package writer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"net"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/avanha/pmaas-plugin-dblog/config"
	"github.com/avanha/pmaas-plugin-dblog/internal/common"
	"github.com/lib/pq"

	commonslices "github.com/avanha/pmaas-common/slices"
)

const allowNotNull = true

type DbWriter struct {
	ctx                     context.Context
	config                  config.PluginConfig
	connectionFactoryFn     func() (*sql.DB, error)
	requestCh               chan Request
	db                      *sql.DB
	connectionHealthy       bool
	lastConnectionCheckTime time.Time
	retryQueue              []Request
	statsTracker            common.WriterStatsTracker
}

type dbConnectionStatus struct {
	healthy bool
}

type columnWithDeclaration struct {
	column           Column
	declaration      string
	dataTypeMismatch bool
}

func CreateDbWriter(
	ctx context.Context,
	pluginConfig config.PluginConfig,
	connectionFactoryFn func() (*sql.DB, error),
	requestCh chan Request,
	statsTracker common.WriterStatsTracker) *DbWriter {
	return &DbWriter{
		ctx:                 ctx,
		config:              pluginConfig,
		connectionFactoryFn: connectionFactoryFn,
		requestCh:           requestCh,
		retryQueue:          make([]Request, 0, 10),
		statsTracker:        statsTracker,
	}
}

func (w *DbWriter) Init() error {
	return w.connect()
}

func (w *DbWriter) Run() {
	run := true
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

RequestProcessing:

	for run {
		select {
		case request, ok := <-w.requestCh:
			if ok {
				w.processRequestAndMaybeRetryQueue(&request)
			} else {
				run = false
				break RequestProcessing
			}
			break
		case <-ticker.C:
			w.checkConnection()
			break
		}
	}

	err := w.close()

	if err == nil {
		fmt.Printf("DbWriter terminated successfully\n")
	} else {
		fmt.Printf("DbWriter terminating with error: %v\n", err)
	}
}

func (w *DbWriter) connect() error {
	db, err := w.connectionFactoryFn()

	if err != nil {
		return fmt.Errorf("unable to connect to database: %v", err)
	}

	err = db.PingContext(w.ctx)

	if err != nil {
		return fmt.Errorf("unable to connect to database: %v", err)
	}

	w.db = db
	w.connectionHealthy = true
	w.lastConnectionCheckTime = time.Now()

	return nil
}

func (w *DbWriter) close() error {
	if w.db == nil {
		return nil
	}

	err := w.db.Close()

	if err != nil {
		return fmt.Errorf("unable to close database connection: %v", err)
	}

	w.db = nil

	return nil
}

func (w *DbWriter) checkConnection() {
	if time.Since(w.lastConnectionCheckTime) < 59*time.Second {
		return
	}

	if !w.connectionHealthy {
		w.lastConnectionCheckTime = time.Now()
		err := w.db.PingContext(w.ctx)

		if err != nil {
			fmt.Printf("DbWriter connection still down: %v\n", err)
			return
		}

		fmt.Printf("DbWriter connection reestablished\n")
		w.connectionHealthy = true
	}

	if len(w.retryQueue) == 0 {
		return
	}

	w.connectionHealthy = w.processRetryQueue()
}

func (w *DbWriter) processRetryQueue() bool {
	oldCount := len(w.retryQueue)

	if oldCount == 0 {
		return true
	}

	fmt.Printf("DbWriter retrying %d request(s)\n", oldCount)

	connectionHealthy, queue, i := w.processRetryQueueSnapshot()

	if connectionHealthy && len(w.retryQueue) > 0 {
		// If any requests failed and enqueued for retry, despite ending the run with a healthy connection,
		// it's possible the requests failed due to a missing table that was re-created at a later point in the
		// run.  Run through the retry queue one more time.
		fmt.Printf("DbWriter %d request(s) re-enqueued during retry run, retrying immediately\n",
			len(w.retryQueue))

		connectionHealthy, queue, i = w.processRetryQueueSnapshot()
	}

	if !connectionHealthy {
		fmt.Printf("DbWriter connection lost while processing retry queue\n")
	}

	if i < len(queue) {
		// Transfer remaining requests to the new retry queue
		w.retryQueue = append(w.retryQueue, queue[i:]...)
	}

	newCount := len(w.retryQueue)

	fmt.Printf("DbWriter successfully processed %d of %d request(s) from the retry queue\n",
		oldCount-newCount, oldCount)

	if oldCount != newCount {
		w.statsTracker.UpdateRetryQueueSize(newCount)
	}

	return connectionHealthy
}

func (w *DbWriter) processRetryQueueSnapshot() (bool, []Request, int) {
	queue := w.retryQueue
	queueSize := len(queue)
	w.retryQueue = make([]Request, 0, queueSize)
	connectionHealthy := true
	i := 0

	for ; connectionHealthy && i < queueSize; i++ {
		connectionHealthy = w.processRequest(&queue[i])
	}

	return connectionHealthy, queue, i
}

func (w *DbWriter) processRequestAndMaybeRetryQueue(request *Request) {
	connectionHealthy := w.processRequest(request)
	w.lastConnectionCheckTime = time.Now()

	if w.connectionHealthy {
		if connectionHealthy {
			// No change in state: was healthy and is still healthy
		} else {
			fmt.Printf("DbWriter connection lost while processing request\n")
			w.connectionHealthy = false
		}
	} else {
		if connectionHealthy {
			fmt.Printf("DbWriter connection found to be healthy while processing request\n")
			w.connectionHealthy = true
			w.connectionHealthy = w.processRetryQueue()
		} else {
			// No change in state: was unhealthy and is still unhealthy.
		}
	}
}

func (w *DbWriter) processRequest(request *Request) bool {
	var err error = nil
	var tableContext *TableContext = nil
	dbStatus := dbConnectionStatus{}

	if request.TableOperation.Operation != 0 {
		tableContext, err = w.processTableOperation(&request.TableOperation, &dbStatus)
	} else if request.InsertDataOperation.Data != nil {
		err = w.processInsertDataOperation(&request.InsertDataOperation, &dbStatus)
	}

	queuedForRetry := false
	var reportedError error = nil

	if err != nil {
		var retryableError *RetryableError

		if errors.As(err, &retryableError) {
			// Create a copy of the request and clear the response channel since we're hiding
			// the retry queue details from the caller.
			requestForRetry := *request
			requestForRetry.ResponseCh = nil
			fmt.Printf("DbWriter %s failed, enqueueing for retry: %v\n", requestForRetry.Describe(), retryableError)
			w.retryQueue = append(w.retryQueue, requestForRetry)
			queuedForRetry = true
			reportedError = nil
		} else {
			reportedError = err
		}
	}

	if request.ResponseCh != nil {
		request.ResponseCh <- Response{
			Error:          reportedError,
			Context:        tableContext,
			QueuedForRetry: queuedForRetry,
		}

		close(request.ResponseCh)
	}

	if err == nil {
		w.statsTracker.ReportSuccess(len(w.retryQueue))
	} else {
		w.statsTracker.ReportFailure(err, len(w.retryQueue))
	}

	return dbStatus.healthy
}

func (w *DbWriter) processTableOperation(operation *TableOperation, dbStatus *dbConnectionStatus) (*TableContext, error) {
	if operation.Operation == TableOperationCreateIfNotExists {
		return w.processTableCreateIfNotExists(operation, dbStatus)
	}

	return nil, fmt.Errorf("unknown table operation: %d", operation.Operation)
}

func (w *DbWriter) processTableCreateIfNotExists(operation *TableOperation, dbStatus *dbConnectionStatus) (*TableContext, error) {
	// In PostgreSQL table names are created in lowercase, and we need to search for them by lowercase.
	tableName := strings.ToLower(operation.Name)

	tableContext := TableContext{
		tableName:          tableName,
		fields:             w.buildSourceFields(operation.Columns),
		insertStatement:    w.buildInsertStatement(tableName, operation.Columns),
		hasOnChangeColumns: w.calculateHasOnChangeColumns(operation.Columns),
	}

	columnsWithDeclaration, err := w.calcColumnDeclarations(operation.Columns)

	if err != nil {
		return &tableContext, err
	}

	tableContext.columns = columnsWithDeclaration

	exists, err := w.tableExists(tableName, dbStatus)

	if err != nil {
		return &tableContext, err
	}

	// Mark the database connection as healthy since we were able to complete the table check query. Other statements
	// might still fail, but that's a separate problem.
	dbStatus.healthy = true

	if exists {
		missingColumns, err := w.tableContainsAllColumns(
			tableName, tableContext.columns, dbStatus)

		if err != nil {
			return &tableContext, err
		}

		if len(missingColumns) == 0 {
			return &tableContext, nil
		}

		err = w.addTableColumns(&tableContext, tableName, missingColumns, dbStatus)

		if err != nil {
			return &tableContext, fmt.Errorf("unable to add columns to table %s: %v", tableName, err)
		}

		return &tableContext, nil
	}

	err = w.createTable(tableName, operation.Columns, dbStatus)

	if err == nil {
		return &tableContext, nil
	}

	return &tableContext, err
}

func (w *DbWriter) calcColumnDeclarations(columns []Column) ([]columnWithDeclaration, error) {
	result := make([]columnWithDeclaration, len(columns))

	for i := 0; i < len(columns); i++ {
		columnType, err := columnTypeString(&columns[i], allowNotNull)

		if err != nil {
			return nil, err
		}

		result[i] = columnWithDeclaration{
			column:      columns[i],
			declaration: columnType,
		}
	}

	return result, nil
}

func (w *DbWriter) tableExists(tableName string, dbStatus *dbConnectionStatus) (bool, error) {
	rows, err := w.db.QueryContext(
		w.ctx,
		fmt.Sprintf(
			"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '%s'",
			tableName))

	if err != nil {
		return false, w.handleDbError(
			fmt.Sprintf("unable to check if table %s exists", tableName),
			dbStatus,
			err)
	}

	defer func() {
		err := rows.Close()

		if err != nil {
			fmt.Printf("Error closing tables cursor: %v\n", err)
		}
	}()

	var count int

	if rows.Next() {
		err = rows.Scan(&count)

		if err != nil {
			return false, fmt.Errorf("unable to read table count: %v", err)
		}
	} else {
		return false, fmt.Errorf("no rows returned")
	}

	return count == 1, nil
}

type dbColumnResult struct {
	name      string
	dataType  int
	maxLength int
	nullable  bool
}

func (w *DbWriter) tableContainsAllColumns(
	tableName string,
	columns []columnWithDeclaration,
	dbStatus *dbConnectionStatus) ([]columnWithDeclaration, error) {
	rows, err := w.db.QueryContext(w.ctx, fmt.Sprintf("SELECT column_name, data_type, character_maximum_length, is_nullable FROM information_schema.columns WHERE table_name = '%s'", tableName))

	if err != nil {
		return nil,
			w.handleDbError(
				fmt.Sprintf("unable to retrieve table %s columns", tableName),
				dbStatus,
				err)
	}

	defer func() {
		err := rows.Close()

		if err != nil {
			fmt.Printf("Error closing column cursor: %v\n", err)
		}
	}()

	existingColumns := make([]dbColumnResult, 0, len(columns)+10)

	for rows.Next() {
		var name, dataTypeValue, nullableValue string
		var maxLength *int
		err := rows.Scan(&name, &dataTypeValue, &maxLength, &nullableValue)

		if err != nil {
			return nil,
				w.handleDbError(
					fmt.Sprintf("unable to read column info for table %s: %v", tableName, err),
					dbStatus,
					err)

		}

		dataType, err := interpretDataType(dataTypeValue)

		if err != nil {
			return nil, fmt.Errorf("unable to interpret data type %s: %w", dataTypeValue, err)
		}

		nullable, err := interpretNullable(nullableValue)

		if err != nil {
			return nil, fmt.Errorf("unable to interpret nullable value %s: %w", nullableValue, err)
		}

		existingColumns = append(existingColumns, dbColumnResult{
			name:      name,
			dataType:  dataType,
			maxLength: interpretMaxLength(maxLength),
			nullable:  nullable,
		})
	}

	requiredColumns := make(map[string]columnWithDeclaration, len(columns))

	for i := 0; i < len(columns); i++ {
		requiredColumns[strings.ToLower(columns[i].column.Name)] = columns[i]
	}

	for _, existingColumn := range existingColumns {
		key := strings.ToLower(existingColumn.name)
		requiredColumn, ok := requiredColumns[key]

		if !ok {
			// An extra column in the table is fine, we don't care
			continue
		}

		if requiredColumn.column.DataType != existingColumn.dataType {
			return nil, fmt.Errorf(
				"column %s has different data type in the existing table: expected %s, got %s",
				requiredColumn.column.Name,
				requiredColumn.declaration,

				existingColumn.dataType)
		}

		delete(requiredColumns, key)
	}

	return slices.Collect(maps.Values(requiredColumns)), nil
}

func interpretDataType(dataType string) (int, error) {
	switch dataType {
	case "bigint":
		return DataTypeBigInt, nil
	case "integer":

		return DataTypeInt, nil
	case "character varying":
		return DataTypeVarChar, nil
	case "timestamp with time zone":
		return DataTypeTimestamp, nil
	}

	return 0, fmt.Errorf("unknown data type: %s", dataType)
}

func interpretMaxLength(maxLength *int) int {
	if maxLength == nil {
		return 0
	}

	return *maxLength
}

func interpretNullable(nullable string) (bool, error) {
	if nullable == "YES" {
		return true, nil
	}

	if nullable == "NO" {
		return false, nil
	}

	return false, fmt.Errorf("unexpected nullable value: %s", nullable)
}

func (w *DbWriter) addTableColumns(tableContext *TableContext, tableName string, missingColumns []columnWithDeclaration, dbStatus *dbConnectionStatus) error {
	missingColumnDescription := strings.Join(
		commonslices.Apply(
			missingColumns,
			func(cwd *columnWithDeclaration) string {
				return fmt.Sprintf("%s %s", cwd.column.Name, cwd.declaration)
			}),
		", ")
	fmt.Printf("Altering table %s adding columns %+v\n", tableName, missingColumnDescription)

	columns := ""

	for i, column := range missingColumns {
		columnType, err := columnTypeString(&column.column, allowNotNull)

		if err != nil {
			return fmt.Errorf("unable to obtain column declaration for column %s: %w", column.column.Name, err)
		}

		if i > 0 {
			columns += ",\n "
		}

		columns += fmt.Sprintf("\tADD COLUMN %s %s", column.column.Name, columnType)
	}

	ddl := fmt.Sprintf("ALTER TABLE %s\n%s;", tableName, columns)
	fmt.Printf("Adding columns to table %s, DDL: %s\n", tableName, ddl)
	// Implement, PostgreSQL syntax:
	// ALTER TABLE table_name
	// ADD COLUMN new_column_name data_type [constraint];

	_, err := w.db.ExecContext(w.ctx, ddl)

	if err != nil {
		return w.handleDbError(fmt.Sprintf("unable to alter table %s", ddl), dbStatus, err)
	}

	return nil
}

func (w *DbWriter) buildSourceFields(columns []Column) []reflect.StructField {
	fields := make([]reflect.StructField, len(columns))

	for i, column := range columns {
		fields[i] = column.Field
	}

	return fields
}

func (w *DbWriter) buildInsertStatement(tableName string, columns []Column) string {
	names := ""
	values := ""

	for i, column := range columns {
		if i > 0 {
			names += ", "
			values += ", "
		}

		names += column.Name
		values += fmt.Sprintf("$%d", i+1)
	}

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", tableName, names, values)
}

func (w *DbWriter) calculateHasOnChangeColumns(columns []Column) bool {
	for i := 0; i < len(columns); i++ {
		if columns[i].Mode == common.FieldModeOnChange {
			return true
		}
	}

	return false
}

func (w *DbWriter) createTable(tableName string, columns []Column, dbStatus *dbConnectionStatus) error {
	fmt.Printf("Creating table %s with %+v\n", tableName, columns)
	tableColumns := "\tid BIGSERIAL PRIMARY KEY"

	for _, column := range columns {
		columnType, err := columnTypeString(&column, allowNotNull)

		if err != nil {
			return fmt.Errorf("unable to obtain column type for column %s: %v", column.Name, err)
		}

		tableColumns += fmt.Sprintf(",\n\t%s %s", column.Name, columnType)
	}

	ddl := fmt.Sprintf("CREATE TABLE %s (\n%s\n);", tableName, tableColumns)

	_, err := w.db.ExecContext(w.ctx, ddl)

	if err != nil {
		return w.handleDbError(fmt.Sprintf("unable to create table %s", ddl), dbStatus, err)
	}

	return nil
}

func (w *DbWriter) processInsertDataOperation(operation *InsertDataOperation,
	dbStatus *dbConnectionStatus) error {
	var err error
	var stmt *sql.Stmt
	var tx *sql.Tx

	tx, err = w.db.BeginTx(w.ctx, nil)

	if err != nil {
		return w.handleDbError("unable to begin transaction", dbStatus, err)
	}

	// Mark database connection as healthy since we were able to begin a transaction.  The insert statement might still
	// fail, but that's a separate problem.
	dbStatus.healthy = true

	defer func() {
		if stmt != nil {
			err := stmt.Close()

			if err != nil {
				fmt.Printf("Error closing preprared statement: %v\n", err)
			}
		}

		if tx != nil {
			err := tx.Rollback()

			if err != nil {
				fmt.Printf("Error rolling back transaction: %v\n", err)
			}
		}
	}()

	stmt, err = w.db.Prepare(operation.Context.insertStatement)

	if err != nil {
		return w.handleDataInsertDbError(
			fmt.Sprintf("unable to prepare insert statement %s", operation.Context.insertStatement),
			dbStatus,
			err)
	}

	var args []any

	if operation.InsertArgFactoryFn == nil {
		args, err = w.getInsertArgsViaReflection(operation.Data, operation.Context)
	} else {
		args, err = operation.InsertArgFactoryFn(&operation.Data)
	}

	if err != nil {
		return fmt.Errorf("unable to obtain insert arguments: %v", err)
	}

	adjustedArgs := w.filterOnChangeArgs(args, operation.Context)

	_, err = stmt.ExecContext(w.ctx, adjustedArgs...)

	closeErr := stmt.Close()
	stmt = nil

	if closeErr != nil {
		fmt.Printf("Error closing preprared statement: %v\n", closeErr)
	}

	if err != nil {
		return w.handleDbError("unable to execute insert statement", dbStatus, err)
	}

	err = tx.Commit()

	if err != nil {
		return w.handleDbError("unable to commit transaction", dbStatus, err)
	}

	tx = nil

	if operation.Context.hasOnChangeColumns {
		operation.Context.previousArgs = args
	}

	return nil
}

func (w *DbWriter) getInsertArgsViaReflection(data any, tableContext *TableContext) ([]any, error) {
	args := make([]any, 0, len(tableContext.fields))

	for _, field := range tableContext.fields {
		value := reflect.ValueOf(data).FieldByIndex(field.Index).Interface()
		args = append(args, value)
	}

	return args, nil
}

func (w *DbWriter) filterOnChangeArgs(args []any, context *TableContext) []any {
	if !context.hasOnChangeColumns {
		return args
	}

	if context.previousArgs == nil {
		return args
	}

	argsLength := len(args)

	if argsLength != len(context.previousArgs) || argsLength != len(context.columns) {
		return args
	}

	newArgs := make([]any, argsLength)

	for i := range args {
		if context.columns[i].column.Mode == common.FieldModeOnChange &&
			w.argsAreEqual(&context.columns[i].column, args[i], context.previousArgs[i]) {
			newArgs[i] = nil
		} else {
			newArgs[i] = args[i]
		}
	}

	return newArgs
}

func (w *DbWriter) argsAreEqual(_ *Column, arg any, previousArg any) bool {
	return arg == previousArg
}

func (w *DbWriter) handleDataInsertDbError(message string,
	dbStatus *dbConnectionStatus, err error) error {
	var postgresError *pq.Error

	if errors.As(err, &postgresError) {
		if postgresError.Code == "42P01" {
			// Table does not exist: This can happen if the table create attempt failed due to a network error, and
			// it was enqueued for retry.  Later the connection comes back online, and we attempt an insert into the
			// table.  In this case, we can enqueue the insert attempt for retry and try again after the current
			// contents of the retry queue are processed, since that may create the missing table.
			retryableError := NewRetryableError(
				fmt.Sprintf("%s: table does not exist: %s", message, postgresError.Message), err)
			retryableError.CausedByMissingTable = true

			return retryableError
		}
	}

	return w.handleDbError(message, dbStatus, err)
}

func (w *DbWriter) handleDbError(message string,
	dbStatus *dbConnectionStatus, err error) error {
	var networkError net.Error

	if errors.As(err, &networkError) {
		dbStatus.healthy = false
		return NewRetryableError(fmt.Sprintf("%s: network error", message), err)
	}

	return fmt.Errorf("%s: %w", message, err)
}

func columnTypeString(column *Column, allowNotNull bool) (string, error) {
	result := ""

	switch column.DataType {
	case DataTypeInt:
		result = "INT"
		break
	case DataTypeBigInt:
		result = "BIGINT"
		break
	case DataTypeDecimal:
		result = "FLOAT4"
		break
	case DataTypeBigDecimal:
		result = "FLOAT8"
		break
	case DataTypeTimestamp:
		result = "TIMESTAMP WITH TIME ZONE"
		break
	case DataTypeVarChar:
		if column.MaxLength > 0 {
			result = fmt.Sprintf("VARCHAR(%d)", column.MaxLength)
			break
		}

		result = "VARCHAR"
	default:
		return "", fmt.Errorf("unknown data type: %d", column.DataType)
	}

	if allowNotNull && !column.Nullable {
		result = fmt.Sprintf("%s NOT NULL", result)
	}

	return result, nil
}
