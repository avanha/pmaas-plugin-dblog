package writer

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net"
	"reflect"
	"regexp"
	"testing"
	"testing/synctest"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/avanha/pmaas-plugin-dblog/config"
	"github.com/avanha/pmaas-plugin-dblog/internal/common"
	"github.com/lib/pq"
)

type TestData struct {
	Int32Value          int32
	StringValue         string
	OnChangeStringValue string
}

const insertStatement = "INSERT INTO mockTable"

// The whitespace here is important
const createTableStatement = `CREATE TABLE mocktable (
        	id BIGSERIAL PRIMARY KEY,
        	int_value INT NOT NULL,
        	string_value VARCHAR(255),
        	on_change_string_value VARCHAR(255)
        );`

const selectTableCountStatement = `SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'mocktable'`

var columns = []Column{
	{
		Name:      "int_value",
		DataType:  DataTypeInt,
		MaxLength: 0,
		Nullable:  false,
		Field: reflect.StructField{
			Name: "IntValue",
		},
	},
	{
		Name:      "string_value",
		DataType:  DataTypeVarChar,
		MaxLength: 255,
		Nullable:  true,
		Field: reflect.StructField{
			Name: "StringValue",
		},
	},
	{
		Name:      "on_change_string_value",
		DataType:  DataTypeVarChar,
		MaxLength: 255,
		Nullable:  true,
		Field: reflect.StructField{
			Name: "OnChangeStringValue",
		},
	},
}

var testData = TestData{Int32Value: 10, StringValue: "test", OnChangeStringValue: "onChangeValue"}
var insertArgs = []driver.Value{10, "test", "onChangeValue"}

var testData2 = TestData{Int32Value: 20, StringValue: "test2", OnChangeStringValue: "onChangeValue"}
var insertArgs2 = []driver.Value{20, "test2", "onChangeValue"}

var testData3 = TestData{Int32Value: 30, StringValue: "test3", OnChangeStringValue: "onChangeValue"}
var insertArgs3 = []driver.Value{30, "test3", "onChangeValue"}

var missingTableError = &pq.Error{Code: "42P01", Message: "relation \"mockTable\" does not exist"}

type dbWriterTestState struct {
	config         *config.PluginConfig
	tableContext   *TableContext
	db             *sql.DB
	dbMock         sqlmock.Sqlmock
	requestCh      chan Request
	dbWriter       *DbWriter
	dbWriterDoneCh chan bool
	retryQueueSize int
	successCount   int
	failureCount   int
	lastError      error
}

func (state *dbWriterTestState) ReportSuccess(retryQueueSize int) {
	state.successCount++
	state.retryQueueSize = retryQueueSize
}

func (state *dbWriterTestState) ReportFailure(cause error, retryQueueSize int) {
	state.failureCount++
	state.retryQueueSize = retryQueueSize
	state.lastError = cause
}

func (state *dbWriterTestState) UpdateRetryQueueSize(count int) {
	state.retryQueueSize = count
}

func setup(t *testing.T) *dbWriterTestState {
	fmt.Printf(">>> Running %s\n", t.Name())
	state := &dbWriterTestState{
		config: &config.PluginConfig{
			DriverName:     "mockDriverName",
			DataSourceName: "mockDataSourceName",
		},
		tableContext: &TableContext{
			tableName: "mockTable",
			columns: []Column{
				{Name: "int_value", DataType: DataTypeInt, Nullable: false, Mode: common.FieldModeAlways},
				{Name: "string_value", DataType: DataTypeVarChar, Nullable: true, Mode: common.FieldModeAlways},
				{Name: "on_change_string_value", DataType: DataTypeVarChar, Nullable: true, Mode: common.FieldModeOnChange},
			},
			insertStatement:    insertStatement,
			fields:             ReflectFields(t, &testData, "Int32Value", "StringValue", "OnChangeStringValue"),
			hasOnChangeColumns: true,
		},
	}

	db, mock, err := sqlmock.New()

	if err != nil {
		t.Fatalf("failed to initialize db mock: %s", err)
	}

	state.db = db
	state.dbMock = mock

	connectionFactoryFn := func() (*sql.DB, error) {
		return db, nil
	}

	state.requestCh = make(chan Request)
	state.dbWriter = CreateDbWriter(context.Background(), *state.config, connectionFactoryFn, state.requestCh, state)

	initErrorCh := make(chan error)
	state.dbWriterDoneCh = make(chan bool)

	go func() {
		err := state.dbWriter.Init()

		if err != nil {
			initErrorCh <- err
		}

		close(initErrorCh)

		state.dbWriter.Run()
		close(state.dbWriterDoneCh)
	}()

	err = <-initErrorCh

	if err != nil {
		t.Fatalf("failed to initialize dbWriter: %s", err)
	}

	return state
}

func (state *dbWriterTestState) expectFullDbClose() {
	// We expect two close calls because the sql infrastructure opens two connections
	state.dbMock.ExpectClose()
	state.dbMock.ExpectClose()
}

func (state *dbWriterTestState) stopDbWriter() {
	close(state.requestCh)
	_ = <-state.dbWriterDoneCh
}

func (state *dbWriterTestState) createTableRequest() Request {
	responseCh := make(chan Response)
	return Request{
		ResponseCh: responseCh,
		TableOperation: TableOperation{
			Operation: TableOperationCreateIfNotExists,
			Name:      "mockTable",
			Columns:   columns,
		},
	}
}

func (state *dbWriterTestState) createDataInsertRequest(data TestData) Request {
	responseCh := make(chan Response)

	return Request{
		ResponseCh: responseCh,
		InsertDataOperation: InsertDataOperation{
			Data:    data,
			Context: state.tableContext,
		},
	}
}

func (state *dbWriterTestState) nullifyOnChangeArg(args []driver.Value) []driver.Value {
	argsCopy := make([]driver.Value, len(args))

	for i := 0; i < len(args)-1; i++ {
		argsCopy[i] = args[i]
	}

	argsCopy[len(args)-1] = sql.NullString{String: "", Valid: false}

	return argsCopy
}

func TestDbWriter_dataInsertRequest_succeeds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		state := setup(t)

		state.dbMock.ExpectBegin()
		state.dbMock.ExpectPrepare(insertStatement)
		state.dbMock.ExpectExec(insertStatement).WithArgs(insertArgs...).WillReturnResult(sqlmock.NewResult(1, 1))
		state.dbMock.ExpectCommit()
		state.expectFullDbClose()

		responseCh := make(chan Response)
		state.requestCh <- Request{
			ResponseCh: responseCh,
			InsertDataOperation: InsertDataOperation{
				Data:    testData,
				Context: state.tableContext,
			},
		}

		response := <-responseCh

		state.stopDbWriter()

		if response.Error != nil {
			t.Fatalf("failed to insert data: %v", response.Error)
		}
	})
}

func TestDbWriter_dataInsertRequest_repeatedArg_convertedToNull(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		state := setup(t)

		state.dbMock.ExpectBegin()
		state.dbMock.ExpectPrepare(insertStatement)
		state.dbMock.ExpectExec(insertStatement).WithArgs(insertArgs...).WillReturnResult(sqlmock.NewResult(1, 1))
		state.dbMock.ExpectCommit()

		state.dbMock.ExpectBegin()
		state.dbMock.ExpectPrepare(insertStatement)
		state.dbMock.ExpectExec(insertStatement).WithArgs(state.nullifyOnChangeArg(insertArgs2)...).WillReturnResult(sqlmock.NewResult(2, 1))
		state.dbMock.ExpectCommit()

		state.dbMock.ExpectBegin()
		state.dbMock.ExpectPrepare(insertStatement)
		state.dbMock.ExpectExec(insertStatement).WithArgs(state.nullifyOnChangeArg(insertArgs3)...).WillReturnResult(sqlmock.NewResult(3, 1))
		state.dbMock.ExpectCommit()

		state.expectFullDbClose()

		request1 := state.createDataInsertRequest(testData)
		state.requestCh <- request1
		response1 := <-request1.ResponseCh

		request2 := state.createDataInsertRequest(testData2)
		state.requestCh <- request2
		response2 := <-request2.ResponseCh

		request3 := state.createDataInsertRequest(testData3)
		state.requestCh <- request3
		response3 := <-request3.ResponseCh

		state.stopDbWriter()

		if response1.Error != nil {
			t.Fatalf("failed to insert data: %v", response1.Error)
		}

		if response2.Error != nil {
			t.Fatalf("failed to insert data: %v", response2.Error)
		}

		if response3.Error != nil {
			t.Fatalf("failed to insert data: %v", response3.Error)
		}
	})
}

func TestDbWriter_dataInsertRequest_failsWithRetryableError_EnqueuedForRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		state := setup(t)
		state.dbMock.ExpectBegin().WillReturnError(net.UnknownNetworkError("mock network error"))
		state.expectFullDbClose()

		responseCh := make(chan Response)
		state.requestCh <- Request{
			ResponseCh: responseCh,
			InsertDataOperation: InsertDataOperation{
				Data:    testData,
				Context: state.tableContext,
			},
		}

		response := <-responseCh

		state.stopDbWriter()

		if response.Error != nil {
			t.Fatalf("failed to insert data: %v", response.Error)
		}

		if !response.QueuedForRetry {
			t.Fatalf("expected request to be marked as queued for retry")
		}

		if len(state.dbWriter.retryQueue) != 1 {
			t.Fatalf("expected 1 item in retry queue, got %d", len(state.dbWriter.retryQueue))
		}
	})
}

func TestDbWriter_dataInsertRequest_failsWithRetryableError_SucceedsOnTimerRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		state := setup(t)
		// Attempt 1
		state.dbMock.ExpectBegin().WillReturnError(net.UnknownNetworkError("network error"))
		// Attempt 2
		state.dbMock.ExpectBegin()
		state.dbMock.ExpectPrepare(insertStatement)
		state.dbMock.ExpectExec(insertStatement).WithArgs(insertArgs...).WillReturnResult(sqlmock.NewResult(1, 1))
		state.dbMock.ExpectCommit()
		state.expectFullDbClose()

		request := state.createDataInsertRequest(testData)
		state.requestCh <- request
		response := <-request.ResponseCh

		// Sleep for 65 seconds, which is enough time for a retry.  Since we're in the async bubble,
		// this will happen instantly.
		time.Sleep(65 * time.Second)

		state.stopDbWriter()

		if response.Error != nil {
			t.Fatalf("failed to insert data: %v", response.Error)
		}

		if !response.QueuedForRetry {
			t.Fatalf("expected request to be marked as queued for retry")
		}

		if len(state.dbWriter.retryQueue) != 0 {
			t.Fatalf("expected 0 items in retry queue, got %d", len(state.dbWriter.retryQueue))
		}
	})
}

func TestDbWriter_dataInsertRequest_failsWithMissingTableErrorThenNetworkError_RetriesAndInserts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		state := setup(t)

		// Attempt 1
		state.dbMock.ExpectBegin()
		state.dbMock.ExpectPrepare(insertStatement).WillReturnError(missingTableError)
		state.dbMock.ExpectRollback()

		// Attempt 2
		state.dbMock.ExpectBegin().WillReturnError(net.UnknownNetworkError("network error"))

		// Attempt 3
		state.dbMock.ExpectBegin()
		state.dbMock.ExpectPrepare(insertStatement)
		state.dbMock.ExpectExec(insertStatement).WithArgs(insertArgs...).WillReturnResult(sqlmock.NewResult(1, 1))
		state.dbMock.ExpectCommit()

		state.expectFullDbClose()

		request := state.createDataInsertRequest(testData)
		state.requestCh <- request
		response := <-request.ResponseCh

		if response.Error != nil {
			t.Fatalf("failed to insert data: %v", response.Error)
		}

		if !response.QueuedForRetry {
			t.Fatalf("expected request to be marked as queued for retry")
		}

		if len(state.dbWriter.retryQueue) != 1 {
			t.Fatalf("expected 1 item in retry queue, got %d", len(state.dbWriter.retryQueue))
		}

		// Sleep for 65 seconds, which is enough time for a retry.  Since we're in the async bubble,
		// this will happen instantly.
		time.Sleep(65 * time.Second)

		// Attempt 2 should have failed as well

		if len(state.dbWriter.retryQueue) != 1 {
			t.Fatalf("expected 1 item in retry queue, got %d", len(state.dbWriter.retryQueue))
		}

		// Sleep again for the next retry attempt
		time.Sleep(65 * time.Second)

		state.stopDbWriter()

		if len(state.dbWriter.retryQueue) != 0 {
			t.Fatalf("expected 0 items in retry queue, got %d", len(state.dbWriter.retryQueue))
		}
	})
}

func TestDbWriter_createTableFailsWithRetriableError_dataInsertFailsWithMissingTable_bothRetriedAndSucceed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		state := setup(t)

		// Table Create
		state.dbMock.
			ExpectQuery(regexp.QuoteMeta(selectTableCountStatement)).WillReturnError(net.UnknownNetworkError("mock network error"))

		// Data Insert
		state.dbMock.ExpectBegin()
		state.dbMock.ExpectPrepare(insertStatement).WillReturnError(missingTableError)
		state.dbMock.ExpectRollback()

		// Table Create Retry
		state.dbMock.
			ExpectQuery(regexp.QuoteMeta(selectTableCountStatement)).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		state.dbMock.
			ExpectExec(regexp.QuoteMeta(createTableStatement)).
			WillReturnResult(sqlmock.NewResult(1, 1))

		// Data Insert Retry
		state.dbMock.ExpectBegin()
		state.dbMock.ExpectPrepare(insertStatement)
		state.dbMock.ExpectExec(insertStatement).WithArgs(insertArgs...).WillReturnResult(sqlmock.NewResult(1, 1))
		state.dbMock.ExpectCommit()

		state.expectFullDbClose()

		// Create table attempt fails
		request := state.createTableRequest()
		state.requestCh <- request
		response := <-request.ResponseCh

		if response.Error != nil {
			t.Fatalf("table create request failed: %v", response.Error)
		}

		if !response.QueuedForRetry {
			t.Fatalf("expected request to be marked as queued for retry")
		}

		// Insert data attempt
		request = state.createDataInsertRequest(testData)
		state.requestCh <- request
		response = <-request.ResponseCh

		if response.Error != nil {
			t.Fatalf("table create request failed: %v", response.Error)
		}

		if !response.QueuedForRetry {
			t.Fatalf("expected request to be marked as queued for retry")
		}

		// Wait for the retry logic to complete
		synctest.Wait()

		if len(state.dbWriter.retryQueue) != 0 {
			t.Fatalf("expected 0 items in retry queue, got %d", len(state.dbWriter.retryQueue))
		}

		state.stopDbWriter()
	})
}

func TestDbWriter_dataInsertRequest_failsWithRetryableError_succeedsOnRetryAfterSuccessfulRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		state := setup(t)
		state.dbMock.ExpectBegin().WillReturnError(net.UnknownNetworkError("mock network error"))
		state.dbMock.ExpectBegin()
		state.dbMock.ExpectPrepare(insertStatement)
		state.dbMock.ExpectExec(insertStatement).WithArgs(insertArgs2...).WillReturnResult(sqlmock.NewResult(1, 1))
		state.dbMock.ExpectCommit()
		state.dbMock.ExpectBegin()
		state.dbMock.ExpectPrepare(insertStatement)
		state.dbMock.ExpectExec(insertStatement).WithArgs(insertArgs...).WillReturnResult(sqlmock.NewResult(1, 1))
		state.dbMock.ExpectCommit()
		state.expectFullDbClose()

		request1 := state.createDataInsertRequest(testData)
		state.requestCh <- request1
		response := <-request1.ResponseCh

		request2 := state.createDataInsertRequest(testData2)
		state.requestCh <- request2
		response2 := <-request2.ResponseCh

		state.stopDbWriter()

		if response.Error != nil {
			t.Fatalf("failed to insert data: %v", response.Error)
		}

		if !response.QueuedForRetry {
			t.Fatalf("expected request to be marked as queued for retry")
		}

		if response2.Error != nil {
			t.Fatalf("failed to insert data: %v", response2.Error)
		}

		if response2.QueuedForRetry {
			t.Fatalf("expected request2 not to be marked as queued for retry")
		}

		if len(state.dbWriter.retryQueue) != 0 {
			t.Fatalf("expected 0 items in retry queue, got %d", len(state.dbWriter.retryQueue))
		}
	})
}

func TestDbWriter_createTableRequestFailsWithRetryableError_succeedsOnTimerRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		state := setup(t)
		state.dbMock.
			ExpectQuery(regexp.QuoteMeta(selectTableCountStatement)).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		state.dbMock.
			ExpectExec(regexp.QuoteMeta(createTableStatement)).
			WillReturnError(net.UnknownNetworkError("mock network error"))
		state.dbMock.
			ExpectQuery(regexp.QuoteMeta(selectTableCountStatement)).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		state.dbMock.
			ExpectExec(regexp.QuoteMeta(createTableStatement)).
			WillReturnResult(sqlmock.NewResult(1, 1))
		state.expectFullDbClose()

		request := state.createTableRequest()
		state.requestCh <- request
		response := <-request.ResponseCh

		if response.Error != nil {
			t.Fatalf("failed to create table: %v", response.Error)
		}

		if !response.QueuedForRetry {
			t.Fatalf("expected request to be marked as queued for retry")
		}

		if response.Context == nil {
			t.Fatalf("expected non-nil TableContext, got nil")
		}

		if response.Context.tableName != "mocktable" || response.Context.insertStatement == "" {
			t.Fatalf("expected TableContext with tableName mocktable and non-empty insert statement, got %+v",
				response.Context)
		}

		// Sleep for 65 seconds, which is enough time for a retry.  Since we're in the async bubble,
		// this will happen instantly.
		time.Sleep(65 * time.Second)

		state.stopDbWriter()

		if response.Error != nil {
			t.Fatalf("failed to create table: %v", response.Error)
		}
	})
}

func ReflectFields(t *testing.T, data any, fieldNames ...string) []reflect.StructField {
	result := make([]reflect.StructField, len(fieldNames))

	structType := reflect.TypeOf(data).Elem()

	for i, fieldName := range fieldNames {
		structField, found := structType.FieldByName(fieldName)

		if !found {
			t.Fatalf("field %s not found in struct %s", fieldName, structType.Name())
		}

		result[i] = structField
	}

	return result
}
