package poller

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/avanha/pmaas-plugin-dblog/internal/common"
	"github.com/avanha/pmaas-plugin-dblog/internal/writer"
	"github.com/avanha/pmaas-spi/tracking"
)

type WriteRequestHandlerFunc func(request writer.Request) error
type Task struct {
	ctx                   context.Context
	target                tracking.Trackable
	trackingConfig        tracking.Config
	initialDelaySeconds   int
	intervalSeconds       int
	targetName            string
	writeRequestHandlerFn WriteRequestHandlerFunc
	writerContext         *writer.TableContext
	statsTracker          common.PollerStatsTracker
}

func NewTask(ctx context.Context, target tracking.Trackable, trackingConfig tracking.Config,
	writeRequestHandlerFun WriteRequestHandlerFunc,
	tracker common.PollerStatsTracker) Task {
	return Task{
		ctx:                   ctx,
		target:                target,
		initialDelaySeconds:   30,
		trackingConfig:        trackingConfig,
		intervalSeconds:       trackingConfig.PollIntervalSeconds,
		targetName:            target.TrackingConfig().Name,
		writeRequestHandlerFn: writeRequestHandlerFun,
		statsTracker:          tracker,
	}
}

func (t *Task) Run() {
	t.statsTracker.ReportStatus("Initial delay")
	run := t.wait(time.Duration(t.initialDelaySeconds) * time.Second)

	if run {
		t.statsTracker.ReportStatus("Initializing storage")
		err := t.ensureStorage()
		if err == nil {
			t.statsTracker.ReportStatus("Running")
		} else {
			message := fmt.Sprintf("poller [%s]: Unable to ensure storage, stopping: %v", t.targetName, err)
			t.statsTracker.ReportStatusWithError("Failed", message)
			fmt.Printf("%s\n", message)
			run = false
		}
	}

	if run {
		ticker := time.NewTicker(time.Duration(t.intervalSeconds) * time.Second)
		defer ticker.Stop()

		for run {
			t.poll()
			run = t.waitForTick(ticker)
		}
	}

	fmt.Printf("poller [%s]: Terminated\n", t.targetName)
	t.statsTracker.ReportStatus("Terminated")
}

func (t *Task) waitForTick(ticker *time.Ticker) bool {
	for {
		select {
		case <-t.ctx.Done():
			return false
		case <-ticker.C:
			return true
		}
	}
}

func (t *Task) ensureStorage() error {
	tableName := t.trackingConfig.Name
	columns, err := t.calculateColumns()

	if err != nil {
		return fmt.Errorf("unable to calculate columns for %s: %w", t.trackingConfig.Name, err)
	}

	columnCount := len(columns)

	if columnCount == 0 {
		return fmt.Errorf("unable to obtain any columns for %s", t.trackingConfig.Name)
	}

	t.statsTracker.ReportFieldCount(columnCount)

	responseCh := make(chan writer.Response, 1)
	err = t.writeRequestHandlerFn(
		writer.Request{
			TableOperation: writer.TableOperation{
				Operation: writer.TableOperationCreateIfNotExists,
				Name:      tableName,
				Columns:   columns,
			},
			ResponseCh: responseCh,
		})

	if err != nil {
		close(responseCh)
		return fmt.Errorf("unable to execute table %s creation request: %w", tableName, err)
	}

	response := <-responseCh

	if response.Error != nil {
		return response.Error
	}

	t.writerContext = response.Context

	return nil
}

func (t *Task) calculateColumns() ([]writer.Column, error) {
	columns := make([]writer.Column, 0, 10)
	dataStructType := t.trackingConfig.Schema.DataStructType

	for i := 0; i < dataStructType.NumField(); i++ {
		field := dataStructType.Field(i)
		fieldTag := field.Tag
		tagValue, ok := fieldTag.Lookup("track")

		if !ok {
			// We only care about tagged fields
			continue
		}

		fieldOptions, err := parseFieldTag(tagValue)

		if err != nil {
			return nil, fmt.Errorf("unable to parse %s field tag: %w", field.Name, err)
		}

		columnName := t.getColumnName(field.Name, &fieldOptions)
		fieldType, err := t.getColumnType(field.Type, &fieldOptions)

		if err != nil {
			return nil, fmt.Errorf("unable to obtain data type for field %s", field.Name)
		}

		columns = append(columns, writer.Column{
			Name:      columnName,
			DataType:  fieldType,
			Nullable:  fieldOptions.Nullable,
			Mode:      fieldOptions.Mode,
			MaxLength: fieldOptions.MaxLength,
		})
	}

	return columns, nil
}

func (t *Task) getColumnName(fieldName string, fieldOptions *fieldOptions) string {
	if fieldOptions.Name != "" {
		return fieldOptions.Name
	}

	return fieldName
}

var StringType = reflect.TypeOf((*string)(nil)).Elem()
var Int32Type = reflect.TypeOf((*int32)(nil)).Elem()
var Int64Type = reflect.TypeOf((*int64)(nil)).Elem()
var Uint32Type = reflect.TypeOf((*uint32)(nil)).Elem()
var Uint64Type = reflect.TypeOf((*uint64)(nil)).Elem()
var Float32Type = reflect.TypeOf((*float32)(nil)).Elem()
var Float64Type = reflect.TypeOf((*float64)(nil)).Elem()
var TimeType = reflect.TypeOf((*time.Time)(nil)).Elem()

func (t *Task) getColumnType(fieldType reflect.Type, fieldOptions *fieldOptions) (int, error) {
	if fieldOptions.DataType != 0 {
		return fieldOptions.DataType, nil
	}

	switch fieldType {
	case StringType:
		return writer.DataTypeVarChar, nil
	case Int32Type:
		return writer.DataTypeInt, nil
	case Uint32Type:
		return writer.DataTypeInt, nil
	case Int64Type:
		return writer.DataTypeBigInt, nil
	case Uint64Type:
		return writer.DataTypeBigInt, nil
	case Float32Type:
		return writer.DataTypeDecimal, nil
	case Float64Type:
		return writer.DataTypeBigDecimal, nil
	case TimeType:
		return writer.DataTypeTimestamp, nil
	}

	return 0, fmt.Errorf("unsupported data type: %v", fieldType)
}

func (t *Task) poll() {
	fmt.Printf("poller [%s]: Polling\n", t.targetName)
	now := time.Now()
	dataSample := t.target.Data()
	dataInsertAttempted := false
	dataInsertQueuedForRetry := false
	dataInsertErrorMessage := ""

	if dataSample.LastUpdateTime.IsZero() {
		fmt.Printf("poller [%s]: Received data with a zero update time\n", t.targetName)
	} else {
		fmt.Printf("poller [%s]: Received: %+v\n", t.targetName, dataSample.Data)
		dataInsertAttempted = true
		responseCh := make(chan writer.Response, 1)
		err := t.writeRequestHandlerFn(
			writer.Request{
				InsertDataOperation: writer.InsertDataOperation{
					Context:            t.writerContext,
					Data:               dataSample.Data,
					InsertArgFactoryFn: t.trackingConfig.Schema.InsertArgFactoryFn,
				},
				ResponseCh: responseCh,
			})

		if err != nil {
			close(responseCh)
			dataInsertErrorMessage = fmt.Sprintf("Error scheduling data insert request: %v", err)
			t.statsTracker.ReportPollResult(now, dataInsertAttempted, false, dataInsertErrorMessage)
			fmt.Printf("poller [%s]: %s\n", t.targetName, dataInsertErrorMessage)

			return
		}

		response := <-responseCh

		if response.Error != nil {
			dataInsertErrorMessage = fmt.Sprintf("Error inserting data: %v", err)
			fmt.Printf("poller [%s]: Error inserting data: %v\n", t.targetName, response.Error)
		} else {
			fmt.Printf("poller [%s]: Data inserted\n", t.targetName)
			dataInsertQueuedForRetry = response.QueuedForRetry
		}
	}

	t.statsTracker.ReportPollResult(now, dataInsertAttempted, dataInsertQueuedForRetry, dataInsertErrorMessage)
}

func (t *Task) wait(duration time.Duration) bool {
	timer := time.NewTimer(duration)

	for {
		select {
		case <-t.ctx.Done():
			timer.Stop()
			return false

		case <-timer.C:
			return true
		}
	}
}
