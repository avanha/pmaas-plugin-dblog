package poller

import (
	"context"
	"reflect"
	"testing"

	"pmaas.io/plugins/dblog/internal/writer"
	"pmaas.io/spi/tracking"
)

type TestData struct {
	Value int32
}

type TestTrackable struct {
}

func (t TestTrackable) TrackingConfig() tracking.Config {
	return tracking.Config{}
}

func (t TestTrackable) Data() tracking.DataSample {
	return tracking.DataSample{
		Data: TestData{
			Value: 50,
		},
	}
}

func TestTask_getColumnType_succeeds(t *testing.T) {
	data := TestData{}
	writeRequestHandlerFun := func(request writer.Request) error {
		return nil
	}
	task := NewTask(context.Background(), TestTrackable{}, tracking.Config{}, writeRequestHandlerFun)
	dataType, err := task.getColumnType(reflect.TypeOf(data.Value), &fieldOptions{})

	if err != nil {
		t.Fatalf("getColumnType failed: %s", err)
	}

	expectedDataType := writer.DataTypeInt

	if dataType != expectedDataType {
		t.Fatalf("expected data type %d, got %d", expectedDataType, dataType)
	}

}
