package trackableWrapper

import (
	"context"
	"fmt"
	"time"

	"github.com/avanha/pmaas-plugin-dblog/data"
	"github.com/avanha/pmaas-plugin-dblog/entities"
	"github.com/avanha/pmaas-plugin-dblog/internal/poller"
	spi "github.com/avanha/pmaas-spi"
	spicommon "github.com/avanha/pmaas-spi/common"
	"github.com/avanha/pmaas-spi/tracking"
)

type TrackableWrapper struct {
	container               spi.IPMAASContainer
	trackable               tracking.Trackable
	pollTaskCancelFn        context.CancelFunc
	id                      string
	name                    string
	trackingConfig          tracking.Config
	fieldCount              int
	registrationTime        time.Time
	status                  string
	pollCount               int
	lastPollTime            time.Time
	successCount            int
	lastSuccessTime         time.Time
	softFailureCount        int
	lastSoftFailureTime     time.Time
	failureCount            int
	lastFailureTime         time.Time
	lastFailureErrorMessage string
	writeRequestHandlerFn   poller.WriteRequestHandlerFunc
	stub                    *Stub
}

func NewTrackableWrapper(
	container spi.IPMAASContainer,
	writeRequestHandlerFn poller.WriteRequestHandlerFunc,
	trackable tracking.Trackable,
	trackingConfig tracking.Config,
	entityId string,
	name string,
) *TrackableWrapper {
	return &TrackableWrapper{
		container:             container,
		trackable:             trackable,
		trackingConfig:        trackingConfig,
		writeRequestHandlerFn: writeRequestHandlerFn,
		id:                    entityId,
		name:                  name,
	}
}

// Poll runs a polling task to track and update the state of the associated TrackableWrapper entity.
// This function continues to run until the given parent context is canceled, or this specific poller
// is canceled via a call to CancelPollTask.  As this is a long-running task, the recommended use is to
// call this from a new goroutine.
func (w *TrackableWrapper) Poll() {
	resultCh := make(chan *poller.Task)
	err := w.container.EnqueueOnPluginGoRoutine(func() {
		resultCh <- w.initPoll()
		close(resultCh)
	})

	if err != nil {
		fmt.Printf("TrackableWrapper[%s]: Unable to enqueue initPoll on plugin goroutine: %v\n",
			w.name, err)
		return
	}

	task := <-resultCh

	if task == nil {
		return
	}

	task.Run()
}

func (w *TrackableWrapper) initPoll() *poller.Task {
	if w.pollTaskCancelFn != nil {
		// A poll task it already running
		return nil
	}

	statsTrackerAdapter := PollerStatsTrackerAdapter{
		container:     w.container,
		entityWrapper: w,
	}
	taskCtx, pollTaskCancelFn := context.WithCancel(context.Background())
	w.pollTaskCancelFn = pollTaskCancelFn

	return poller.NewTask(taskCtx, w.trackable, w.trackingConfig, w.writeRequestHandlerFn, statsTrackerAdapter)
}

func (w *TrackableWrapper) CancelPollTask() {
	if w.pollTaskCancelFn == nil {
		fmt.Printf("TrackableWrapper[%s]: No poll task is running\n", w.name)
		return
	}

	if w.pollTaskCancelFn != nil {
		w.pollTaskCancelFn()
		w.pollTaskCancelFn = nil
	}
}

// GetStub returns a proxy struct that implements the entities.TrackableEntityWrapper interface.  The function ensures
// that only one stub is created for the entity.  This function is not thread-safe, but that is because
// it's only called from the plugin goroutine, whether directly or via the PMAAS server.
func (t *TrackableWrapper) GetStub() entities.TrackableEntityWrapper {
	if t.stub == nil {
		t.stub = NewStub(
			t.name,
			&spicommon.ThreadSafeEntityWrapper[entities.TrackableEntityWrapper]{
				Container: t.container,
				Entity:    t,
			})
	}

	return t.stub
}

func (t *TrackableWrapper) CloseStubIfPresent() {
	if t.stub != nil {
		t.stub.Close()
		t.stub = nil
	}
}

func (w *TrackableWrapper) ToLoggedTrackableEntity() data.LoggedTrackableEntity {
	return data.LoggedTrackableEntity{
		Id:                  w.id,
		Name:                w.name,
		Status:              w.status,
		TrackingMode:        getTrackingModeDescription(w.trackingConfig.TrackingMode),
		PollIntervalSeconds: w.trackingConfig.PollIntervalSeconds,
		DataSource: fmt.Sprintf("%s/%s", w.trackingConfig.Schema.DataStructType.PkgPath(),
			w.trackingConfig.Schema.DataStructType.Name()),
		FieldCount:              w.fieldCount,
		HasArgFactoryFunction:   w.trackingConfig.Schema.InsertArgFactoryFn != nil,
		RegistrationTime:        w.registrationTime,
		PollCount:               w.pollCount,
		LastPollTime:            w.lastPollTime,
		SuccessCount:            w.successCount,
		LastSuccessTime:         w.lastSuccessTime,
		SoftFailureCount:        w.softFailureCount,
		LastSoftFailureTime:     w.lastSoftFailureTime,
		FailureCount:            w.failureCount,
		LastFailureTime:         w.lastFailureTime,
		LastFailureErrorMessage: w.lastFailureErrorMessage,
	}
}

func (w *TrackableWrapper) GetHistory() <-chan any {
	result := make(chan any)
	close(result)

	return result
}

func getTrackingModeDescription(trackingMode int) string {
	switch trackingMode {
	case tracking.ModePoll:
		return "Polling"
	case tracking.ModePush:
		return "Push"
	default:
		return "Unknown"
	}
}
