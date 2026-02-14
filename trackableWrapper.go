package dblog

import (
	"context"
	"fmt"
	"time"

	"github.com/avanha/pmaas-plugin-dblog/entities"
	"github.com/avanha/pmaas-spi/tracking"
)

type trackableWrapper struct {
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
}

func (w *trackableWrapper) toLoggedTrackableEntity() entities.LoggedTrackableEntity {
	return entities.LoggedTrackableEntity{
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
