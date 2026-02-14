package dblog

import (
	"fmt"
	"time"
)

type pollerStatsTrackerAdapter struct {
	parent        *plugin
	entityWrapper *trackableWrapper
}

func (p pollerStatsTrackerAdapter) ReportPollResult(pollTime time.Time, dataInsertAttempted bool, dataInsertQueuedForRetry bool, dataInsertErrorMessage string) {
	err := p.parent.container.EnqueueOnPluginGoRoutine(func() {
		p.entityWrapper.pollCount++
		p.entityWrapper.lastPollTime = pollTime

		if dataInsertAttempted {
			if dataInsertErrorMessage == "" {
				if dataInsertQueuedForRetry {
					p.entityWrapper.softFailureCount++
					p.entityWrapper.lastSoftFailureTime = pollTime
				} else {
					p.entityWrapper.successCount++
					p.entityWrapper.lastSuccessTime = pollTime
				}
			} else {
				p.entityWrapper.failureCount++
				p.entityWrapper.lastFailureTime = pollTime
				p.entityWrapper.lastFailureErrorMessage = dataInsertErrorMessage
			}
		}
	})

	if err != nil {
		fmt.Printf("dblog.pollerStatsTrackerAdapter: Unable to report poll result: %v\n", err)
	}

}

func (p pollerStatsTrackerAdapter) ReportFieldCount(fieldCount int) {
	err := p.parent.container.EnqueueOnPluginGoRoutine(func() {
		p.entityWrapper.fieldCount = fieldCount
	})

	if err != nil {
		fmt.Printf("dblog.pollerStatsTrackerAdapter: Unable to report field count update: %v\n", err)
	}
}

func (p pollerStatsTrackerAdapter) ReportStatus(statusDescription string) {
	err := p.parent.container.EnqueueOnPluginGoRoutine(func() {
		p.entityWrapper.status = statusDescription
	})

	if err != nil {
		fmt.Printf("dblog.pollerStatsTrackerAdapter: Unable to report status update: %v\n", err)
	}
}

func (p pollerStatsTrackerAdapter) ReportStatusWithError(statusDescription string, errorMessage string) {
	err := p.parent.container.EnqueueOnPluginGoRoutine(func() {
		p.entityWrapper.status = statusDescription
		p.entityWrapper.failureCount++
		p.entityWrapper.lastFailureTime = time.Now()
		p.entityWrapper.lastFailureErrorMessage = errorMessage
	})

	if err != nil {
		fmt.Printf("dblog.pollerStatsTrackerAdapter: Unable to report status update: %v\n", err)
	}
}
