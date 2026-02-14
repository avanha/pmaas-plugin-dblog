package common

import "time"

type PollerStatsTracker interface {
	ReportFieldCount(fieldCount int)
	ReportPollResult(pollTime time.Time, dataInsertAttempted bool, dataInsertQueuedForRetry bool, dataInsertErrorMessage string)
	ReportStatus(statusDescription string)
	ReportStatusWithError(statusDescription string, errorMessage string)
}
