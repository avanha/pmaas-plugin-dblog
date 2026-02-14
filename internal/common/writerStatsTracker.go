package common

type WriterStatsTracker interface {
	ReportSuccess(retryQueueSize int)
	ReportFailure(cause error, retryQueueSize int)
	UpdateRetryQueueSize(count int)
}
