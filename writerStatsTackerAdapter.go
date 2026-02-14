package dblog

import "fmt"

type writerStatsTrackerAdapter struct {
	parent *plugin
}

func (w *writerStatsTrackerAdapter) ReportSuccess(retryQueueSize int) {
	err := w.parent.container.EnqueueOnPluginGoRoutine(func() { w.parent.onDbWriterSuccess(retryQueueSize) })

	if err != nil {
		fmt.Printf("dblog.writerStatsTrackerAdapter: Unable to report success: %v\n", err)
	}
}

func (w *writerStatsTrackerAdapter) ReportFailure(cause error, retryQueueSize int) {
	err := w.parent.container.EnqueueOnPluginGoRoutine(func() {
		w.parent.onDbWriterFailure(cause, retryQueueSize)
	})

	if err != nil {
		fmt.Printf("dblog.writerStatsTrackerAdapter: Unable to report failure: %v\n", err)
	}
}

func (w *writerStatsTrackerAdapter) UpdateRetryQueueSize(retryQueueSize int) {
	err := w.parent.container.EnqueueOnPluginGoRoutine(func() {
		w.parent.onDbWriterRetryQueueSizeChange(retryQueueSize)
	})

	if err != nil {
		fmt.Printf("dblog.writerStatsTrackerAdapter: Unable to report retry queue size update: %v\n", err)
	}
}
