package dblog

type writerStatsTrackerAdapter struct {
	parent *plugin
}

func (w *writerStatsTrackerAdapter) ReportSuccess(retryQueueSize int) {
	err := w.parent.container.EnqueueOnPluginGoRoutine(func() { w.parent.onDbWriterSuccess(retryQueueSize) })

	if err != nil {
		w.parent.log.Warn("unable to report success", "error", err)
	}
}

func (w *writerStatsTrackerAdapter) ReportFailure(cause error, retryQueueSize int) {
	err := w.parent.container.EnqueueOnPluginGoRoutine(func() {
		w.parent.onDbWriterFailure(cause, retryQueueSize)
	})

	if err != nil {
		w.parent.log.Warn("unable to report failure", "error", err)
	}
}

func (w *writerStatsTrackerAdapter) UpdateRetryQueueSize(retryQueueSize int) {
	err := w.parent.container.EnqueueOnPluginGoRoutine(func() {
		w.parent.onDbWriterRetryQueueSizeChange(retryQueueSize)
	})

	if err != nil {
		w.parent.log.Warn("unable to report retry queue size update", "error", err)
	}
}
