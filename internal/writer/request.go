package writer

import "github.com/avanha/pmaas-spi/tracking"

type Request struct {
	TableOperation
	InsertDataOperation
	ResponseCh           chan Response
	attempt              int
	missingTableFailures int
}

func (r *Request) Describe() string {
	if r.TableOperation.Name != "" {
		return "TableOperation"
	}

	if r.InsertDataOperation.Data != nil {
		return "InsertDataOperation"
	}

	return "UnknownOperation"
}

type Response struct {
	Error          error
	Context        *TableContext
	QueuedForRetry bool
}

const TableOperationCreateIfNotExists = 1

type TableOperation struct {
	Operation int
	Name      string
	Columns   []Column
}

type InsertDataOperation struct {
	Context            *TableContext
	Data               any
	InsertArgFactoryFn tracking.InsertArgFactoryFunc
}
