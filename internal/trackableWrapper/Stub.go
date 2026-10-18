package trackableWrapper

import (
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/avanha/pmaas-plugin-dblog/entities"
	spicommon "github.com/avanha/pmaas-spi/common"
	"github.com/avanha/pmaas-spi/tracking"
)

type Stub struct {
	log                    *slog.Logger
	name                   string
	closeFn                func() error
	entityWrapperReference atomic.Pointer[spicommon.ThreadSafeEntityWrapper[entities.TrackableEntityWrapper]]
}

func NewStub(log *slog.Logger, name string, entityWrapper *spicommon.ThreadSafeEntityWrapper[entities.TrackableEntityWrapper]) *Stub {
	stub := &Stub{
		log:  log,
		name: name,
	}

	stub.entityWrapperReference.Store(entityWrapper)

	stub.closeFn = func() error {
		if stub.entityWrapperReference.CompareAndSwap(entityWrapper, nil) {
			stub.closeFn = nil
			return nil
		}

		return fmt.Errorf("failed to clear entity wrapper, current value does not match expected value")
	}

	return stub
}

func (s *Stub) Close() {
	closeFn := s.closeFn

	if closeFn == nil {
		return
	}

	err := closeFn()

	if err != nil {
		s.log.Error("failed to close stub", "name", s.name, "error", err)
	}
}

func (s *Stub) GetHistory() tracking.ResultOrError[<-chan tracking.DataSample] {
	return spicommon.ThreadSafeEntityWrapperExecValueFunc(
		s.entityWrapperReference.Load(),
		func(target entities.TrackableEntityWrapper) tracking.ResultOrError[<-chan tracking.DataSample] {
			return target.GetHistory()
		})
}

func (s *Stub) GetMostRecentSample() tracking.ResultOrError[tracking.DataSample] {
	return spicommon.ThreadSafeEntityWrapperExecValueFunc(
		s.entityWrapperReference.Load(),
		func(target entities.TrackableEntityWrapper) tracking.ResultOrError[tracking.DataSample] {
			return target.GetMostRecentSample()
		})
}
