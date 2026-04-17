package trackableWrapper

import (
	"fmt"
	"sync/atomic"

	"github.com/avanha/pmaas-plugin-dblog/entities"
	spicommon "github.com/avanha/pmaas-spi/common"
)

type Stub struct {
	name                   string
	closeFn                func() error
	entityWrapperReference atomic.Pointer[spicommon.ThreadSafeEntityWrapper[entities.TrackableEntityWrapper]]
}

func NewStub(name string, entityWrapper *spicommon.ThreadSafeEntityWrapper[entities.TrackableEntityWrapper]) *Stub {
	stub := &Stub{
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
		fmt.Printf("Failed to close Stub %s: %v", s.name, err)
	}
}

func (s *Stub) GetHistory() <-chan any {
	return spicommon.ThreadSafeEntityWrapperExecValueFunc(
		s.entityWrapperReference.Load(),
		func(target entities.TrackableEntityWrapper) <-chan any { return target.GetHistory() })
}
