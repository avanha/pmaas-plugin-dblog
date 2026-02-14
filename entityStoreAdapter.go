package dblog

import (
	"fmt"

	"github.com/avanha/pmaas-plugin-dblog/internal/common"
)

type entityStoreAdapter struct {
	parent *plugin
}

func (e entityStoreAdapter) GetStatusAndEntities() (common.StatusAndEntities, error) {
	// HTTP requests come in on arbitrary Go routines, so execute getEntities on the main plugin Go routine to get all
	// states atomically.
	resultCh := make(chan common.StatusAndEntities)
	err := e.parent.container.EnqueueOnPluginGoRoutine(func() {
		resultCh <- e.parent.getStatusAndEntities()
		close(resultCh)
	})

	if err != nil {
		close(resultCh)

		return common.StatusAndEntities{}, fmt.Errorf("unable to get status and entities: %w", err)
	}

	result := <-resultCh

	return result, nil
}
