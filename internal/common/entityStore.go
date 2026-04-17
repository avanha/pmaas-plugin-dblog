package common

import (
	"github.com/avanha/pmaas-plugin-dblog/data"
)

type StatusAndEntities struct {
	Status   data.PluginStatus
	Entities []data.LoggedTrackableEntity
}

type EntityStore interface {
	GetStatusAndEntities() (StatusAndEntities, error)
}
