package common

import "github.com/avanha/pmaas-plugin-dblog/entities"

type StatusAndEntities struct {
	Status   entities.StatusEntity
	Entities []entities.LoggedTrackableEntity
}

type EntityStore interface {
	GetStatusAndEntities() (StatusAndEntities, error)
}
