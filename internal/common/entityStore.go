package common

import "pmaas.io/plugins/dblog/entities"

type StatusAndEntities struct {
	Status   entities.StatusEntity
	Entities []entities.LoggedTrackableEntity
}

type EntityStore interface {
	GetStatusAndEntities() (StatusAndEntities, error)
}
