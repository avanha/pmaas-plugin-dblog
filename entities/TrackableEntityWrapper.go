package entities

import spi "github.com/avanha/pmaas-spi/tracking"

type TrackableEntityWrapper interface {
	spi.TrackableHistoryRepo
}
