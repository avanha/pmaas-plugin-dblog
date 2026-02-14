package entities

import "time"

type StatusEntity struct {
	DriverName         string
	DbName             string
	Status             string
	LastFailureTime    time.Time
	LastFailureMessage string
	SuccessCount       int
	FailureCount       int
	RetryQueueSize     int
}
