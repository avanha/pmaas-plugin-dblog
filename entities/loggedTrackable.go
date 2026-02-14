package entities

import "time"

type LoggedTrackableEntity struct {
	Id                      string
	Name                    string
	TrackingMode            string
	PollIntervalSeconds     int
	DataSource              string
	HasArgFactoryFunction   bool
	FieldCount              int
	RegistrationTime        time.Time
	Status                  string
	PollCount               int
	LastPollTime            time.Time
	SuccessCount            int
	LastSuccessTime         time.Time
	SoftFailureCount        int
	LastSoftFailureTime     time.Time
	FailureCount            int
	LastFailureTime         time.Time
	LastFailureErrorMessage string
}
