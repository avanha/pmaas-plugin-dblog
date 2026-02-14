package writer

import "fmt"

type RetryableError struct {
	Message              string
	Err                  error
	CausedByMissingTable bool
}

func (e *RetryableError) Error() string {
	return fmt.Sprintf("%s: %v", e.Message, e.Err)
}

func (e *RetryableError) Unwrap() error {
	return e.Err
}

func NewRetryableError(message string, err error) *RetryableError {
	return &RetryableError{
		Message: message,
		Err:     err,
	}
}
