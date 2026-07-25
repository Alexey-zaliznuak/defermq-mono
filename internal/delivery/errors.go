package delivery

import (
	"errors"
	"fmt"
	"time"
)

type PushError struct {
	Err        error
	Retryable  bool
	Code       string
	RetryAfter time.Duration
}

func (e *PushError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Err == nil {
		return e.Code
	}
	if e.Code == "" {
		return e.Err.Error()
	}
	return fmt.Sprintf("%s: %v", e.Code, e.Err)
}

func (e *PushError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func NewPushError(code string, retryable bool, err error) *PushError {
	return &PushError{Err: err, Retryable: retryable, Code: code}
}

func ErrorInfo(err error) *PushError {
	if err == nil {
		return nil
	}
	var pushErr *PushError
	if errors.As(err, &pushErr) {
		return pushErr
	}
	return &PushError{Err: err, Retryable: true, Code: "adapter_error"}
}

func ErrorMessage(err error, maxBytes int) string {
	if err == nil {
		return ""
	}
	message := err.Error()
	if maxBytes > 0 && len(message) > maxBytes {
		message = message[:maxBytes]
	}
	return message
}
