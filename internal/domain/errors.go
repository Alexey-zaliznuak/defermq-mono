package domain

import "errors"

var (
	ErrNotFound        = errors.New("not found")
	ErrConflict        = errors.New("conflict")
	ErrInvalidState    = errors.New("invalid state")
	ErrOwnershipLost   = errors.New("processing ownership lost")
	ErrStaleRevision   = errors.New("stale schedule revision")
	ErrTooEarly        = errors.New("delivery is not due")
	ErrPayloadTooLarge = errors.New("payload is too large")
	ErrAdapterDisabled = errors.New("destination adapter is disabled")
)
