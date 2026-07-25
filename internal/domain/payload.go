package domain

import (
	"time"

	"github.com/google/uuid"
)

type Payload struct {
	ID          uuid.UUID
	Body        []byte
	Headers     map[string]string
	ContentType string
	SizeBytes   int64
	CreatedAt   time.Time
}
