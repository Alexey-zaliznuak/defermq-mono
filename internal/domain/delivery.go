package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type DeliveryStatus string

const (
	StatusScheduled  DeliveryStatus = "scheduled"
	StatusProcessing DeliveryStatus = "processing"
	StatusDelivered  DeliveryStatus = "delivered"
	StatusCancelled  DeliveryStatus = "cancelled"
	StatusDead       DeliveryStatus = "dead"
)

func (s DeliveryStatus) Terminal() bool {
	return s == StatusDelivered || s == StatusCancelled || s == StatusDead
}

type Delivery struct {
	ID                    uuid.UUID
	PayloadID             uuid.UUID
	IdempotencyKey        *string
	DestinationType       DestinationType
	Destination           json.RawMessage
	DeliverAt             time.Time
	Status                DeliveryStatus
	ScheduleRevision      int64
	HotRegisteredRevision *int64
	Attempts              int
	MaxAttempts           int
	ProcessingOwner       *string
	ProcessingUntil       *time.Time
	LastError             *string
	LastAttemptAt         *time.Time
	DeliveredAt           *time.Time
	CancelledAt           *time.Time
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

type DeliverySummary struct {
	ID               uuid.UUID       `json:"id"`
	Status           DeliveryStatus  `json:"status"`
	DeliverAt        time.Time       `json:"deliver_at"`
	DestinationType  DestinationType `json:"destination_type"`
	ScheduleRevision int64           `json:"schedule_revision"`
	Attempts         int             `json:"attempts"`
	MaxAttempts      int             `json:"max_attempts"`
	LastError        *string         `json:"last_error,omitempty"`
	CreatedAt        time.Time       `json:"created_at"`
	UpdatedAt        time.Time       `json:"updated_at"`
	DeliveredAt      *time.Time      `json:"delivered_at,omitempty"`
	CancelledAt      *time.Time      `json:"cancelled_at,omitempty"`
}
