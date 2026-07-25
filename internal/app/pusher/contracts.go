package pusher

import (
	"context"
	"time"

	"github.com/defermq/defermq/internal/domain"
	"github.com/google/uuid"
)

type Message interface {
	Data() []byte
	Ack(context.Context) error
	Nak(context.Context, time.Duration) error
	Term(context.Context) error
}

type Consumer interface {
	Type() domain.DestinationType
	Next(context.Context) (Message, error)
	Ready(context.Context) error
	Close(context.Context) error
}

type ClaimReason string

const (
	Claimed         ClaimReason = "claimed"
	ClaimNotFound   ClaimReason = "not_found"
	ClaimStale      ClaimReason = "stale_revision"
	ClaimTerminal   ClaimReason = "terminal"
	ClaimProcessing ClaimReason = "processing"
	ClaimTooEarly   ClaimReason = "too_early"
)

type ClaimResult struct {
	Reason   ClaimReason
	Delivery *domain.Delivery
	Payload  *domain.Payload
	Wait     time.Duration
}

type ClaimRequest struct {
	DeliveryID       uuid.UUID
	ScheduleRevision int64
}

// Repository is intentionally declared by the Pusher consumer. Every state
// transition must commit before the caller acknowledges the ready message.
type Repository interface {
	ClaimBatch(
		context.Context,
		[]ClaimRequest,
		string,
		time.Duration,
		time.Duration,
	) ([]ClaimResult, error)
	Heartbeat(context.Context, uuid.UUID, string, time.Duration) (bool, error)
	MarkDelivered(context.Context, uuid.UUID, string) (bool, error)
	ScheduleRetry(
		context.Context,
		uuid.UUID,
		string,
		time.Duration,
		string,
		time.Duration,
	) (bool, error)
	MarkDead(context.Context, uuid.UUID, string, string) (bool, error)
	Ready(context.Context) error
}
