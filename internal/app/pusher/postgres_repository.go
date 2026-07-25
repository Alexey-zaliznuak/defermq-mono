package pusher

import (
	"context"
	"errors"
	"time"

	"github.com/defermq/defermq/internal/domain"
	"github.com/defermq/defermq/internal/storage/postgres"
	"github.com/google/uuid"
)

// postgresBackend is the narrow storage surface consumed by Pusher. Keeping it
// here makes orchestration testable without making storage define an interface
// for its callers.
type postgresBackend interface {
	ClaimDelivery(context.Context, postgres.ClaimDeliveryParams) (domain.Delivery, *postgres.ClaimRejection, error)
	GetPayload(context.Context, uuid.UUID, int64) (domain.Payload, error)
	HeartbeatDelivery(context.Context, uuid.UUID, string, time.Duration) error
	MarkDelivered(context.Context, uuid.UUID, string) error
	ScheduleRetry(context.Context, postgres.RetryDeliveryParams) (domain.Delivery, error)
	MarkDead(context.Context, uuid.UUID, string, error) error
	Ping(context.Context) error
}

type PostgresRepository struct {
	backend postgresBackend
}

func NewPostgresRepository(backend postgresBackend) (*PostgresRepository, error) {
	if backend == nil {
		return nil, errors.New("Pusher PostgreSQL backend is required")
	}
	return &PostgresRepository{backend: backend}, nil
}

func (r *PostgresRepository) Claim(
	ctx context.Context,
	id uuid.UUID,
	revision int64,
	owner string,
	lease time.Duration,
	tolerance time.Duration,
) (ClaimResult, error) {
	record, rejection, err := r.backend.ClaimDelivery(ctx, postgres.ClaimDeliveryParams{
		DeliveryID:         id,
		ScheduleRevision:   revision,
		Owner:              owner,
		Lease:              lease,
		ClockSkewTolerance: tolerance,
	})
	if err != nil {
		return ClaimResult{}, err
	}
	if rejection == nil {
		return ClaimResult{Reason: Claimed, Delivery: &record}, nil
	}
	if !rejection.Exists {
		return ClaimResult{Reason: ClaimNotFound}, nil
	}
	if rejection.ScheduleRevision != revision {
		return ClaimResult{Reason: ClaimStale}, nil
	}
	if rejection.Status.Terminal() {
		return ClaimResult{Reason: ClaimTerminal}, nil
	}
	if rejection.Status == domain.StatusProcessing {
		return ClaimResult{Reason: ClaimProcessing}, nil
	}
	wait := time.Until(rejection.DeliverAt)
	if wait > tolerance {
		return ClaimResult{Reason: ClaimTooEarly, Wait: wait}, nil
	}
	return ClaimResult{Reason: ClaimStale}, nil
}

func (r *PostgresRepository) LoadPayload(
	ctx context.Context,
	id uuid.UUID,
	maxBytes int64,
) (domain.Payload, error) {
	return r.backend.GetPayload(ctx, id, maxBytes)
}

func (r *PostgresRepository) Heartbeat(
	ctx context.Context,
	id uuid.UUID,
	owner string,
	lease time.Duration,
) (bool, error) {
	err := r.backend.HeartbeatDelivery(ctx, id, owner, lease)
	if errors.Is(err, domain.ErrOwnershipLost) {
		return false, nil
	}
	return err == nil, err
}

func (r *PostgresRepository) MarkDelivered(
	ctx context.Context,
	id uuid.UUID,
	owner string,
) (bool, error) {
	err := r.backend.MarkDelivered(ctx, id, owner)
	if errors.Is(err, domain.ErrOwnershipLost) {
		return false, nil
	}
	return err == nil, err
}

func (r *PostgresRepository) ScheduleRetry(
	ctx context.Context,
	id uuid.UUID,
	owner string,
	delay time.Duration,
	errorText string,
	hotHorizon time.Duration,
) (bool, error) {
	_, err := r.backend.ScheduleRetry(ctx, postgres.RetryDeliveryParams{
		DeliveryID: id,
		Owner:      owner,
		Delay:      delay,
		HotHorizon: hotHorizon,
		LastError:  errorText,
	})
	if errors.Is(err, domain.ErrOwnershipLost) {
		return false, nil
	}
	return err == nil, err
}

func (r *PostgresRepository) MarkDead(
	ctx context.Context,
	id uuid.UUID,
	owner string,
	errorText string,
) (bool, error) {
	err := r.backend.MarkDead(ctx, id, owner, errors.New(errorText))
	if errors.Is(err, domain.ErrOwnershipLost) {
		return false, nil
	}
	return err == nil, err
}

func (r *PostgresRepository) Ready(ctx context.Context) error {
	return r.backend.Ping(ctx)
}
