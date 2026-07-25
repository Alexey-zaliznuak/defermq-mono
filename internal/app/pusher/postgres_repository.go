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
	ClaimDeliveriesBatch(
		context.Context,
		[]postgres.ClaimRequest,
		string,
		time.Duration,
		time.Duration,
	) ([]postgres.BatchClaimResult, error)
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

func (r *PostgresRepository) ClaimBatch(
	ctx context.Context,
	requests []ClaimRequest,
	owner string,
	lease time.Duration,
	tolerance time.Duration,
) ([]ClaimResult, error) {
	storageRequests := make([]postgres.ClaimRequest, len(requests))
	for index, request := range requests {
		storageRequests[index] = postgres.ClaimRequest{
			DeliveryID: request.DeliveryID, ScheduleRevision: request.ScheduleRevision,
		}
	}
	records, err := r.backend.ClaimDeliveriesBatch(ctx, storageRequests, owner, lease, tolerance)
	if err != nil {
		return nil, err
	}
	results := make([]ClaimResult, len(records))
	for index, record := range records {
		results[index] = ClaimResult{
			Reason: ClaimReason(record.Reason), Delivery: record.Delivery,
			Payload: record.Payload, Wait: record.Wait,
		}
	}
	return results, nil
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
