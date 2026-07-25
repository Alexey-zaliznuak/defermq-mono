package gateway

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/defermq/defermq/internal/domain"
	"github.com/defermq/defermq/internal/observability"
	"github.com/defermq/defermq/internal/storage/postgres"
	"github.com/google/uuid"
)

// PostgresRepository adapts the shared PostgreSQL store to the consumer-owned
// Gateway repository contract.
type PostgresRepository struct {
	store   *postgres.Store
	metrics *observability.GatewayMetrics
}

func NewPostgresRepository(store *postgres.Store, metrics *observability.GatewayMetrics) (*PostgresRepository, error) {
	if store == nil {
		return nil, fmt.Errorf("postgres store is required")
	}
	return &PostgresRepository{store: store, metrics: metrics}, nil
}

func (r *PostgresRepository) Create(ctx context.Context, command CreateCommand) (domain.Delivery, bool, error) {
	started := time.Now()
	id := command.DeliveryID
	var err error
	if id == uuid.Nil {
		id, err = uuid.NewV7()
		if err != nil {
			return domain.Delivery{}, false, fmt.Errorf("generate delivery ID: %w", err)
		}
	}
	command.Payload.ID, err = nonNilUUID(command.Payload.ID)
	if err != nil {
		return domain.Delivery{}, false, err
	}
	delivery, replay, err := r.store.CreateDelivery(ctx, postgres.CreateDeliveryParams{
		Delivery: domain.Delivery{
			ID: id, PayloadID: command.Payload.ID, IdempotencyKey: command.IdempotencyKey,
			DestinationType: command.DestinationType, Destination: command.Destination,
			DeliverAt: command.DeliverAt.UTC(), MaxAttempts: command.MaxAttempts,
		},
		Payload: command.Payload, HotHorizon: command.HotHorizon,
	})
	r.observe("create", started, err)
	return delivery, replay, err
}

func (r *PostgresRepository) Get(ctx context.Context, id uuid.UUID) (Message, error) {
	started := time.Now()
	delivery, err := r.store.GetDelivery(ctx, id)
	if err != nil {
		r.observe("get", started, err)
		return Message{}, err
	}
	var metadata PayloadMetadata
	err = r.store.Pool().QueryRow(ctx, `
		SELECT id, content_type, size_bytes, created_at
		FROM message_payloads
		WHERE id = $1`, delivery.PayloadID).Scan(
		&metadata.ID, &metadata.ContentType, &metadata.SizeBytes, &metadata.CreatedAt,
	)
	if err != nil {
		err = fmt.Errorf("get payload metadata: %w", err)
		r.observe("get", started, err)
		return Message{}, err
	}
	metadata.CreatedAt = metadata.CreatedAt.UTC()
	r.observe("get", started, nil)
	return Message{Delivery: delivery, Payload: metadata}, nil
}

func (r *PostgresRepository) Reschedule(
	ctx context.Context,
	id uuid.UUID,
	deliverAt time.Time,
	hotHorizon time.Duration,
) (domain.Delivery, error) {
	started := time.Now()
	delivery, err := r.store.RescheduleDelivery(ctx, postgres.RescheduleParams{
		DeliveryID: id, DeliverAt: deliverAt.UTC(), HotHorizon: hotHorizon,
	})
	r.observe("reschedule", started, err)
	return delivery, err
}

func (r *PostgresRepository) Cancel(ctx context.Context, id uuid.UUID) (domain.Delivery, error) {
	started := time.Now()
	delivery, err := r.store.CancelDelivery(ctx, id)
	r.observe("cancel", started, err)
	return delivery, err
}

func (r *PostgresRepository) Ping(ctx context.Context) error {
	started := time.Now()
	err := r.store.Ping(ctx)
	r.observe("ping", started, err)
	return err
}

func (r *PostgresRepository) CheckSchema(ctx context.Context) error {
	started := time.Now()
	err := r.store.SchemaReady(ctx)
	r.observe("schema_check", started, err)
	return err
}

func nonNilUUID(id uuid.UUID) (uuid.UUID, error) {
	if id == uuid.Nil {
		generated, err := uuid.NewV7()
		if err != nil {
			return uuid.Nil, fmt.Errorf("generate payload ID: %w", err)
		}
		return generated, nil
	}
	return id, nil
}

func (r *PostgresRepository) observe(operation string, started time.Time, err error) {
	if r.metrics == nil {
		return
	}
	r.metrics.DBOperationDuration.WithLabelValues(operation).Observe(time.Since(started).Seconds())
	if err != nil && !errors.Is(err, domain.ErrNotFound) && !errors.Is(err, domain.ErrInvalidState) {
		r.metrics.DBErrors.WithLabelValues(operation).Inc()
	}
}
