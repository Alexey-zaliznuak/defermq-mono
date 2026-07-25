package gateway

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/defermq/defermq/internal/domain"
	"github.com/defermq/defermq/internal/ingest"
	"github.com/defermq/defermq/internal/storage/postgres"
	"github.com/google/uuid"
)

type CommandPublisher interface {
	PublishWithSequence(context.Context, ingest.Command) (uint64, error)
}

type dependencyChecker interface {
	Ready(context.Context) error
}

// IngestRepository publishes all mutations durably before acknowledging HTTP.
// The small pending cache makes read-after-write work on the accepting Gateway;
// PostgreSQL remains authoritative once the writer commits the command.
type IngestRepository struct {
	store     *postgres.Store
	publisher CommandPublisher
	nats      dependencyChecker
	pending   PendingStore
	now       func() time.Time
}

func NewIngestRepository(store *postgres.Store, publisher CommandPublisher, nats dependencyChecker, pending PendingStore) (*IngestRepository, error) {
	if store == nil || publisher == nil || nats == nil || pending == nil {
		return nil, errors.New("store, ingest publisher, NATS checker and pending store are required")
	}
	return &IngestRepository{
		store: store, publisher: publisher, nats: nats, now: time.Now,
		pending: pending,
	}, nil
}

func (r *IngestRepository) Create(ctx context.Context, command CreateCommand) (domain.Delivery, bool, error) {
	if command.DeliveryID == uuid.Nil || command.Payload.ID == uuid.Nil {
		return domain.Delivery{}, false, errors.New("ingest IDs are required")
	}
	if existing, err := r.store.GetDelivery(ctx, command.DeliveryID); err == nil {
		return existing, true, nil
	} else if !errors.Is(err, domain.ErrNotFound) {
		return domain.Delivery{}, false, err
	}
	pending, err := r.pending.Get(ctx, command.DeliveryID)
	if err == nil {
		return pending.Message.Delivery, true, nil
	}
	if !errors.Is(err, domain.ErrNotFound) {
		return domain.Delivery{}, false, err
	}
	createdAt := r.now().UTC()
	delivery := domain.Delivery{
		ID: command.DeliveryID, PayloadID: command.Payload.ID, IdempotencyKey: command.IdempotencyKey,
		DestinationType: command.DestinationType, Destination: command.Destination,
		DeliverAt: command.DeliverAt.UTC(), Status: domain.StatusScheduled, ScheduleRevision: 1,
		MaxAttempts: command.MaxAttempts, CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	ingestCommand := ingest.Command{
		SchemaVersion: ingest.SchemaVersion, Kind: ingest.KindCreate,
		CommandID: command.DeliveryID, DeliveryID: command.DeliveryID, PayloadID: command.Payload.ID,
		IdempotencyKey: command.IdempotencyKey, Destination: command.Destination,
		DestinationType: command.DestinationType, DeliverAt: command.DeliverAt.UTC(),
		MaxAttempts: command.MaxAttempts, HotHorizon: command.HotHorizon,
		Payload: &ingest.Payload{
			Body: command.Payload.Body, Headers: command.Payload.Headers,
			ContentType: command.Payload.ContentType, SizeBytes: command.Payload.SizeBytes,
		},
	}
	sequence, err := r.publisher.PublishWithSequence(ctx, ingestCommand)
	if err != nil {
		return domain.Delivery{}, false, err
	}
	message := Message{Delivery: delivery, Payload: PayloadMetadata{
		ID: command.Payload.ID, ContentType: command.Payload.ContentType,
		SizeBytes: command.Payload.SizeBytes, CreatedAt: createdAt,
	}}
	if err := r.pending.Put(ctx, delivery.ID, message, sequence); err != nil {
		return domain.Delivery{}, false, err
	}
	return delivery, false, nil
}

func (r *IngestRepository) Get(ctx context.Context, id uuid.UUID) (Message, error) {
	delivery, err := r.store.GetDelivery(ctx, id)
	if err == nil {
		pending, pendingErr := r.pending.Get(ctx, id)
		if pendingErr == nil && pending.Message.Delivery.ScheduleRevision > delivery.ScheduleRevision {
			return pending.Message, nil
		}
		if pendingErr != nil && !errors.Is(pendingErr, domain.ErrNotFound) {
			return Message{}, pendingErr
		}
		if deleteErr := r.pending.DeletePersisted(ctx, id, delivery.ScheduleRevision); deleteErr != nil {
			return Message{}, deleteErr
		}
		var metadata PayloadMetadata
		err = r.store.Pool().QueryRow(ctx, `
			SELECT id, content_type, size_bytes, created_at
			FROM message_payloads WHERE id = $1`, delivery.PayloadID,
		).Scan(&metadata.ID, &metadata.ContentType, &metadata.SizeBytes, &metadata.CreatedAt)
		if err != nil {
			return Message{}, fmt.Errorf("get payload metadata: %w", err)
		}
		metadata.CreatedAt = metadata.CreatedAt.UTC()
		return Message{Delivery: delivery, Payload: metadata}, nil
	}
	if !errors.Is(err, domain.ErrNotFound) {
		return Message{}, err
	}
	pending, err := r.pending.Get(ctx, id)
	if err != nil {
		return Message{}, err
	}
	return pending.Message, nil
}

func (r *IngestRepository) Reschedule(ctx context.Context, id uuid.UUID, deliverAt time.Time, hotHorizon time.Duration) (domain.Delivery, error) {
	message, err := r.Get(ctx, id)
	if err != nil {
		return domain.Delivery{}, err
	}
	if message.Delivery.Status != domain.StatusScheduled {
		return domain.Delivery{}, domain.ErrInvalidState
	}
	commandID, err := uuid.NewV7()
	if err != nil {
		return domain.Delivery{}, err
	}
	sequence, err := r.publisher.PublishWithSequence(ctx, ingest.Command{
		SchemaVersion: ingest.SchemaVersion, Kind: ingest.KindReschedule, CommandID: commandID,
		DeliveryID: id, DeliverAt: deliverAt.UTC(), HotHorizon: hotHorizon,
		ExpectedRevision: message.Delivery.ScheduleRevision,
	})
	if err != nil {
		return domain.Delivery{}, err
	}
	message.Delivery.DeliverAt = deliverAt.UTC()
	message.Delivery.ScheduleRevision++
	message.Delivery.HotRegisteredRevision = nil
	message.Delivery.UpdatedAt = r.now().UTC()
	if err := r.pending.Put(ctx, id, message, sequence); err != nil {
		return domain.Delivery{}, err
	}
	return message.Delivery, nil
}

func (r *IngestRepository) Cancel(ctx context.Context, id uuid.UUID) (domain.Delivery, error) {
	message, err := r.Get(ctx, id)
	if err != nil {
		return domain.Delivery{}, err
	}
	if message.Delivery.Status != domain.StatusScheduled {
		return domain.Delivery{}, domain.ErrInvalidState
	}
	commandID, err := uuid.NewV7()
	if err != nil {
		return domain.Delivery{}, err
	}
	sequence, err := r.publisher.PublishWithSequence(ctx, ingest.Command{
		SchemaVersion: ingest.SchemaVersion, Kind: ingest.KindCancel,
		CommandID: commandID, DeliveryID: id,
		ExpectedRevision: message.Delivery.ScheduleRevision,
	})
	if err != nil {
		return domain.Delivery{}, err
	}
	now := r.now().UTC()
	message.Delivery.Status = domain.StatusCancelled
	message.Delivery.ScheduleRevision++
	message.Delivery.HotRegisteredRevision = nil
	message.Delivery.CancelledAt = &now
	message.Delivery.UpdatedAt = now
	if err := r.pending.Put(ctx, id, message, sequence); err != nil {
		return domain.Delivery{}, err
	}
	return message.Delivery, nil
}

func (r *IngestRepository) Ping(ctx context.Context) error {
	return errors.Join(r.store.Ping(ctx), r.nats.Ready(ctx))
}

func (r *IngestRepository) CheckSchema(ctx context.Context) error {
	return r.store.SchemaReady(ctx)
}
