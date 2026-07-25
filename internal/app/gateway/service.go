package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/defermq/defermq/internal/domain"
	"github.com/defermq/defermq/internal/ingest"
	"github.com/google/uuid"
)

type Repository interface {
	// Create atomically persists payload and delivery. It must use database time to
	// decide whether a ready/schedule outbox row is needed. A uniqueness race on
	// IdempotencyKey must return the existing delivery with replay=true.
	Create(context.Context, CreateCommand) (delivery domain.Delivery, replay bool, err error)
	Get(context.Context, uuid.UUID) (Message, error)
	// Reschedule and Cancel must fence stale NATS events by incrementing revision.
	Reschedule(context.Context, uuid.UUID, time.Time, time.Duration) (domain.Delivery, error)
	Cancel(context.Context, uuid.UUID) (domain.Delivery, error)
	Ping(context.Context) error
	CheckSchema(context.Context) error
}

type CreateCommand struct {
	DeliveryID      uuid.UUID
	Payload         domain.Payload
	Destination     json.RawMessage
	DestinationType domain.DestinationType
	DeliverAt       time.Time
	MaxAttempts     int
	IdempotencyKey  *string
	HotHorizon      time.Duration
}

type CreateInput struct {
	Payload        domain.Payload
	Destination    domain.Destination
	DeliverAt      time.Time
	MaxAttempts    int
	IdempotencyKey string
}

type Message struct {
	Delivery domain.Delivery
	Payload  PayloadMetadata
}

type PayloadMetadata struct {
	ID          uuid.UUID
	ContentType string
	SizeBytes   int64
	CreatedAt   time.Time
}

type Service struct {
	repository             Repository
	hotHorizon             time.Duration
	maxPayloadBytes        int64
	defaultMaxAttempts     int
	maxIdempotencyKeyBytes int
	enabledDestinations    map[domain.DestinationType]bool
}

type Options struct {
	HotHorizon             time.Duration
	MaxPayloadBytes        int64
	DefaultMaxAttempts     int
	MaxIdempotencyKeyBytes int
	EnabledDestinations    map[domain.DestinationType]bool
}

func New(repository Repository, options Options) (*Service, error) {
	if repository == nil {
		return nil, errors.New("gateway repository is required")
	}
	if options.HotHorizon < 0 || options.MaxPayloadBytes <= 0 || options.DefaultMaxAttempts <= 0 ||
		options.MaxIdempotencyKeyBytes <= 0 || len(options.EnabledDestinations) == 0 {
		return nil, errors.New("invalid gateway service options")
	}
	return &Service{
		repository:             repository,
		hotHorizon:             options.HotHorizon,
		maxPayloadBytes:        options.MaxPayloadBytes,
		defaultMaxAttempts:     options.DefaultMaxAttempts,
		maxIdempotencyKeyBytes: options.MaxIdempotencyKeyBytes,
		enabledDestinations:    options.EnabledDestinations,
	}, nil
}

func (s *Service) Create(ctx context.Context, input CreateInput) (domain.Delivery, bool, error) {
	if input.DeliverAt.IsZero() {
		return domain.Delivery{}, false, fmt.Errorf("%w: deliver_at is required", ErrValidation)
	}
	input.DeliverAt = input.DeliverAt.UTC()
	if input.MaxAttempts == 0 {
		input.MaxAttempts = s.defaultMaxAttempts
	}
	if input.MaxAttempts < 1 || input.MaxAttempts > 1000 {
		return domain.Delivery{}, false, fmt.Errorf("%w: max_attempts must be between 1 and 1000", ErrValidation)
	}
	if input.Payload.ContentType == "" {
		return domain.Delivery{}, false, fmt.Errorf("%w: payload content_type is required", ErrValidation)
	}
	if int64(len(input.Payload.Body)) > s.maxPayloadBytes {
		return domain.Delivery{}, false, domain.ErrPayloadTooLarge
	}
	if err := domain.ValidateHeaders(input.Payload.Headers); err != nil {
		return domain.Delivery{}, false, err
	}
	input.Payload.SizeBytes = int64(len(input.Payload.Body))
	if err := input.Destination.Validate(s.enabledDestinations); err != nil {
		return domain.Delivery{}, false, err
	}
	destination, err := json.Marshal(input.Destination)
	if err != nil {
		return domain.Delivery{}, false, fmt.Errorf("encode destination: %w", err)
	}
	var key *string
	if input.IdempotencyKey != "" {
		trimmed := strings.TrimSpace(input.IdempotencyKey)
		if trimmed == "" || len(trimmed) > s.maxIdempotencyKeyBytes {
			return domain.Delivery{}, false, fmt.Errorf("%w: invalid Idempotency-Key", ErrValidation)
		}
		key = &trimmed
	}
	deliveryID, payloadID, err := ingest.IDs(key)
	if err != nil {
		return domain.Delivery{}, false, err
	}
	input.Payload.ID = payloadID
	return s.repository.Create(ctx, CreateCommand{
		DeliveryID:      deliveryID,
		Payload:         input.Payload,
		Destination:     destination,
		DestinationType: input.Destination.Type,
		DeliverAt:       input.DeliverAt,
		MaxAttempts:     input.MaxAttempts,
		IdempotencyKey:  key,
		HotHorizon:      s.hotHorizon,
	})
}

func (s *Service) Get(ctx context.Context, id uuid.UUID) (Message, error) {
	return s.repository.Get(ctx, id)
}

func (s *Service) Reschedule(ctx context.Context, id uuid.UUID, deliverAt time.Time) (domain.Delivery, error) {
	if deliverAt.IsZero() {
		return domain.Delivery{}, fmt.Errorf("%w: deliver_at is required", ErrValidation)
	}
	return s.repository.Reschedule(ctx, id, deliverAt.UTC(), s.hotHorizon)
}

func (s *Service) Cancel(ctx context.Context, id uuid.UUID) (domain.Delivery, error) {
	return s.repository.Cancel(ctx, id)
}

func (s *Service) Ready(ctx context.Context) map[string]error {
	return map[string]error{
		"postgres": s.repository.Ping(ctx),
		"schema":   s.repository.CheckSchema(ctx),
	}
}
