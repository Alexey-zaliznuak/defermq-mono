package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/defermq/defermq/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func (s *Store) CreateDelivery(ctx context.Context, p CreateDeliveryParams) (domain.Delivery, bool, error) {
	if err := validateCreate(p); err != nil {
		return domain.Delivery{}, false, err
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return domain.Delivery{}, false, fmt.Errorf("begin create delivery: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	headers, err := json.Marshal(p.Payload.Headers)
	if err != nil {
		return domain.Delivery{}, false, fmt.Errorf("encode payload headers: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO message_payloads (id, body, headers, content_type, size_bytes)
		VALUES ($1, $2, $3, $4, $5)`,
		p.Payload.ID, p.Payload.Body, headers, p.Payload.ContentType, p.Payload.SizeBytes)
	if err != nil {
		return domain.Delivery{}, false, fmt.Errorf("insert payload: %w", err)
	}

	d := p.Delivery
	row := tx.QueryRow(ctx, `
		INSERT INTO deliveries (
			id, idempotency_key, payload_id, destination_type, destination,
			deliver_at, max_attempts
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING `+deliveryColumns,
		d.ID, d.IdempotencyKey, p.Payload.ID, d.DestinationType, d.Destination,
		d.DeliverAt.UTC(), d.MaxAttempts)
	created, err := scanDelivery(row)
	if err != nil {
		if isUniqueViolation(err) && d.IdempotencyKey != nil {
			_ = tx.Rollback(ctx)
			existing, getErr := s.getDeliveryByIdempotencyKey(ctx, *d.IdempotencyKey)
			if getErr != nil {
				return domain.Delivery{}, false, fmt.Errorf("resolve idempotency conflict: %w", getErr)
			}
			return existing, true, nil
		}
		return domain.Delivery{}, false, fmt.Errorf("insert delivery: %w", err)
	}

	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return domain.Delivery{}, false, fmt.Errorf("read database time: %w", err)
	}
	if kind, ok := outboxKindFor(created.DeliverAt, now, p.HotHorizon); ok {
		if err := insertOutbox(ctx, tx, created, kind); err != nil {
			return domain.Delivery{}, false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Delivery{}, false, fmt.Errorf("commit create delivery: %w", err)
	}
	return created, false, nil
}

func validateCreate(p CreateDeliveryParams) error {
	if p.Delivery.ID == uuid.Nil || p.Payload.ID == uuid.Nil {
		return errors.New("delivery and payload IDs are required")
	}
	if p.Delivery.PayloadID != uuid.Nil && p.Delivery.PayloadID != p.Payload.ID {
		return errors.New("delivery payload ID does not match payload")
	}
	if p.Delivery.DeliverAt.IsZero() {
		return errors.New("deliver_at is required")
	}
	if p.Delivery.MaxAttempts <= 0 {
		return errors.New("max_attempts must be positive")
	}
	if p.Payload.ContentType == "" || p.Payload.SizeBytes < 0 || p.Payload.SizeBytes != int64(len(p.Payload.Body)) {
		return errors.New("invalid payload metadata")
	}
	if len(p.Delivery.Destination) == 0 {
		return errors.New("destination is required")
	}
	return nil
}

func outboxKindFor(deliverAt, now time.Time, hotHorizon time.Duration) (OutboxKind, bool) {
	if !deliverAt.After(now) {
		return OutboxReady, true
	}
	if !deliverAt.After(now.Add(hotHorizon)) {
		return OutboxHotRegister, true
	}
	return "", false
}

func insertOutbox(ctx context.Context, tx pgx.Tx, d domain.Delivery, kind OutboxKind) error {
	var deliverAt any
	if kind == OutboxHotRegister {
		deliverAt = d.DeliverAt
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO nats_outbox (
			delivery_id, schedule_revision, kind, deliver_at, destination_type
		)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (delivery_id, schedule_revision, kind) DO NOTHING`,
		d.ID, d.ScheduleRevision, kind, deliverAt, d.DestinationType)
	if err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}
	return nil
}

func (s *Store) GetDelivery(ctx context.Context, id uuid.UUID) (domain.Delivery, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	return scanDelivery(s.pool.QueryRow(ctx,
		`SELECT `+deliveryColumns+` FROM deliveries WHERE id = $1`, id))
}

func (s *Store) GetDeliveryByIdempotencyKey(ctx context.Context, key string) (domain.Delivery, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	return s.getDeliveryByIdempotencyKey(ctx, key)
}

func (s *Store) getDeliveryByIdempotencyKey(ctx context.Context, key string) (domain.Delivery, error) {
	return scanDelivery(s.pool.QueryRow(ctx,
		`SELECT `+deliveryColumns+` FROM deliveries WHERE idempotency_key = $1`, key))
}

func (s *Store) CancelDelivery(ctx context.Context, id uuid.UUID) (domain.Delivery, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	d, err := scanDelivery(s.pool.QueryRow(ctx, `
		UPDATE deliveries
		SET status = 'cancelled',
			schedule_revision = schedule_revision + 1,
			hot_registered_revision = NULL,
			cancelled_at = clock_timestamp(),
			updated_at = clock_timestamp()
		WHERE id = $1 AND status = 'scheduled'
		RETURNING `+deliveryColumns, id))
	if !errors.Is(err, domain.ErrNotFound) {
		return d, err
	}
	return domain.Delivery{}, s.stateError(ctx, id)
}

func (s *Store) RescheduleDelivery(ctx context.Context, p RescheduleParams) (domain.Delivery, error) {
	if p.DeliverAt.IsZero() {
		return domain.Delivery{}, errors.New("deliver_at is required")
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return domain.Delivery{}, fmt.Errorf("begin reschedule: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	d, err := scanDelivery(tx.QueryRow(ctx, `
		UPDATE deliveries
		SET deliver_at = $2,
			schedule_revision = schedule_revision + 1,
			hot_registered_revision = NULL,
			updated_at = clock_timestamp()
		WHERE id = $1 AND status = 'scheduled'
		RETURNING `+deliveryColumns, p.DeliveryID, p.DeliverAt.UTC()))
	if errors.Is(err, domain.ErrNotFound) {
		return domain.Delivery{}, s.stateErrorTx(ctx, tx, p.DeliveryID)
	}
	if err != nil {
		return domain.Delivery{}, fmt.Errorf("reschedule delivery: %w", err)
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return domain.Delivery{}, fmt.Errorf("read database time: %w", err)
	}
	if kind, ok := outboxKindFor(d.DeliverAt, now, p.HotHorizon); ok {
		if err := insertOutbox(ctx, tx, d, kind); err != nil {
			return domain.Delivery{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Delivery{}, fmt.Errorf("commit reschedule: %w", err)
	}
	return d, nil
}

func (s *Store) stateError(ctx context.Context, id uuid.UUID) error {
	var status domain.DeliveryStatus
	err := s.pool.QueryRow(ctx, `SELECT status FROM deliveries WHERE id = $1`, id).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("read delivery state: %w", err)
	}
	return fmt.Errorf("%w: delivery is %s", domain.ErrInvalidState, status)
}

func (s *Store) stateErrorTx(ctx context.Context, tx pgx.Tx, id uuid.UUID) error {
	var status domain.DeliveryStatus
	err := tx.QueryRow(ctx, `SELECT status FROM deliveries WHERE id = $1`, id).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("read delivery state: %w", err)
	}
	return fmt.Errorf("%w: delivery is %s", domain.ErrInvalidState, status)
}

func (s *Store) GetPayload(ctx context.Context, id uuid.UUID, maxBytes int64) (domain.Payload, error) {
	if maxBytes <= 0 {
		return domain.Payload{}, errors.New("payload byte limit must be positive")
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	var p domain.Payload
	err := s.pool.QueryRow(ctx, `
		SELECT id, body, headers, content_type, size_bytes, created_at
		FROM message_payloads
		WHERE id = $1 AND size_bytes <= $2`, id, maxBytes).Scan(
		&p.ID, &p.Body, &p.Headers, &p.ContentType, &p.SizeBytes, &p.CreatedAt)
	if err == nil {
		return p, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.Payload{}, fmt.Errorf("get payload: %w", err)
	}
	var size int64
	err = s.pool.QueryRow(ctx, `SELECT size_bytes FROM message_payloads WHERE id = $1`, id).Scan(&size)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Payload{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Payload{}, fmt.Errorf("classify payload: %w", err)
	}
	return domain.Payload{}, fmt.Errorf("%w: payload is %d bytes, limit is %d", domain.ErrPayloadTooLarge, size, maxBytes)
}
