package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/defermq/defermq/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ClaimDelivery atomically fences a ready event by revision and takes a
// processing lease. A rejected claim is returned as classification data rather
// than a storage error so callers can ACK stale and terminal events safely.
func (s *Store) ClaimDelivery(
	ctx context.Context,
	p ClaimDeliveryParams,
) (domain.Delivery, *ClaimRejection, error) {
	if p.DeliveryID == uuid.Nil || p.ScheduleRevision <= 0 || p.Owner == "" || p.Lease <= 0 ||
		p.ClockSkewTolerance < 0 {
		return domain.Delivery{}, nil, errors.New("invalid delivery claim parameters")
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	d, err := scanDelivery(s.pool.QueryRow(ctx, `
		UPDATE deliveries
		SET status = 'processing',
			processing_owner = $1,
			processing_until = clock_timestamp() + $2 * interval '1 millisecond',
			attempts = attempts + 1,
			last_attempt_at = clock_timestamp(),
			updated_at = clock_timestamp()
		WHERE id = $3
		  AND schedule_revision = $4
		  AND status = 'scheduled'
		  AND deliver_at <= clock_timestamp() + $5 * interval '1 millisecond'
		RETURNING `+deliveryColumns,
		p.Owner, durationMillis(p.Lease), p.DeliveryID, p.ScheduleRevision,
		durationMillis(p.ClockSkewTolerance)))
	if err == nil {
		return d, nil, nil
	}
	if !errors.Is(err, domain.ErrNotFound) {
		return domain.Delivery{}, nil, fmt.Errorf("claim delivery: %w", err)
	}
	rejection, err := classifyClaimRejection(ctx, s.pool, p.DeliveryID)
	if err != nil {
		return domain.Delivery{}, nil, err
	}
	return domain.Delivery{}, &rejection, nil
}

type queryRower interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func classifyClaimRejection(ctx context.Context, db queryRower, id uuid.UUID) (ClaimRejection, error) {
	var result ClaimRejection
	err := db.QueryRow(ctx, `
		SELECT status, schedule_revision, deliver_at
		FROM deliveries WHERE id = $1`, id).Scan(
		&result.Status, &result.ScheduleRevision, &result.DeliverAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, nil
	}
	if err != nil {
		return ClaimRejection{}, fmt.Errorf("classify rejected claim: %w", err)
	}
	result.Exists = true
	result.DeliverAt = result.DeliverAt.UTC()
	return result, nil
}

// HeartbeatDelivery extends a lease only while ownership is still valid.
func (s *Store) HeartbeatDelivery(ctx context.Context, id uuid.UUID, owner string, lease time.Duration) error {
	if owner == "" || lease <= 0 {
		return errors.New("heartbeat owner and positive lease are required")
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	tag, err := s.pool.Exec(ctx, `
		UPDATE deliveries
		SET processing_until = clock_timestamp() + $3 * interval '1 millisecond',
			updated_at = clock_timestamp()
		WHERE id = $1 AND status = 'processing' AND processing_owner = $2`,
		id, owner, durationMillis(lease))
	if err != nil {
		return fmt.Errorf("heartbeat delivery: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrOwnershipLost
	}
	return nil
}

func (s *Store) MarkDelivered(ctx context.Context, id uuid.UUID, owner string) error {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	tag, err := s.pool.Exec(ctx, `
		UPDATE deliveries
		SET status = 'delivered',
			delivered_at = clock_timestamp(),
			processing_owner = NULL,
			processing_until = NULL,
			last_error = NULL,
			updated_at = clock_timestamp()
		WHERE id = $1 AND status = 'processing' AND processing_owner = $2`,
		id, owner)
	if err != nil {
		return fmt.Errorf("mark delivery successful: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrOwnershipLost
	}
	return nil
}

func (s *Store) ScheduleRetry(ctx context.Context, p RetryDeliveryParams) (domain.Delivery, error) {
	if p.Owner == "" || p.Delay < 0 || p.HotHorizon < 0 {
		return domain.Delivery{}, errors.New("invalid retry parameters")
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return domain.Delivery{}, fmt.Errorf("begin delivery retry: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	d, err := scanDelivery(tx.QueryRow(ctx, `
		UPDATE deliveries
		SET status = 'scheduled',
			deliver_at = clock_timestamp() + $3 * interval '1 millisecond',
			schedule_revision = schedule_revision + 1,
			hot_registered_revision = NULL,
			processing_owner = NULL,
			processing_until = NULL,
			last_error = left($4, 2048),
			updated_at = clock_timestamp()
		WHERE id = $1 AND status = 'processing' AND processing_owner = $2
		RETURNING `+deliveryColumns,
		p.DeliveryID, p.Owner, durationMillis(p.Delay), p.LastError))
	if errors.Is(err, domain.ErrNotFound) {
		return domain.Delivery{}, domain.ErrOwnershipLost
	}
	if err != nil {
		return domain.Delivery{}, fmt.Errorf("schedule delivery retry: %w", err)
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
		return domain.Delivery{}, fmt.Errorf("commit delivery retry: %w", err)
	}
	return d, nil
}

func (s *Store) MarkDead(ctx context.Context, id uuid.UUID, owner string, cause error) error {
	message := "delivery failed permanently"
	if cause != nil {
		message = cause.Error()
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	tag, err := s.pool.Exec(ctx, `
		UPDATE deliveries
		SET status = 'dead',
			processing_owner = NULL,
			processing_until = NULL,
			last_error = left($3, 2048),
			updated_at = clock_timestamp()
		WHERE id = $1 AND status = 'processing' AND processing_owner = $2`,
		id, owner, message)
	if err != nil {
		return fmt.Errorf("mark delivery dead: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrOwnershipLost
	}
	return nil
}

func (s *Store) DeliveryState(ctx context.Context, id uuid.UUID) (ClaimRejection, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	return classifyClaimRejection(ctx, s.pool, id)
}
