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

type PromotionResult struct {
	Candidates int
	Inserted   int
}

// PromotionCutoff returns database time plus the configured hot horizon.
func (s *Store) PromotionCutoff(ctx context.Context, hotHorizon time.Duration) (time.Time, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	var cutoff time.Time
	err := s.pool.QueryRow(ctx,
		`SELECT clock_timestamp() + $1 * interval '1 millisecond'`,
		durationMillis(hotHorizon)).Scan(&cutoff)
	if err != nil {
		return time.Time{}, fmt.Errorf("read promotion cutoff: %w", err)
	}
	return cutoff.UTC(), nil
}

// PromoteBatch creates schedule outbox records for the next ordered candidate
// batch. Candidates that already have an outbox row are excluded, preventing a
// conflict from making a drain cycle spin forever.
func (s *Store) PromoteBatch(ctx context.Context, cutoff time.Time, limit int) (PromotionResult, error) {
	if limit <= 0 {
		return PromotionResult{}, errors.New("promotion limit must be positive")
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	var result PromotionResult
	err := s.pool.QueryRow(ctx, `
		WITH candidates AS MATERIALIZED (
			SELECT d.id, d.schedule_revision, d.deliver_at, d.destination_type
			FROM deliveries d
			WHERE d.status = 'scheduled'
			  AND d.deliver_at <= $1
			  AND d.hot_registered_revision IS DISTINCT FROM d.schedule_revision
			  AND NOT EXISTS (
				SELECT 1 FROM nats_outbox o
				WHERE o.delivery_id = d.id
				  AND o.schedule_revision = d.schedule_revision
				  AND o.kind = 'schedule'
			  )
			ORDER BY d.deliver_at, d.id
			LIMIT $2
			FOR UPDATE OF d SKIP LOCKED
		), inserted AS (
			INSERT INTO nats_outbox (
				delivery_id, schedule_revision, kind, deliver_at, destination_type
			)
			SELECT id, schedule_revision, 'schedule', deliver_at, destination_type
			FROM candidates
			ON CONFLICT (delivery_id, schedule_revision, kind) DO NOTHING
			RETURNING 1
		)
		SELECT
			(SELECT count(*) FROM candidates),
			(SELECT count(*) FROM inserted)`,
		cutoff.UTC(), limit).Scan(&result.Candidates, &result.Inserted)
	if err != nil {
		return PromotionResult{}, fmt.Errorf("promote delivery batch: %w", err)
	}
	return result, nil
}

func (s *Store) ClaimOutbox(ctx context.Context, owner string, limit int, lockTTL time.Duration) ([]OutboxItem, error) {
	if owner == "" || limit <= 0 || lockTTL <= 0 {
		return nil, errors.New("outbox owner, positive limit, and lock TTL are required")
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	rows, err := s.pool.Query(ctx, `
		WITH picked AS (
			SELECT id
			FROM nats_outbox
			WHERE published_at IS NULL
			  AND available_at <= clock_timestamp()
			  AND (locked_until IS NULL OR locked_until < clock_timestamp())
			ORDER BY available_at, id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE nats_outbox o
		SET locked_by = $2,
			locked_until = clock_timestamp() + $3 * interval '1 millisecond',
			publish_attempts = publish_attempts + 1
		FROM picked
		WHERE o.id = picked.id
		RETURNING `+outboxColumns,
		limit, owner, durationMillis(lockTTL))
	if err != nil {
		return nil, fmt.Errorf("claim outbox: %w", err)
	}
	defer rows.Close()
	items := make([]OutboxItem, 0, limit)
	for rows.Next() {
		item, scanErr := scanOutbox(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scan claimed outbox: %w", scanErr)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claimed outbox: %w", err)
	}
	return items, nil
}

func (s *Store) MarkOutboxPublished(ctx context.Context, id int64, owner string) error {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin outbox completion: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var deliveryID uuid.UUID
	var revision int64
	var kind OutboxKind
	err = tx.QueryRow(ctx, `
		UPDATE nats_outbox
		SET published_at = clock_timestamp(),
			locked_by = NULL,
			locked_until = NULL,
			last_error = NULL
		WHERE id = $1 AND locked_by = $2 AND published_at IS NULL
		RETURNING delivery_id, schedule_revision, kind`,
		id, owner).Scan(&deliveryID, &revision, &kind)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrOwnershipLost
	}
	if err != nil {
		return fmt.Errorf("mark outbox published: %w", err)
	}
	if kind == OutboxSchedule {
		if _, err := tx.Exec(ctx, `
			UPDATE deliveries
			SET hot_registered_revision = $2, updated_at = clock_timestamp()
			WHERE id = $1 AND schedule_revision = $2 AND status = 'scheduled'`,
			deliveryID, revision); err != nil {
			return fmt.Errorf("mark delivery hot registration: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit outbox completion: %w", err)
	}
	return nil
}

func (s *Store) MarkOutboxFailed(ctx context.Context, id int64, owner string, retryAfter time.Duration, cause error) error {
	if retryAfter < 0 {
		return errors.New("outbox retry delay cannot be negative")
	}
	message := "publish failed"
	if cause != nil {
		message = cause.Error()
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	tag, err := s.pool.Exec(ctx, `
		UPDATE nats_outbox
		SET available_at = clock_timestamp() + $3 * interval '1 millisecond',
			locked_by = NULL,
			locked_until = NULL,
			last_error = left($4, 2048)
		WHERE id = $1 AND locked_by = $2 AND published_at IS NULL`,
		id, owner, durationMillis(retryAfter), message)
	if err != nil {
		return fmt.Errorf("release failed outbox: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrOwnershipLost
	}
	return nil
}

func (s *Store) ReconcileOverdue(ctx context.Context, grace time.Duration, limit int) (int64, error) {
	if grace < 0 || limit <= 0 {
		return 0, errors.New("non-negative overdue grace and positive limit are required")
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	tag, err := s.pool.Exec(ctx, `
		WITH candidates AS (
			SELECT id, schedule_revision, deliver_at, destination_type
			FROM deliveries
			WHERE status = 'scheduled'
			  AND deliver_at <= clock_timestamp() - $1 * interval '1 millisecond'
			ORDER BY deliver_at, id
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		INSERT INTO nats_outbox (
			delivery_id, schedule_revision, kind, deliver_at, destination_type
		)
		SELECT id, schedule_revision, 'ready', NULL, destination_type
		FROM candidates
		ON CONFLICT (delivery_id, schedule_revision, kind) DO NOTHING`,
		durationMillis(grace), limit)
	if err != nil {
		return 0, fmt.Errorf("reconcile overdue deliveries: %w", err)
	}
	return tag.RowsAffected(), nil
}

func (s *Store) ReapExpiredLeases(ctx context.Context, recoveryDelay time.Duration, limit int) (int64, error) {
	if recoveryDelay < 0 || limit <= 0 {
		return 0, errors.New("non-negative recovery delay and positive limit are required")
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	var count int64
	err := s.pool.QueryRow(ctx, `
		WITH expired AS MATERIALIZED (
			SELECT id
			FROM deliveries
			WHERE status = 'processing'
			  AND processing_until < clock_timestamp()
			ORDER BY processing_until, id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		), recovered AS (
			UPDATE deliveries d
			SET status = 'scheduled',
				schedule_revision = d.schedule_revision + 1,
				hot_registered_revision = NULL,
				processing_owner = NULL,
				processing_until = NULL,
				deliver_at = clock_timestamp() + $2 * interval '1 millisecond',
				last_error = 'processing lease expired',
				updated_at = clock_timestamp()
			FROM expired
			WHERE d.id = expired.id
			RETURNING d.id, d.schedule_revision, d.destination_type
		), inserted AS (
			INSERT INTO nats_outbox (
				delivery_id, schedule_revision, kind, deliver_at, destination_type
			)
			SELECT id, schedule_revision, 'ready', NULL, destination_type
			FROM recovered
			ON CONFLICT (delivery_id, schedule_revision, kind) DO NOTHING
		)
		SELECT count(*) FROM recovered`,
		limit, durationMillis(recoveryDelay)).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("reap processing leases: %w", err)
	}
	return count, nil
}

func (s *Store) CleanupRetention(
	ctx context.Context,
	terminalRetention time.Duration,
	outboxRetention time.Duration,
	limit int,
) (RetentionResult, error) {
	if terminalRetention < 0 || outboxRetention < 0 || limit <= 0 {
		return RetentionResult{}, errors.New("non-negative retention and positive limit are required")
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return RetentionResult{}, fmt.Errorf("begin retention cleanup: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var result RetentionResult
	err = tx.QueryRow(ctx, `
		WITH victims AS MATERIALIZED (
			SELECT id, payload_id
			FROM deliveries
			WHERE status IN ('delivered', 'cancelled', 'dead')
			  AND updated_at < clock_timestamp() - $1 * interval '1 millisecond'
			ORDER BY updated_at, id
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		), deleted_deliveries AS (
			DELETE FROM deliveries d
			USING victims v
			WHERE d.id = v.id
			RETURNING d.payload_id
		), deleted_payloads AS (
			DELETE FROM message_payloads p
			USING deleted_deliveries d
			WHERE p.id = d.payload_id
			  AND NOT EXISTS (SELECT 1 FROM deliveries x WHERE x.payload_id = p.id)
			RETURNING 1
		)
		SELECT
			(SELECT count(*) FROM deleted_deliveries),
			(SELECT count(*) FROM deleted_payloads)`,
		durationMillis(terminalRetention), limit).Scan(&result.Deliveries, &result.Payloads)
	if err != nil {
		return RetentionResult{}, fmt.Errorf("delete retained deliveries: %w", err)
	}
	tag, err := tx.Exec(ctx, `
		WITH victims AS (
			SELECT id
			FROM nats_outbox
			WHERE published_at IS NOT NULL
			  AND published_at < clock_timestamp() - $1 * interval '1 millisecond'
			ORDER BY published_at, id
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		DELETE FROM nats_outbox o USING victims v WHERE o.id = v.id`,
		durationMillis(outboxRetention), limit)
	if err != nil {
		return RetentionResult{}, fmt.Errorf("delete retained outbox: %w", err)
	}
	result.Outbox = tag.RowsAffected()
	if err := tx.Commit(ctx); err != nil {
		return RetentionResult{}, fmt.Errorf("commit retention cleanup: %w", err)
	}
	return result, nil
}
