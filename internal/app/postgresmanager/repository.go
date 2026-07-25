package postgresmanager

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/defermq/defermq/internal/hotstorage/natsjs"
	"github.com/defermq/defermq/internal/manager"
	"github.com/defermq/defermq/internal/observability"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	pool    *pgxpool.Pool
	metrics *observability.ManagerMetrics
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

func (r *Repository) SetMetrics(metrics *observability.ManagerMetrics) {
	r.metrics = metrics
}

func (r *Repository) Ping(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

func (r *Repository) PromotionCutoff(ctx context.Context, horizon time.Duration) (time.Time, error) {
	var cutoff time.Time
	err := r.pool.QueryRow(ctx,
		`SELECT clock_timestamp() + ($1::bigint * interval '1 microsecond')`,
		horizon.Microseconds(),
	).Scan(&cutoff)
	if r.metrics != nil {
		result := "success"
		if err != nil {
			result = "error"
		}
		r.metrics.PromoterCycles.WithLabelValues(result).Inc()
	}
	return cutoff.UTC(), err
}

func (r *Repository) PromoteBatch(ctx context.Context, cutoff time.Time, limit int) (manager.PromotionResult, error) {
	started := time.Now()
	const query = `
WITH candidates AS MATERIALIZED (
	SELECT id, schedule_revision, deliver_at, destination_type
	FROM deliveries
	WHERE status = 'scheduled'
	  AND deliver_at <= $1
	  AND hot_registered_revision IS DISTINCT FROM schedule_revision
	  AND NOT EXISTS (
		SELECT 1 FROM nats_outbox AS existing
		WHERE existing.delivery_id = deliveries.id
		  AND existing.schedule_revision = deliveries.schedule_revision
		  AND existing.kind = 'schedule'
	  )
	ORDER BY deliver_at, id
	LIMIT $2
	FOR UPDATE SKIP LOCKED
), inserted AS (
	INSERT INTO nats_outbox (delivery_id, schedule_revision, kind, deliver_at, destination_type)
	SELECT id, schedule_revision, 'schedule', deliver_at, destination_type
	FROM candidates
	ON CONFLICT (delivery_id, schedule_revision, kind) DO NOTHING
	RETURNING 1
)
SELECT (SELECT count(*) FROM candidates), (SELECT count(*) FROM inserted)`
	var result manager.PromotionResult
	err := r.pool.QueryRow(ctx, query, cutoff.UTC(), limit).Scan(&result.Candidates, &result.Created)
	if err == nil && r.metrics != nil {
		r.metrics.PromoterBatchDuration.Observe(time.Since(started).Seconds())
		r.metrics.PromoterBatches.WithLabelValues(strconv.FormatBool(result.Candidates == limit)).Inc()
		r.metrics.PromoterBatchSize.Observe(float64(result.Candidates))
		r.metrics.PromotedMessages.Add(float64(result.Created))
	}
	return result, err
}

func (r *Repository) ClaimOutbox(
	ctx context.Context,
	workerID string,
	limit int,
	lockTTL time.Duration,
) ([]manager.OutboxRecord, error) {
	const query = `
WITH picked AS (
	SELECT o.id, d.deliver_at AS delivery_deliver_at
	FROM nats_outbox AS o
	JOIN deliveries AS d ON d.id = o.delivery_id
	WHERE o.published_at IS NULL
	  AND o.available_at <= clock_timestamp()
	  AND (o.locked_until IS NULL OR o.locked_until < clock_timestamp())
	ORDER BY o.available_at, o.id
	LIMIT $1
	FOR UPDATE OF o SKIP LOCKED
)
UPDATE nats_outbox AS o
SET locked_by = $2,
	locked_until = clock_timestamp() + ($3::bigint * interval '1 microsecond'),
	publish_attempts = publish_attempts + 1
FROM picked
WHERE o.id = picked.id
RETURNING o.id, o.delivery_id, o.schedule_revision, o.kind::text,
	COALESCE(o.deliver_at, picked.delivery_deliver_at), o.destination_type, o.publish_attempts`
	rows, err := r.pool.Query(ctx, query, limit, workerID, lockTTL.Microseconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	records := make([]manager.OutboxRecord, 0, limit)
	for rows.Next() {
		var record manager.OutboxRecord
		var kind string
		if err := rows.Scan(
			&record.ID,
			&record.DeliveryID,
			&record.ScheduleRevision,
			&kind,
			&record.DeliverAt,
			&record.DestinationType,
			&record.PublishAttempts,
		); err != nil {
			return nil, err
		}
		record.Kind = natsjs.OutboxKind(kind)
		record.LockedBy = workerID
		record.DeliverAt = record.DeliverAt.UTC()
		records = append(records, record)
	}
	if r.metrics != nil {
		for _, record := range records {
			r.metrics.OutboxClaimed.WithLabelValues(string(record.Kind)).Inc()
		}
	}
	return records, rows.Err()
}

func (r *Repository) MarkOutboxPublished(ctx context.Context, record manager.OutboxRecord) error {
	return pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
UPDATE nats_outbox
SET published_at = clock_timestamp(), locked_by = NULL, locked_until = NULL, last_error = NULL
WHERE id = $1 AND locked_by = $2 AND published_at IS NULL`, record.ID, record.LockedBy)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("outbox %d ownership lost", record.ID)
		}
		if record.Kind == natsjs.OutboxSchedule {
			_, err = tx.Exec(ctx, `
UPDATE deliveries
SET hot_registered_revision = $2, updated_at = clock_timestamp()
WHERE id = $1 AND schedule_revision = $2 AND status = 'scheduled'`,
				record.DeliveryID, record.ScheduleRevision)
		}
		if r.metrics != nil {
			r.metrics.OutboxPublished.WithLabelValues(string(record.Kind), "success").Inc()
		}
		return err
	})
}

func (r *Repository) MarkOutboxFailed(
	ctx context.Context,
	record manager.OutboxRecord,
	delay time.Duration,
	message string,
) error {
	tag, err := r.pool.Exec(ctx, `
UPDATE nats_outbox
SET available_at = clock_timestamp() + ($3::bigint * interval '1 microsecond'),
	locked_by = NULL, locked_until = NULL, last_error = $4
WHERE id = $1 AND locked_by = $2 AND published_at IS NULL`,
		record.ID, record.LockedBy, delay.Microseconds(), message)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("outbox %d ownership lost", record.ID)
	}
	if r.metrics != nil {
		r.metrics.OutboxPublished.WithLabelValues(string(record.Kind), "failure").Inc()
		r.metrics.OutboxPublishRetries.WithLabelValues(string(record.Kind)).Inc()
	}
	return nil
}

func (r *Repository) ReconcileOverdue(ctx context.Context, grace time.Duration, limit int) (int, error) {
	const query = `
WITH candidates AS MATERIALIZED (
	SELECT id, schedule_revision, deliver_at, destination_type
	FROM deliveries
	WHERE status = 'scheduled'
	  AND deliver_at <= clock_timestamp() - ($1::bigint * interval '1 microsecond')
	  AND NOT EXISTS (
		SELECT 1 FROM nats_outbox AS existing
		WHERE existing.delivery_id = deliveries.id
		  AND existing.schedule_revision = deliveries.schedule_revision
		  AND existing.kind = 'ready'
	  )
	ORDER BY deliver_at, id
	LIMIT $2
	FOR UPDATE SKIP LOCKED
), inserted AS (
	INSERT INTO nats_outbox (delivery_id, schedule_revision, kind, deliver_at, destination_type)
	SELECT id, schedule_revision, 'ready', deliver_at, destination_type
	FROM candidates
	ON CONFLICT (delivery_id, schedule_revision, kind) DO NOTHING
	RETURNING 1
)
SELECT count(*) FROM inserted`
	var count int
	err := r.pool.QueryRow(ctx, query, grace.Microseconds(), limit).Scan(&count)
	if err == nil && r.metrics != nil {
		r.metrics.OverdueReconciled.Add(float64(count))
	}
	return count, err
}

func (r *Repository) ReapExpiredProcessing(ctx context.Context, recoveryDelay time.Duration, limit int) (int, error) {
	const query = `
WITH candidates AS MATERIALIZED (
	SELECT id
	FROM deliveries
	WHERE status = 'processing' AND processing_until < clock_timestamp()
	ORDER BY processing_until, id
	LIMIT $1
	FOR UPDATE SKIP LOCKED
), recovered AS (
	UPDATE deliveries AS d
	SET status = 'scheduled',
		schedule_revision = d.schedule_revision + 1,
		hot_registered_revision = NULL,
		processing_owner = NULL,
		processing_until = NULL,
		deliver_at = clock_timestamp() + ($2::bigint * interval '1 microsecond'),
		last_error = 'processing lease expired',
		updated_at = clock_timestamp()
	FROM candidates
	WHERE d.id = candidates.id
	RETURNING d.id, d.schedule_revision, d.deliver_at, d.destination_type
), inserted AS (
	INSERT INTO nats_outbox (delivery_id, schedule_revision, kind, deliver_at, destination_type)
	SELECT id, schedule_revision, 'ready', deliver_at, destination_type
	FROM recovered
	ON CONFLICT (delivery_id, schedule_revision, kind) DO NOTHING
	RETURNING 1
)
SELECT count(*) FROM recovered`
	var count int
	err := r.pool.QueryRow(ctx, query, limit, recoveryDelay.Microseconds()).Scan(&count)
	if err == nil && r.metrics != nil {
		r.metrics.ProcessingLeasesReaped.Add(float64(count))
	}
	return count, err
}

func (r *Repository) DeleteTerminal(ctx context.Context, retention time.Duration, limit int) (int, int, error) {
	var deliveries, payloads int
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
WITH candidates AS (
	SELECT id
	FROM deliveries
	WHERE status IN ('delivered', 'cancelled', 'dead')
	  AND updated_at < clock_timestamp() - ($1::bigint * interval '1 microsecond')
	ORDER BY updated_at, id
	LIMIT $2
	FOR UPDATE SKIP LOCKED
)
DELETE FROM deliveries AS d
USING candidates
WHERE d.id = candidates.id
RETURNING d.payload_id`, retention.Microseconds(), limit)
		if err != nil {
			return err
		}
		var payloadIDs []uuid.UUID
		for rows.Next() {
			var id uuid.UUID
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			payloadIDs = append(payloadIDs, id)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		deliveries = len(payloadIDs)
		if len(payloadIDs) == 0 {
			return nil
		}
		tag, err := tx.Exec(ctx, `
DELETE FROM message_payloads AS p
WHERE p.id = ANY($1)
  AND NOT EXISTS (SELECT 1 FROM deliveries AS d WHERE d.payload_id = p.id)`, payloadIDs)
		if err != nil {
			return err
		}
		payloads = int(tag.RowsAffected())
		return nil
	})
	if err == nil && r.metrics != nil {
		r.metrics.RetentionDeleted.WithLabelValues("delivery").Add(float64(deliveries))
		r.metrics.RetentionDeleted.WithLabelValues("payload").Add(float64(payloads))
	}
	return deliveries, payloads, err
}

func (r *Repository) DeletePublishedOutbox(ctx context.Context, retention time.Duration, limit int) (int, error) {
	tag, err := r.pool.Exec(ctx, `
WITH candidates AS (
	SELECT id
	FROM nats_outbox
	WHERE published_at < clock_timestamp() - ($1::bigint * interval '1 microsecond')
	ORDER BY published_at, id
	LIMIT $2
)
DELETE FROM nats_outbox AS o
USING candidates
WHERE o.id = candidates.id`, retention.Microseconds(), limit)
	if err != nil {
		return 0, err
	}
	deleted := int(tag.RowsAffected())
	if r.metrics != nil {
		r.metrics.RetentionDeleted.WithLabelValues("outbox").Add(float64(deleted))
	}
	return deleted, nil
}

var (
	_ manager.PromoterRepository         = (*Repository)(nil)
	_ manager.OutboxRepository           = (*Repository)(nil)
	_ manager.OverdueRepository          = (*Repository)(nil)
	_ manager.ProcessingReaperRepository = (*Repository)(nil)
	_ manager.RetentionRepository        = (*Repository)(nil)
)

func validateRepository(pool *pgxpool.Pool) error {
	if pool == nil {
		return errors.New("PostgreSQL pool is nil")
	}
	return nil
}
