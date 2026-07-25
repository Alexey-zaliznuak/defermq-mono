package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/defermq/defermq/internal/ingest"
	"github.com/jackc/pgx/v5"
)

// ApplyIngestBatch commits a fetched JetStream batch atomically. Every
// operation is safe to replay after a lost consumer ACK.
func (s *Store) ApplyIngestBatch(ctx context.Context, commands []ingest.Command) error {
	if len(commands) == 0 {
		return nil
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin ingest batch: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows := make([][]any, 0, len(commands))
	for index, command := range commands {
		if err := command.Validate(); err != nil {
			return err
		}
		var headers []byte
		var body []byte
		var contentType string
		var sizeBytes int64
		if command.Payload != nil {
			headers, err = json.Marshal(command.Payload.Headers)
			if err != nil {
				return fmt.Errorf("encode ingest payload headers: %w", err)
			}
			body = command.Payload.Body
			contentType = command.Payload.ContentType
			sizeBytes = command.Payload.SizeBytes
		}
		rows = append(rows, []any{
			index, string(command.Kind), command.DeliveryID, command.PayloadID,
			command.IdempotencyKey, command.DestinationType, command.Destination,
			command.DeliverAt.UTC(), command.MaxAttempts, body, headers,
			contentType, sizeBytes, durationMillis(command.HotHorizon),
			command.ExpectedRevision,
		})
	}
	if _, err := tx.Exec(ctx, `
		CREATE TEMP TABLE ingest_batch (
			ord integer NOT NULL, kind text NOT NULL, delivery_id uuid NOT NULL,
			payload_id uuid, idempotency_key text, destination_type text,
			destination jsonb, deliver_at timestamptz, max_attempts integer,
			body bytea, headers jsonb, content_type text, size_bytes bigint,
			hot_horizon_ms bigint, expected_revision bigint
		) ON COMMIT DROP`); err != nil {
		return fmt.Errorf("create ingest staging table: %w", err)
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"ingest_batch"}, []string{
		"ord", "kind", "delivery_id", "payload_id", "idempotency_key",
		"destination_type", "destination", "deliver_at", "max_attempts",
		"body", "headers", "content_type", "size_bytes", "hot_horizon_ms",
		"expected_revision",
	}, pgx.CopyFromRows(rows)); err != nil {
		return fmt.Errorf("copy ingest batch: %w", err)
	}
	var databaseNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&databaseNow); err != nil {
		return fmt.Errorf("read database time for ingest batch: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		WITH payload_rows AS (
			SELECT DISTINCT ON (payload_id)
				payload_id, body, headers, content_type, size_bytes
			FROM ingest_batch
			WHERE kind = 'create'
			ORDER BY payload_id, ord
		), inserted_payloads AS (
			INSERT INTO message_payloads (id, body, headers, content_type, size_bytes)
			SELECT payload_id, body, headers, content_type, size_bytes
			FROM payload_rows
			ON CONFLICT (id) DO NOTHING
			RETURNING id
		), delivery_rows AS (
			SELECT DISTINCT ON (delivery_id)
				delivery_id, idempotency_key, payload_id, destination_type,
				destination, deliver_at, max_attempts, hot_horizon_ms
			FROM ingest_batch
			WHERE kind = 'create'
			ORDER BY delivery_id, ord
		), inserted_deliveries AS (
			INSERT INTO deliveries (
				id, idempotency_key, payload_id, destination_type, destination,
				deliver_at, max_attempts
			)
			SELECT delivery_id, idempotency_key, payload_id, destination_type,
				destination, deliver_at, max_attempts
			FROM delivery_rows
			ON CONFLICT DO NOTHING
			RETURNING id, deliver_at, destination_type, schedule_revision
		)
		INSERT INTO nats_outbox (
			delivery_id, schedule_revision, kind, deliver_at, destination_type
		)
		SELECT d.id, d.schedule_revision,
			CASE WHEN d.deliver_at <= $1 THEN 'ready'::nats_outbox_kind
			     ELSE 'hot_register'::nats_outbox_kind END,
			CASE WHEN d.deliver_at <= $1 THEN NULL ELSE d.deliver_at END,
			d.destination_type
		FROM inserted_deliveries d
		JOIN delivery_rows r ON r.delivery_id = d.id
		WHERE d.deliver_at <= $1 + r.hot_horizon_ms * interval '1 millisecond'
		ON CONFLICT (delivery_id, schedule_revision, kind) DO NOTHING`,
		databaseNow.UTC()); err != nil {
		return fmt.Errorf("insert ingest create batch: %w", err)
	}
	for _, command := range commands {
		switch command.Kind {
		case ingest.KindCancel:
			if _, err := tx.Exec(ctx, `
				UPDATE deliveries
				SET status = 'cancelled', schedule_revision = schedule_revision + 1,
					hot_registered_revision = NULL, cancelled_at = $3, updated_at = $3
				WHERE id = $1 AND status = 'scheduled' AND schedule_revision = $2`,
				command.DeliveryID, command.ExpectedRevision, databaseNow.UTC()); err != nil {
				return fmt.Errorf("apply cancel command: %w", err)
			}
		case ingest.KindReschedule:
			if err := applyRescheduleCommand(ctx, tx, command, databaseNow); err != nil {
				return err
			}
		}
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM message_payloads p
		USING (SELECT DISTINCT payload_id FROM ingest_batch WHERE kind = 'create') staged
		WHERE p.id = staged.payload_id
		  AND NOT EXISTS (SELECT 1 FROM deliveries d WHERE d.payload_id = p.id)`); err != nil {
		return fmt.Errorf("clean orphan ingest payloads: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit ingest batch: %w", err)
	}
	return nil
}

func applyRescheduleCommand(ctx context.Context, tx pgx.Tx, command ingest.Command, databaseNow time.Time) error {
	if _, err := tx.Exec(ctx, `
		WITH changed AS (
			UPDATE deliveries
			SET deliver_at = $2, schedule_revision = schedule_revision + 1,
				hot_registered_revision = NULL, updated_at = $5
			WHERE id = $1 AND status = 'scheduled'
			  AND schedule_revision = $3 AND deliver_at IS DISTINCT FROM $2
			RETURNING id, schedule_revision, destination_type, deliver_at
		)
		INSERT INTO nats_outbox (
			delivery_id, schedule_revision, kind, deliver_at, destination_type
		)
		SELECT id, schedule_revision,
			CASE WHEN deliver_at <= $5 THEN 'ready'::nats_outbox_kind
			     ELSE 'hot_register'::nats_outbox_kind END,
			CASE WHEN deliver_at <= $5 THEN NULL ELSE deliver_at END,
			destination_type
		FROM changed
		WHERE deliver_at <= $5 + $4 * interval '1 millisecond'
		ON CONFLICT (delivery_id, schedule_revision, kind) DO NOTHING`,
		command.DeliveryID, command.DeliverAt.UTC(), command.ExpectedRevision,
		durationMillis(command.HotHorizon), databaseNow.UTC()); err != nil {
		return fmt.Errorf("apply reschedule command: %w", err)
	}
	return nil
}
