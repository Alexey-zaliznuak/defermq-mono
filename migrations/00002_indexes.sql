-- +goose Up
CREATE UNIQUE INDEX deliveries_idempotency_key_uidx
    ON deliveries (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX deliveries_scheduler_idx
    ON deliveries (deliver_at, id)
    INCLUDE (schedule_revision, hot_registered_revision, destination_type)
    WHERE status = 'scheduled';

CREATE INDEX deliveries_processing_lease_idx
    ON deliveries (processing_until, id)
    WHERE status = 'processing';

CREATE INDEX deliveries_status_created_idx
    ON deliveries (status, created_at, id);

CREATE INDEX nats_outbox_pending_idx
    ON nats_outbox (available_at, id)
    WHERE published_at IS NULL;

CREATE INDEX nats_outbox_lock_idx
    ON nats_outbox (locked_until, id)
    WHERE published_at IS NULL;

-- +goose Down
DROP INDEX IF EXISTS nats_outbox_lock_idx;
DROP INDEX IF EXISTS nats_outbox_pending_idx;
DROP INDEX IF EXISTS deliveries_status_created_idx;
DROP INDEX IF EXISTS deliveries_processing_lease_idx;
DROP INDEX IF EXISTS deliveries_scheduler_idx;
DROP INDEX IF EXISTS deliveries_idempotency_key_uidx;
