-- +goose Up
CREATE TYPE delivery_status AS ENUM (
    'scheduled',
    'processing',
    'delivered',
    'cancelled',
    'dead'
);

CREATE TYPE nats_outbox_kind AS ENUM ('schedule', 'ready');

CREATE TABLE message_payloads (
    id UUID PRIMARY KEY,
    body BYTEA NOT NULL,
    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    content_type TEXT NOT NULL,
    size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE deliveries (
    id UUID PRIMARY KEY,
    idempotency_key TEXT,
    payload_id UUID NOT NULL REFERENCES message_payloads(id),
    destination_type TEXT NOT NULL
        CHECK (destination_type IN ('http', 'kafka', 'rabbit', 'postgres')),
    destination JSONB NOT NULL,
    deliver_at TIMESTAMPTZ NOT NULL,
    status delivery_status NOT NULL DEFAULT 'scheduled',
    schedule_revision BIGINT NOT NULL DEFAULT 1 CHECK (schedule_revision > 0),
    hot_registered_revision BIGINT,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts INTEGER NOT NULL CHECK (max_attempts > 0),
    processing_owner TEXT,
    processing_until TIMESTAMPTZ,
    last_error TEXT,
    last_attempt_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    cancelled_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (hot_registered_revision IS NULL OR hot_registered_revision <= schedule_revision)
);

CREATE TABLE nats_outbox (
    id BIGSERIAL PRIMARY KEY,
    delivery_id UUID NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
    schedule_revision BIGINT NOT NULL,
    kind nats_outbox_kind NOT NULL,
    deliver_at TIMESTAMPTZ,
    destination_type TEXT NOT NULL
        CHECK (destination_type IN ('http', 'kafka', 'rabbit', 'postgres')),
    available_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    locked_by TEXT,
    locked_until TIMESTAMPTZ,
    publish_attempts INTEGER NOT NULL DEFAULT 0,
    published_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (delivery_id, schedule_revision, kind)
);

-- +goose Down
DROP TABLE IF EXISTS nats_outbox;
DROP TABLE IF EXISTS deliveries;
DROP TABLE IF EXISTS message_payloads;
DROP TYPE IF EXISTS nats_outbox_kind;
DROP TYPE IF EXISTS delivery_status;
