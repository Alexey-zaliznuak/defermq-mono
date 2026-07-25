ALTER TYPE nats_outbox_kind RENAME VALUE 'schedule' TO 'hot_register';

ALTER TABLE deliveries
    ADD COLUMN ready_published_revision BIGINT,
    ADD COLUMN ready_published_at TIMESTAMPTZ,
    ADD CONSTRAINT deliveries_ready_revision_check
        CHECK (
            ready_published_revision IS NULL
            OR ready_published_revision <= schedule_revision
        ),
    ADD CONSTRAINT deliveries_ready_published_pair_check
        CHECK (
            (ready_published_revision IS NULL) = (ready_published_at IS NULL)
        );

CREATE INDEX deliveries_hot_repair_idx
    ON deliveries (deliver_at, id)
    WHERE status = 'scheduled';
