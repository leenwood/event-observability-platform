-- +goose Up
CREATE TABLE IF NOT EXISTS webhook_events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key VARCHAR(255) NOT NULL,
    source          VARCHAR(100) NOT NULL,
    event_type      VARCHAR(100) NOT NULL,
    payload         JSONB        NOT NULL DEFAULT '{}',
    status          VARCHAR(20)  NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending','processing','processed','failed')),
    retry_count     INTEGER      NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    processed_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_webhook_events_idempotency_key ON webhook_events (idempotency_key);
CREATE INDEX IF NOT EXISTS idx_webhook_events_status          ON webhook_events (status);
CREATE INDEX IF NOT EXISTS idx_webhook_events_created_at      ON webhook_events (created_at DESC);

-- +goose Down
DROP TABLE IF EXISTS webhook_events;
