-- +goose Up
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key        VARCHAR(255) PRIMARY KEY,
    event_id   UUID         NOT NULL REFERENCES webhook_events (id) ON DELETE CASCADE,
    response   JSONB        NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ  NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_idempotency_keys_expires_at ON idempotency_keys (expires_at);

-- +goose Down
DROP TABLE IF EXISTS idempotency_keys;
