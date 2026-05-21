-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS events_log
(
    event_id     String,
    source       LowCardinality(String),
    event_type   LowCardinality(String),
    status       LowCardinality(String),
    processed_at DateTime,
    date         Date
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, source, event_type, event_id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS events_log;
-- +goose StatementEnd
