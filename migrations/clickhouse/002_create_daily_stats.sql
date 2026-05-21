-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS daily_event_stats
(
    date        Date,
    source      LowCardinality(String),
    event_type  LowCardinality(String),
    event_count UInt64
)
ENGINE = SummingMergeTree(event_count)
ORDER BY (date, source, event_type);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_event_stats_mv
TO daily_event_stats AS
SELECT
    date,
    source,
    event_type,
    count() AS event_count
FROM events_log
GROUP BY date, source, event_type;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP VIEW IF EXISTS daily_event_stats_mv;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS daily_event_stats;
-- +goose StatementEnd
