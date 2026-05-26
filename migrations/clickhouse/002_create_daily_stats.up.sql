-- Target table aggregated by Materialized View.
-- SummingMergeTree automatically sums event_count for rows
-- with the same ORDER BY key during background merges.
CREATE TABLE IF NOT EXISTS daily_event_stats
(
    date        Date,
    source      LowCardinality(String),
    event_type  LowCardinality(String),
    event_count UInt64
)
ENGINE = SummingMergeTree(event_count)
ORDER BY (date, source, event_type);

-- Materialized View: incremental population of daily_event_stats
-- on every INSERT into events_log.
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_event_stats_mv
TO daily_event_stats AS
SELECT
    date,
    source,
    event_type,
    count() AS event_count
FROM events_log
GROUP BY date, source, event_type;
