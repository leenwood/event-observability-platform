package clickhouse

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/leenwood/event-observability-platform/internal/pkg/analytics"
)

var queryTracer = otel.Tracer("storage/clickhouse/querier")

// EventQuerier reads aggregated analytics from ClickHouse.
type EventQuerier struct {
	db *DB
}

func NewEventQuerier(db *DB) *EventQuerier {
	return &EventQuerier{db: db}
}

// DailyEvents returns aggregated event counts per day, source and event_type
// for the requested date range. Reads from the daily_event_stats materialized view.
// SummingMergeTree may have unmerged parts — the sum() handles that correctly.
func (q *EventQuerier) DailyEvents(ctx context.Context, from, to time.Time) ([]analytics.DailyEventStat, error) {
	ctx, span := queryTracer.Start(ctx, "clickhouse.DailyEvents")
	defer span.End()

	span.SetAttributes(
		attribute.String("query.from", from.Format("2006-01-02")),
		attribute.String("query.to", to.Format("2006-01-02")),
	)

	const query = `
		SELECT
			date,
			source,
			event_type,
			sum(event_count) AS event_count
		FROM daily_event_stats
		WHERE date >= ? AND date <= ?
		GROUP BY date, source, event_type
		ORDER BY date ASC, source ASC, event_type ASC`

	rows, err := q.db.Conn().Query(ctx, query, from, to)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "query failed")
		return nil, fmt.Errorf("daily events query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var stats []analytics.DailyEventStat
	for rows.Next() {
		var s analytics.DailyEventStat
		if err := rows.Scan(&s.Date, &s.Source, &s.EventType, &s.EventCount); err != nil {
			return nil, fmt.Errorf("scan daily event stat: %w", err)
		}
		stats = append(stats, s)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate daily events: %w", err)
	}

	span.SetAttributes(attribute.Int("result.count", len(stats)))
	return stats, nil
}
