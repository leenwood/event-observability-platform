package analytics

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

var queryTracer = otel.Tracer("analytics/query")

type DailyEventStat struct {
	Date       time.Time `json:"date"`
	Source     string    `json:"source"`
	EventType  string    `json:"event_type"`
	EventCount uint64    `json:"event_count"`
}

type Querier struct {
	conn driver.Conn
}

func NewQuerier(conn driver.Conn) *Querier {
	return &Querier{conn: conn}
}

// DailyEvents returns aggregated event counts per day, source and event_type
// for the requested date range. Reads from the daily_event_stats materialized view.
// SummingMergeTree may have unmerged parts — the sum() handles that correctly.
func (q *Querier) DailyEvents(ctx context.Context, from, to time.Time) ([]DailyEventStat, error) {
	ctx, span := queryTracer.Start(ctx, "analytics.DailyEvents")
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

	rows, err := q.conn.Query(ctx, query, from, to)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "query failed")
		return nil, fmt.Errorf("daily events query: %w", err)
	}
	defer rows.Close()

	var stats []DailyEventStat
	for rows.Next() {
		var s DailyEventStat
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
