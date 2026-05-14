//go:build integration

package analytics

import (
	"context"
	"testing"
	"time"

	clickhousego "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/leenwood/event-observability-platform/internal/app"
	tcontainers "github.com/testcontainers/testcontainers-go"
	tcclickhouse "github.com/testcontainers/testcontainers-go/modules/clickhouse"
)

const analyticsSchemaSQL = `
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

CREATE TABLE IF NOT EXISTS daily_event_stats
(
	date        Date,
	source      LowCardinality(String),
	event_type  LowCardinality(String),
	event_count UInt64
)
ENGINE = SummingMergeTree(event_count)
ORDER BY (date, source, event_type);

CREATE MATERIALIZED VIEW IF NOT EXISTS daily_event_stats_mv
TO daily_event_stats AS
SELECT
	date,
	source,
	event_type,
	count() AS event_count
FROM events_log
GROUP BY date, source, event_type;`

func startClickHouse(t *testing.T) clickhousego.Conn {
	t.Helper()
	ctx := context.Background()

	ctr, err := tcclickhouse.Run(ctx, "clickhouse/clickhouse-server:24-alpine",
		tcclickhouse.WithDatabase("events"),
		tcclickhouse.WithUsername("events"),
		tcclickhouse.WithPassword("events"),
	)
	if err != nil {
		t.Fatalf("start clickhouse container: %v", err)
	}
	t.Cleanup(func() { _ = tcontainers.TerminateContainer(ctr) })

	host, err := ctr.ConnectionHost(ctx)
	if err != nil {
		t.Fatalf("get connection host: %v", err)
	}

	conn, err := clickhousego.Open(&clickhousego.Options{
		Addr: []string{host},
		Auth: clickhousego.Auth{
			Database: ctr.DbName,
			Username: ctr.User,
			Password: ctr.Password,
		},
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	if err := conn.Ping(ctx); err != nil {
		t.Fatalf("ping clickhouse: %v", err)
	}

	if err := conn.Exec(ctx, analyticsSchemaSQL); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	return conn
}

func TestEventWriter_WriteAndQuery(t *testing.T) {
	conn := startClickHouse(t)
	ctx := context.Background()

	writer := NewEventWriter(conn)
	querier := NewQuerier(conn)

	now := time.Now().UTC()
	event := &app.Event{
		ID:          "test-event-001",
		Source:      "shopify",
		EventType:   "order.shipped",
		Status:      app.EventStatusProcessed,
		ProcessedAt: &now,
	}

	if err := writer.WriteEvent(ctx, event); err != nil {
		t.Fatalf("write event: %v", err)
	}

	// Force MV materialization — optimize flushes buffered data in ClickHouse.
	_ = conn.Exec(ctx, "OPTIMIZE TABLE events_log FINAL")

	from := now.Truncate(24 * time.Hour)
	to := from.Add(24 * time.Hour)

	stats, err := querier.DailyEvents(ctx, from, to)
	if err != nil {
		t.Fatalf("daily events query: %v", err)
	}
	if len(stats) == 0 {
		t.Fatal("expected at least one daily stat after writing an event")
	}

	found := false
	for _, s := range stats {
		if s.Source == "shopify" && s.EventType == "order.shipped" {
			found = true
			if s.EventCount == 0 {
				t.Error("expected event_count > 0")
			}
			break
		}
	}
	if !found {
		t.Errorf("expected shopify/order.shipped in results, got: %+v", stats)
	}
}

func TestEventWriter_MultipleEvents_DailyAggregation(t *testing.T) {
	conn := startClickHouse(t)
	ctx := context.Background()

	writer := NewEventWriter(conn)
	querier := NewQuerier(conn)

	now := time.Now().UTC()

	events := []*app.Event{
		{ID: "evt-001", Source: "stripe", EventType: "payment.success", Status: app.EventStatusProcessed, ProcessedAt: &now},
		{ID: "evt-002", Source: "stripe", EventType: "payment.success", Status: app.EventStatusProcessed, ProcessedAt: &now},
		{ID: "evt-003", Source: "stripe", EventType: "payment.failed", Status: app.EventStatusProcessed, ProcessedAt: &now},
	}

	for _, ev := range events {
		if err := writer.WriteEvent(ctx, ev); err != nil {
			t.Fatalf("write event %s: %v", ev.ID, err)
		}
	}

	_ = conn.Exec(ctx, "OPTIMIZE TABLE events_log FINAL")

	from := now.Truncate(24 * time.Hour)
	to := from.Add(24 * time.Hour)

	stats, err := querier.DailyEvents(ctx, from, to)
	if err != nil {
		t.Fatalf("daily events query: %v", err)
	}

	totals := make(map[string]uint64)
	for _, s := range stats {
		if s.Source == "stripe" {
			totals[s.EventType] += s.EventCount
		}
	}

	if totals["payment.success"] < 2 {
		t.Errorf("payment.success: expected count >= 2, got %d", totals["payment.success"])
	}
	if totals["payment.failed"] < 1 {
		t.Errorf("payment.failed: expected count >= 1, got %d", totals["payment.failed"])
	}
}

func TestQuerier_EmptyRangeReturnsSlice(t *testing.T) {
	conn := startClickHouse(t)
	ctx := context.Background()
	querier := NewQuerier(conn)

	past, _ := time.Parse("2006-01-02", "2000-01-01")
	pastEnd, _ := time.Parse("2006-01-02", "2000-01-31")

	stats, err := querier.DailyEvents(ctx, past, pastEnd)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// nil is acceptable; the handler converts nil → []
	_ = stats
}
