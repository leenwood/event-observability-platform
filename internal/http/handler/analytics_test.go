package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/leenwood/event-observability-platform/internal/analytics"
)

// stubQuerier implements analyticsQuerier for unit tests.
type stubQuerier struct {
	stats []analytics.DailyEventStat
	err   error
}

func (s *stubQuerier) DailyEvents(_ context.Context, _, _ time.Time) ([]analytics.DailyEventStat, error) {
	return s.stats, s.err
}

func newAnalyticsHandler(q analyticsQuerier) *AnalyticsHandler {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return NewAnalyticsHandler(q, log)
}

func getAnalytics(t *testing.T, h *AnalyticsHandler, query string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/analytics/daily-events?"+query, nil)
	rr := httptest.NewRecorder()
	h.DailyEvents(rr, req)
	return rr
}

func TestAnalyticsHandler_MissingParams(t *testing.T) {
	h := newAnalyticsHandler(&stubQuerier{})

	cases := []struct{ name, query string }{
		{"missing both", ""},
		{"missing to", "from=2024-01-01"},
		{"missing from", "to=2024-01-31"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rr := getAnalytics(t, h, tc.query)
			if rr.Code != http.StatusBadRequest {
				t.Errorf("expected 400, got %d", rr.Code)
			}
		})
	}
}

func TestAnalyticsHandler_InvalidDateFormat(t *testing.T) {
	h := newAnalyticsHandler(&stubQuerier{})

	cases := []struct{ name, query string }{
		{"bad from", "from=01-01-2024&to=2024-01-31"},
		{"bad to", "from=2024-01-01&to=31-01-2024"},
		{"non-date from", "from=yesterday&to=2024-01-31"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rr := getAnalytics(t, h, tc.query)
			if rr.Code != http.StatusBadRequest {
				t.Errorf("expected 400, got %d", rr.Code)
			}
		})
	}
}

func TestAnalyticsHandler_ReversedRange(t *testing.T) {
	h := newAnalyticsHandler(&stubQuerier{})
	rr := getAnalytics(t, h, "from=2024-01-31&to=2024-01-01")
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for reversed range, got %d", rr.Code)
	}
}

func TestAnalyticsHandler_RangeTooLarge(t *testing.T) {
	h := newAnalyticsHandler(&stubQuerier{})
	from := "2024-01-01"
	to := time.Now().AddDate(1, 1, 0).Format("2006-01-02")
	rr := getAnalytics(t, h, fmt.Sprintf("from=%s&to=%s", from, to))
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for >366d range, got %d", rr.Code)
	}
}

func TestAnalyticsHandler_QueryError(t *testing.T) {
	h := newAnalyticsHandler(&stubQuerier{err: errors.New("ch down")})
	rr := getAnalytics(t, h, "from=2024-01-01&to=2024-01-31")
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 on query error, got %d", rr.Code)
	}
}

func TestAnalyticsHandler_EmptyResultIsArray(t *testing.T) {
	h := newAnalyticsHandler(&stubQuerier{stats: nil})
	rr := getAnalytics(t, h, "from=2024-01-01&to=2024-01-31")
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var result []analytics.DailyEventStat
	if err := json.NewDecoder(rr.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result == nil {
		t.Error("expected [] not null")
	}
	if len(result) != 0 {
		t.Errorf("expected empty array, got %d items", len(result))
	}
}

func TestAnalyticsHandler_ReturnsStats(t *testing.T) {
	date, _ := time.Parse("2006-01-02", "2024-01-15")
	stub := &stubQuerier{stats: []analytics.DailyEventStat{
		{Date: date, Source: "shopify", EventType: "order.shipped", EventCount: 42},
	}}

	h := newAnalyticsHandler(stub)
	rr := getAnalytics(t, h, "from=2024-01-01&to=2024-01-31")
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}

	var result []analytics.DailyEventStat
	if err := json.NewDecoder(rr.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 stat, got %d", len(result))
	}
	if result[0].EventCount != 42 {
		t.Errorf("expected event_count=42, got %d", result[0].EventCount)
	}
}
