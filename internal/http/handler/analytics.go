package handler

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/leenwood/event-observability-platform/internal/analytics"
	"github.com/leenwood/event-observability-platform/internal/observability/logger"
)

const dateLayout = "2006-01-02"

type analyticsQuerier interface {
	DailyEvents(ctx context.Context, from, to time.Time) ([]analytics.DailyEventStat, error)
}

type AnalyticsHandler struct {
	querier analyticsQuerier
	log     *slog.Logger
}

func NewAnalyticsHandler(querier analyticsQuerier, log *slog.Logger) *AnalyticsHandler {
	return &AnalyticsHandler{querier: querier, log: log}
}

func (h *AnalyticsHandler) DailyEvents(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	log := logger.FromContext(ctx, h.log)

	fromStr := r.URL.Query().Get("from")
	toStr := r.URL.Query().Get("to")

	if fromStr == "" || toStr == "" {
		respondError(w, http.StatusBadRequest, "MISSING_PARAMS",
			"query parameters 'from' and 'to' are required (format: YYYY-MM-DD)")
		return
	}

	from, err := time.Parse(dateLayout, fromStr)
	if err != nil {
		respondError(w, http.StatusBadRequest, "INVALID_DATE",
			"'from' must be in YYYY-MM-DD format")
		return
	}

	to, err := time.Parse(dateLayout, toStr)
	if err != nil {
		respondError(w, http.StatusBadRequest, "INVALID_DATE",
			"'to' must be in YYYY-MM-DD format")
		return
	}

	if to.Before(from) {
		respondError(w, http.StatusBadRequest, "INVALID_RANGE",
			"'to' must be equal to or after 'from'")
		return
	}

	if to.Sub(from) > 366*24*time.Hour {
		respondError(w, http.StatusBadRequest, "RANGE_TOO_LARGE",
			"date range cannot exceed 366 days")
		return
	}

	stats, err := h.querier.DailyEvents(ctx, from, to)
	if err != nil {
		log.ErrorContext(ctx, "analytics query failed", slog.String("error", err.Error()))
		respondError(w, http.StatusInternalServerError, "QUERY_ERROR", "failed to query analytics")
		return
	}

	if stats == nil {
		stats = []analytics.DailyEventStat{}
	}

	writeJSON(w, http.StatusOK, stats)
}
