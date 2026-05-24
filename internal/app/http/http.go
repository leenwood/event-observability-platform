package http

import (
	"context"
	"fmt"
	"log/slog"
	nethttp "net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	httpSwagger "github.com/swaggo/http-swagger"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	_ "github.com/leenwood/event-observability-platform/docs/swagger"
	"github.com/leenwood/event-observability-platform/internal/app/http/handler"
	"github.com/leenwood/event-observability-platform/internal/app/http/middleware"
	"github.com/leenwood/event-observability-platform/internal/pkg/analytics"
	"github.com/leenwood/event-observability-platform/internal/pkg/domain"
	"github.com/leenwood/event-observability-platform/internal/pkg/idempotency"
	"github.com/leenwood/event-observability-platform/internal/pkg/platform/metrics"
)

const maxBodyBytes int64 = 1 << 20 // 1 MiB

// Config holds HTTP server listen and timeout parameters.
type Config struct {
	Host         string
	Port         int
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
	PprofEnabled bool
}

// Pinger is satisfied by any store that can verify its own connectivity.
type Pinger interface {
	Ping(ctx context.Context) error
}

// DailyEventsQuerier is satisfied by the ClickHouse analytics querier.
type DailyEventsQuerier interface {
	DailyEvents(ctx context.Context, from, to time.Time) ([]analytics.DailyEventStat, error)
}

// Deps groups all application-layer dependencies needed to wire the HTTP server.
type Deps struct {
	DB             Pinger
	EventRepo      domain.EventRepository
	IdemStore      idempotency.Store
	Publisher      domain.Publisher
	EventQuerier   DailyEventsQuerier
	Metrics        *metrics.Metrics
	Log            *slog.Logger
	EventsTopic    string
	IdempotencyTTL time.Duration
}

// NewServer wires all routes, handlers, and middleware and returns a configured server.
func NewServer(cfg Config, deps Deps) *nethttp.Server {
	mux := nethttp.NewServeMux()

	healthHandler := handler.NewHealthHandler(deps.DB)
	mux.HandleFunc("GET /health", healthHandler.Health)
	mux.HandleFunc("GET /ready", healthHandler.Ready)

	webhookHandler := handler.NewWebhookHandler(
		deps.EventRepo, deps.IdemStore, deps.Publisher, deps.EventsTopic,
		deps.Metrics, deps.Log, deps.IdempotencyTTL,
	)
	mux.HandleFunc("POST /webhooks/events", webhookHandler.HandleEvent)

	analyticsHandler := handler.NewAnalyticsHandler(deps.EventQuerier, deps.Log)
	mux.HandleFunc("GET /analytics/daily-events", analyticsHandler.DailyEvents)

	mux.Handle("GET /metrics", promhttp.HandlerFor(deps.Metrics.Registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))
	mux.Handle("/swagger/", httpSwagger.WrapHandler)

	if cfg.PprofEnabled {
		mux.HandleFunc("GET /debug/pprof/", pprof.Index)
		mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	}

	chain := otelhttp.NewHandler(
		middleware.Chain(
			mux,
			middleware.Recover(deps.Log),
			middleware.Logger(deps.Log, deps.Metrics),
			middleware.RequestID,
			middleware.MaxBodySize(maxBodyBytes),
		),
		"http.server",
		otelhttp.WithFilter(func(r *nethttp.Request) bool {
			p := r.URL.Path
			return p != "/metrics" && p != "/health"
		}),
	)

	return &nethttp.Server{
		Addr:         fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Handler:      chain,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}
}
