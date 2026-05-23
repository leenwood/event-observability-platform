package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	httpSwagger "github.com/swaggo/http-swagger"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	_ "github.com/leenwood/event-observability-platform/docs/swagger"
	"github.com/leenwood/event-observability-platform/internal/app/server/handler"
	"github.com/leenwood/event-observability-platform/internal/app/server/middleware"
	"github.com/leenwood/event-observability-platform/internal/config"
	kafkaclient "github.com/leenwood/event-observability-platform/internal/pkg/messaging"
	"github.com/leenwood/event-observability-platform/internal/pkg/platform/logger"
	"github.com/leenwood/event-observability-platform/internal/pkg/platform/metrics"
	"github.com/leenwood/event-observability-platform/internal/pkg/platform/tracing"
	chstorage "github.com/leenwood/event-observability-platform/internal/pkg/storage/clickhouse"
	"github.com/leenwood/event-observability-platform/internal/pkg/storage/postgres"
)

const maxBodyBytes = 1 << 20 // 1 MiB

// RunServer initialises all dependencies, starts the HTTP server, and blocks
// until ctx is cancelled or a fatal error occurs.
func RunServer(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	log := logger.New(cfg.Log.Level, cfg.Log.Format)

	initCtx, initCancel := context.WithTimeout(ctx, 10*time.Second)
	defer initCancel()

	shutdownTracing, err := tracing.Init(initCtx, tracing.Config{
		Enabled:      cfg.OTel.Enabled,
		ServiceName:  cfg.OTel.ServiceName,
		ExporterType: cfg.OTel.ExporterType,
		Endpoint:     cfg.OTel.Endpoint,
	})
	if err != nil {
		return fmt.Errorf("init tracing: %w", err)
	}

	if cfg.OTel.Enabled {
		log.Info("opentelemetry tracing enabled", "exporter", cfg.OTel.ExporterType)
	}

	m := metrics.New()

	db, err := postgres.New(
		initCtx,
		cfg.Postgres.DSN,
		cfg.Postgres.MaxOpenConns,
		cfg.Postgres.MaxIdleConns,
		cfg.Postgres.ConnMaxLifetime,
	)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer db.Close()
	log.Info("connected to postgres")

	eventRepo := postgres.NewEventRepository(db)
	idemStore := postgres.NewIdempotencyStore(db)

	producer := kafkaclient.NewProducer(cfg.Kafka.Brokers)
	defer func() {
		if err := producer.Close(); err != nil {
			log.Error("producer close", "error", err)
		}
	}()

	chDB, err := chstorage.New(initCtx, chstorage.Config{
		Addr:     cfg.ClickHouse.Addr,
		Database: cfg.ClickHouse.Database,
		Username: cfg.ClickHouse.Username,
		Password: cfg.ClickHouse.Password,
	})
	if err != nil {
		return fmt.Errorf("connect clickhouse: %w", err)
	}
	defer func() {
		if err := chDB.Close(); err != nil {
			log.Error("clickhouse close", "error", err)
		}
	}()
	log.Info("connected to clickhouse")

	mux := http.NewServeMux()

	healthHandler := handler.NewHealthHandler(db)
	mux.HandleFunc("GET /health", healthHandler.Health)
	mux.HandleFunc("GET /ready", healthHandler.Ready)

	webhookHandler := handler.NewWebhookHandler(
		eventRepo, idemStore, producer, cfg.Kafka.TopicEvents,
		m, log, cfg.App.IdempotencyTTL,
	)
	mux.HandleFunc("POST /webhooks/events", webhookHandler.HandleEvent)

	analyticsHandler := handler.NewAnalyticsHandler(chstorage.NewEventQuerier(chDB), log)
	mux.HandleFunc("GET /analytics/daily-events", analyticsHandler.DailyEvents)

	mux.Handle("GET /metrics", promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))
	mux.Handle("/swagger/", httpSwagger.WrapHandler)

	if cfg.HTTP.PprofEnabled {
		mux.HandleFunc("GET /debug/pprof/", pprof.Index)
		mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
		log.Info("pprof enabled", "path", "/debug/pprof/")
	}

	chain := otelhttp.NewHandler(
		middleware.Chain(
			mux,
			middleware.Recover(log),
			middleware.Logger(log, m),
			middleware.RequestID,
			middleware.MaxBodySize(maxBodyBytes),
		),
		"http.server",
		otelhttp.WithMessageEvents(otelhttp.ReadEvents, otelhttp.WriteEvents),
	)

	addr := fmt.Sprintf("%s:%d", cfg.HTTP.Host, cfg.HTTP.Port)
	srv := &http.Server{
		Addr:         addr,
		Handler:      chain,
		ReadTimeout:  cfg.HTTP.ReadTimeout,
		WriteTimeout: cfg.HTTP.WriteTimeout,
		IdleTimeout:  cfg.HTTP.IdleTimeout,
	}

	srvErr := make(chan error, 1)
	go func() {
		log.Info("server starting", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			srvErr <- err
		}
	}()

	select {
	case err := <-srvErr:
		return fmt.Errorf("server error: %w", err)
	case <-ctx.Done():
	}

	log.Info("shutting down")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown error", "error", err)
	}

	if err := shutdownTracing(shutdownCtx); err != nil {
		log.Error("tracing shutdown error", "error", err)
	}

	log.Info("server stopped")
	return nil
}
