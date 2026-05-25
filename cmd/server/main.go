package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/leenwood/event-observability-platform/internal/config"
	"github.com/leenwood/event-observability-platform/internal/http/handler"
	"github.com/leenwood/event-observability-platform/internal/http/middleware"
	"github.com/leenwood/event-observability-platform/internal/idempotency"
	kafkaclient "github.com/leenwood/event-observability-platform/internal/integrations/kafka"
	"github.com/leenwood/event-observability-platform/internal/metrics"
	"github.com/leenwood/event-observability-platform/internal/observability/logger"
	"github.com/leenwood/event-observability-platform/internal/observability/tracing"
	"github.com/leenwood/event-observability-platform/internal/storage/postgres"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

const maxBodyBytes = 1 << 20 // 1 MiB

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	log := logger.New(cfg.Log.Level, cfg.Log.Format)

	initCtx, initCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer initCancel()

	shutdownTracing, err := tracing.Init(initCtx, tracing.Config{
		Enabled:      cfg.OTel.Enabled,
		ServiceName:  cfg.OTel.ServiceName,
		ExporterType: cfg.OTel.ExporterType,
		Endpoint:     cfg.OTel.Endpoint,
	})
	if err != nil {
		log.Error("failed to init tracing", "error", err)
		os.Exit(1)
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
		log.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	log.Info("connected to postgres")

	eventRepo := postgres.NewEventRepository(db)
	idemStore := idempotency.NewPostgresStore(db.Pool)

	producer := kafkaclient.NewProducer(cfg.Kafka.Brokers)
	defer producer.Close()

	mux := http.NewServeMux()

	healthHandler := handler.NewHealthHandler(db)
	mux.HandleFunc("GET /health", healthHandler.Health)
	mux.HandleFunc("GET /ready", healthHandler.Ready)

	webhookHandler := handler.NewWebhookHandler(
		eventRepo, idemStore, producer, cfg.Kafka.TopicEvents,
		m, log, cfg.App.IdempotencyTTL,
	)
	mux.HandleFunc("POST /webhooks/events", webhookHandler.HandleEvent)

	mux.Handle("GET /metrics", promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))

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

	go func() {
		log.Info("server starting", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("shutting down")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("shutdown error", "error", err)
	}

	if err := shutdownTracing(shutdownCtx); err != nil {
		log.Error("tracing shutdown error", "error", err)
	}

	log.Info("server stopped")
}
