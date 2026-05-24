package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/leenwood/event-observability-platform/internal"
	apphttp "github.com/leenwood/event-observability-platform/internal/app/http"
	kafkaclient "github.com/leenwood/event-observability-platform/internal/pkg/messaging"
	"github.com/leenwood/event-observability-platform/internal/pkg/platform/logger"
	"github.com/leenwood/event-observability-platform/internal/pkg/platform/metrics"
	"github.com/leenwood/event-observability-platform/internal/pkg/platform/tracing"
	chstorage "github.com/leenwood/event-observability-platform/internal/pkg/storage/clickhouse"
	"github.com/leenwood/event-observability-platform/internal/pkg/storage/postgres"
)

// RunServer initialises all dependencies, starts the HTTP server, and blocks
// until ctx is cancelled or a fatal error occurs.
func RunServer(ctx context.Context) error {
	cfg, err := internal.Load()
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

	if cfg.HTTP.PprofEnabled {
		log.Info("pprof enabled", "path", "/debug/pprof/")
	}

	srv := apphttp.NewServer(apphttp.Config{
		Host:         cfg.HTTP.Host,
		Port:         cfg.HTTP.Port,
		ReadTimeout:  cfg.HTTP.ReadTimeout,
		WriteTimeout: cfg.HTTP.WriteTimeout,
		IdleTimeout:  cfg.HTTP.IdleTimeout,
		PprofEnabled: cfg.HTTP.PprofEnabled,
	}, apphttp.Deps{
		DB:             db,
		EventRepo:      eventRepo,
		IdemStore:      idemStore,
		Publisher:      producer,
		EventQuerier:   chstorage.NewEventQuerier(chDB),
		Metrics:        m,
		Log:            log,
		EventsTopic:    cfg.Kafka.TopicEvents,
		IdempotencyTTL: cfg.App.IdempotencyTTL,
	})

	srvErr := make(chan error, 1)
	go func() {
		log.Info("http starting", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			srvErr <- err
		}
	}()

	select {
	case err := <-srvErr:
		return fmt.Errorf("http error: %w", err)
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

	log.Info("http stopped")
	return nil
}
