package service

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/leenwood/event-observability-platform/internal"
	"github.com/leenwood/event-observability-platform/internal/pkg/messaging"
	"github.com/leenwood/event-observability-platform/internal/pkg/platform/logger"
	"github.com/leenwood/event-observability-platform/internal/pkg/platform/metrics"
	"github.com/leenwood/event-observability-platform/internal/pkg/platform/tracing"
	chstorage "github.com/leenwood/event-observability-platform/internal/pkg/storage/clickhouse"
	"github.com/leenwood/event-observability-platform/internal/pkg/storage/postgres"
)

// Infra holds all shared infrastructure dependencies initialised at startup.
type Infra struct {
	Cfg      *internal.Config
	Log      *slog.Logger
	Metrics  *metrics.Metrics
	DB       *postgres.DB
	ChDB     *chstorage.DB
	Producer *messaging.Producer

	shutdownTracing func(context.Context) error
}

// initInfra loads config and initialises tracing, databases, and Kafka producer.
// serviceNameSuffix is appended to cfg.OTel.ServiceName (pass "" for the server,
// "-worker" for the worker binary).
func initInfra(ctx context.Context, serviceNameSuffix string) (*Infra, error) {
	cfg, err := internal.Load()
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}

	log := logger.New(cfg.Log.Level, cfg.Log.Format)

	initCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	serviceName := cfg.OTel.ServiceName + serviceNameSuffix
	shutdownTracing, err := tracing.Init(initCtx, tracing.Config{
		Enabled:      cfg.OTel.Enabled,
		ServiceName:  serviceName,
		ExporterType: cfg.OTel.ExporterType,
		Endpoint:     cfg.OTel.Endpoint,
	})
	if err != nil {
		return nil, fmt.Errorf("init tracing: %w", err)
	}

	if cfg.OTel.Enabled {
		log.Info("opentelemetry tracing enabled", "service", serviceName, "exporter", cfg.OTel.ExporterType)
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
		_ = shutdownTracing(initCtx)
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	log.Info("connected to postgres")

	chDB, err := chstorage.New(initCtx, chstorage.Config{
		Addr:     cfg.ClickHouse.Addr,
		Database: cfg.ClickHouse.Database,
		Username: cfg.ClickHouse.Username,
		Password: cfg.ClickHouse.Password,
	})
	if err != nil {
		db.Close()
		_ = shutdownTracing(initCtx)
		return nil, fmt.Errorf("connect clickhouse: %w", err)
	}
	log.Info("connected to clickhouse")

	return &Infra{
		Cfg:             cfg,
		Log:             log,
		Metrics:         m,
		DB:              db,
		ChDB:            chDB,
		Producer:        messaging.NewProducer(cfg.Kafka.Brokers),
		shutdownTracing: shutdownTracing,
	}, nil
}

// Shutdown drains the Kafka producer, closes storage connections, and flushes
// tracing spans. Must be called with a context that has a valid deadline.
func (i *Infra) Shutdown(ctx context.Context) {
	if err := i.Producer.Close(); err != nil {
		i.Log.Error("producer close", "error", err)
	}
	if err := i.ChDB.Close(); err != nil {
		i.Log.Error("clickhouse close", "error", err)
	}
	i.DB.Close()
	if err := i.shutdownTracing(ctx); err != nil {
		i.Log.Error("tracing shutdown error", "error", err)
	}
}
