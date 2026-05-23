package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/leenwood/event-observability-platform/internal/app/processor"
	"github.com/leenwood/event-observability-platform/internal/config"
	"github.com/leenwood/event-observability-platform/internal/pkg/messaging"
	"github.com/leenwood/event-observability-platform/internal/pkg/platform/logger"
	"github.com/leenwood/event-observability-platform/internal/pkg/platform/metrics"
	"github.com/leenwood/event-observability-platform/internal/pkg/platform/tracing"
	chstorage "github.com/leenwood/event-observability-platform/internal/pkg/storage/clickhouse"
	"github.com/leenwood/event-observability-platform/internal/pkg/storage/postgres"
)

// RunWorker initialises all dependencies, starts Processor and DLQHandler
// goroutines, and blocks until ctx is cancelled.
func RunWorker(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	log := logger.New(cfg.Log.Level, cfg.Log.Format)

	initCtx, initCancel := context.WithTimeout(ctx, 10*time.Second)
	defer initCancel()

	shutdownTracing, err := tracing.Init(initCtx, tracing.Config{
		Enabled:      cfg.OTel.Enabled,
		ServiceName:  cfg.OTel.ServiceName + "-worker",
		ExporterType: cfg.OTel.ExporterType,
		Endpoint:     cfg.OTel.Endpoint,
	})
	if err != nil {
		return fmt.Errorf("init tracing: %w", err)
	}

	m := metrics.New()

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

	eventRepo := postgres.NewEventRepository(db)

	producer := messaging.NewProducer(cfg.Kafka.Brokers)
	defer func() {
		if err := producer.Close(); err != nil {
			log.Error("producer close", "error", err)
		}
	}()

	eventsConsumer := messaging.NewConsumer(
		cfg.Kafka.Brokers,
		cfg.Kafka.TopicEvents,
		cfg.Kafka.ConsumerGroup,
	)
	defer func() {
		if err := eventsConsumer.Close(); err != nil {
			log.Error("events consumer close", "error", err)
		}
	}()

	dlqConsumer := messaging.NewConsumer(
		cfg.Kafka.Brokers,
		cfg.Kafka.TopicDLQ,
		cfg.Kafka.ConsumerGroup+"-dlq",
	)
	defer func() {
		if err := dlqConsumer.Close(); err != nil {
			log.Error("dlq consumer close", "error", err)
		}
	}()

	proc := processor.NewProcessor(
		eventsConsumer,
		producer,
		eventRepo,
		chstorage.NewEventWriter(chDB),
		m,
		log,
		processor.Topics{
			Events: cfg.Kafka.TopicEvents,
			DLQ:    cfg.Kafka.TopicDLQ,
		},
		cfg.Kafka.MaxRetries,
	)

	dlqHandler := processor.NewDLQHandler(dlqConsumer, eventRepo, log)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Info("event processor started", "topic", cfg.Kafka.TopicEvents)
		if err := proc.Run(ctx); err != nil {
			log.Error("processor error", "error", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Info("dlq handler started", "topic", cfg.Kafka.TopicDLQ)
		if err := dlqHandler.Run(ctx); err != nil {
			log.Error("dlq handler error", "error", err)
		}
	}()

	<-ctx.Done()
	log.Info("worker shutting down")

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Info("worker stopped cleanly")
	case <-time.After(30 * time.Second):
		log.Warn("worker shutdown timed out")
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer shutdownCancel()

	if err := shutdownTracing(shutdownCtx); err != nil {
		log.Error("tracing shutdown error", "error", err)
	}

	return nil
}
