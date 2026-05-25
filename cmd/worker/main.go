package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/leenwood/event-observability-platform/internal/config"
	"github.com/leenwood/event-observability-platform/internal/integrations/kafka"
	"github.com/leenwood/event-observability-platform/internal/metrics"
	"github.com/leenwood/event-observability-platform/internal/observability/logger"
	"github.com/leenwood/event-observability-platform/internal/observability/tracing"
	"github.com/leenwood/event-observability-platform/internal/storage/postgres"
	"github.com/leenwood/event-observability-platform/internal/worker"
)

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
		ServiceName:  cfg.OTel.ServiceName + "-worker",
		ExporterType: cfg.OTel.ExporterType,
		Endpoint:     cfg.OTel.Endpoint,
	})
	if err != nil {
		log.Error("failed to init tracing", "error", err)
		os.Exit(1)
	}

	m := metrics.New()
	_ = m // worker metrics are written directly; HTTP exposure added if needed

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

	eventRepo := postgres.NewEventRepository(db)

	producer := kafka.NewProducer(cfg.Kafka.Brokers)
	defer producer.Close()

	eventsConsumer := kafka.NewConsumer(
		cfg.Kafka.Brokers,
		cfg.Kafka.TopicEvents,
		cfg.Kafka.ConsumerGroup,
	)
	defer eventsConsumer.Close()

	dlqConsumer := kafka.NewConsumer(
		cfg.Kafka.Brokers,
		cfg.Kafka.TopicDLQ,
		cfg.Kafka.ConsumerGroup+"-dlq",
	)
	defer dlqConsumer.Close()

	processor := worker.NewProcessor(
		eventsConsumer,
		producer,
		eventRepo,
		m,
		log,
		worker.Topics{
			Events: cfg.Kafka.TopicEvents,
			DLQ:    cfg.Kafka.TopicDLQ,
		},
		cfg.Kafka.MaxRetries,
	)

	dlqHandler := worker.NewDLQHandler(dlqConsumer, eventRepo, log)

	ctx, cancel := context.WithCancel(context.Background())

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Info("event processor started", "topic", cfg.Kafka.TopicEvents)
		if err := processor.Run(ctx); err != nil {
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

	<-quit
	log.Info("worker shutting down")
	cancel()

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

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := shutdownTracing(shutdownCtx); err != nil {
		log.Error("tracing shutdown error", "error", err)
	}
}
