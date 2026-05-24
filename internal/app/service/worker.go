package service

import (
	"context"
	"sync"
	"time"

	"github.com/leenwood/event-observability-platform/internal/app/processor"
	"github.com/leenwood/event-observability-platform/internal/pkg/messaging"
	chstorage "github.com/leenwood/event-observability-platform/internal/pkg/storage/clickhouse"
	"github.com/leenwood/event-observability-platform/internal/pkg/storage/postgres"
)

// RunWorker initialises all dependencies, starts Processor and DLQHandler
// goroutines, and blocks until ctx is cancelled.
func RunWorker(ctx context.Context) error {
	infra, err := initInfra(ctx, "-worker")
	if err != nil {
		return err
	}

	cfg := infra.Cfg
	log := infra.Log

	eventRepo := postgres.NewEventRepository(infra.DB)

	eventsConsumer := messaging.NewConsumer(cfg.Kafka.Brokers, cfg.Kafka.TopicEvents, cfg.Kafka.ConsumerGroup)
	defer func() {
		if err := eventsConsumer.Close(); err != nil {
			log.Error("events consumer close", "error", err)
		}
	}()

	dlqConsumer := messaging.NewConsumer(cfg.Kafka.Brokers, cfg.Kafka.TopicDLQ, cfg.Kafka.ConsumerGroup+"-dlq")
	defer func() {
		if err := dlqConsumer.Close(); err != nil {
			log.Error("dlq consumer close", "error", err)
		}
	}()

	proc := processor.NewProcessor(
		eventsConsumer,
		infra.Producer,
		eventRepo,
		chstorage.NewEventWriter(infra.ChDB),
		infra.Metrics,
		log,
		processor.Topics{Events: cfg.Kafka.TopicEvents, DLQ: cfg.Kafka.TopicDLQ},
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
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
		log.Info("worker stopped cleanly")
	case <-time.After(30 * time.Second):
		log.Warn("worker shutdown timed out")
	}

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	infra.Shutdown(shutdownCtx)
	return nil
}
