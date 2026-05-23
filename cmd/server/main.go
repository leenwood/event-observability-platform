package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/leenwood/event-observability-platform/internal/app/service"
)

// @title       Event Observability Platform API
// @version     1.0
// @description Production-oriented webhook ingestion, async event processing, and analytics service.
// @description
// @description Demonstrates OpenTelemetry tracing, Prometheus metrics, Kafka pipelines,
// @description ClickHouse analytics, idempotent processing, and circuit breaker patterns.
//
// @contact.name  leenwood
// @contact.url   https://github.com/leenwood/event-observability-platform
//
// @host      localhost:8080
// @BasePath  /
//
// @tag.name         webhooks
// @tag.description  Webhook ingestion endpoint
// @tag.name         analytics
// @tag.description  ClickHouse-backed analytical queries
// @tag.name         health
// @tag.description  Liveness and readiness probes.

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := service.RunServer(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
