# Event Observability Platform — Build Plan

## Overview

Production-grade Go repository demonstrating observability, integrations, analytics pipelines,
and distributed systems patterns. Each iteration results in a shippable commit pushed to GitHub.

---

## Iteration 1 — Project Skeleton & Infrastructure Foundation

**Goal:** Runnable repo with all infrastructure services wired up, config loading, and health endpoints.

### Tasks

- [ ] Create directory structure: `cmd/`, `internal/`, `migrations/`, `deployments/`, `docs/`, `examples/`
- [ ] Initialize Go module (`go.mod`, `go.sum`) with core dependencies
- [ ] Write `Makefile` with targets: `build`, `run`, `test`, `lint`, `docker-up`, `docker-down`, `migrate`
- [ ] Write `docker-compose.yml` with services:
  - PostgreSQL 16
  - ClickHouse 24
  - Kafka + Zookeeper (or Redpanda as single-container alternative)
  - Prometheus
  - Grafana
- [ ] Write `.env.example` with all required environment variables
- [ ] Implement `internal/config/` — config struct, env loading via `os.Getenv` or `github.com/caarlos0/env`
- [ ] Implement `cmd/server/main.go` — wires up config, logger, HTTP server
- [ ] Implement `GET /health` — returns 200 + `{"status":"ok"}`
- [ ] Implement `GET /ready` — checks DB connectivity, returns 200 or 503
- [ ] Add graceful shutdown (context + signal handling)
- [ ] Write skeleton `README.md` with all planned sections (empty placeholders)

**Commit:** `feat: project skeleton, docker-compose, health endpoints`

---

## Iteration 2 — Structured Logging + Request Middleware

**Goal:** Every request has `request_id`, `trace_id`, structured JSON logs.

### Tasks

- [ ] Add `slog` (stdlib) or `zerolog` as structured logger
- [ ] Implement `internal/observability/logger/` — logger factory, context helpers
- [ ] Implement HTTP middleware:
  - `RequestID` — generates UUID, injects into context + response header `X-Request-ID`
  - `Logger` — logs method, path, status, latency, request_id
  - `Recover` — panic recovery with logged stack trace
- [ ] Propagate `request_id` through context via typed key
- [ ] Add `GET /debug/pprof` endpoints (behind build tag or env flag `PPROF_ENABLED=true`)
- [ ] Validate all env variables on startup; fail fast with descriptive error if missing

**Commit:** `feat: structured logging, request-id middleware, pprof endpoint`

---

## Iteration 3 — OpenTelemetry Tracing + Prometheus Metrics

**Goal:** Distributed traces exported to collector, Prometheus metrics scraped on `/metrics`.

### Tasks

- [ ] Add OpenTelemetry SDK dependencies (`go.opentelemetry.io/otel`, `otlptracehttp/grpc`)
- [ ] Implement `internal/observability/tracing/` — tracer provider init, shutdown
- [ ] Add OTLP exporter (stdout for local dev, OTLP HTTP for production)
- [ ] Wire OTel middleware: extract/inject trace context, start server span per request
- [ ] Propagate `span.TraceID()` into logger fields as `trace_id`
- [ ] Add Prometheus client (`github.com/prometheus/client_golang`)
- [ ] Implement `internal/metrics/` — metric definitions:
  - `http_requests_total` (counter, labels: method, path, status)
  - `http_request_duration_seconds` (histogram, labels: method, path)
  - `events_processed_total` (counter, labels: source, status)
  - `events_failed_total` (counter, labels: source, reason)
  - `events_duplicate_total` (counter)
  - `queue_lag_messages` (gauge, labels: topic)
- [ ] Expose `GET /metrics` via Prometheus handler
- [ ] Add `deployments/prometheus/prometheus.yml` scrape config
- [ ] Add Grafana datasource + basic dashboard JSON in `deployments/grafana/`

**Commit:** `feat: opentelemetry tracing, prometheus metrics, grafana dashboard`

---

## Iteration 4 — PostgreSQL Layer + Event Domain Model

**Goal:** Core domain entities persisted to PostgreSQL with proper idempotency.

### Tasks

- [ ] Write PostgreSQL migrations in `migrations/postgres/`:
  - `001_create_webhook_events.up.sql` — events table (id, idempotency_key, source, payload, status, created_at, processed_at)
  - `001_create_webhook_events.down.sql`
  - `002_create_idempotency_keys.up.sql` — idempotency_keys table (key, event_id, created_at, expires_at)
  - `002_create_idempotency_keys.down.sql`
- [ ] Add migration runner to Makefile (`migrate up`, `migrate down`)
- [ ] Implement `internal/storage/postgres/` — connection pool (`pgx/v5`), ping check
- [ ] Implement `internal/storage/postgres/event_repository.go`:
  - `Insert(ctx, event) error`
  - `FindByID(ctx, id) (*Event, error)`
  - `UpdateStatus(ctx, id, status) error`
  - `ListByStatus(ctx, status, limit) ([]*Event, error)`
- [ ] Implement `internal/idempotency/` — check + store idempotency key with TTL, return `ErrDuplicate` if seen
- [ ] Define domain types in `internal/app/event.go`: `Event`, `EventStatus` (pending/processing/processed/failed)
- [ ] Write unit tests for idempotency logic (in-memory store for tests)

**Commit:** `feat: postgres migrations, event repository, idempotency key store`

---

## Iteration 5 — Webhook Ingestion API

**Goal:** `POST /webhooks/events` fully working: validate, deduplicate, persist, return trace_id.

### Tasks

- [ ] Implement `internal/http/handler/webhook.go`:
  - Parse JSON body
  - Validate required fields: `source`, `event_type`, `payload`, `idempotency_key`
  - Check idempotency key — return `200` with original response if duplicate (not `409`)
  - Persist event to PostgreSQL with status `pending`
  - Return `202 Accepted` with `event_id` and `trace_id`
- [ ] Implement `internal/http/handler/errors.go` — unified JSON error responses
- [ ] Add input size limit middleware (max body 1MB)
- [ ] Add `Content-Type: application/json` validation
- [ ] Increment `events_processed_total`, `events_duplicate_total` metrics on relevant paths
- [ ] Add span attributes: `event.source`, `event.type`, `event.idempotency_key`
- [ ] Write `examples/webhook_request.http` or `examples/curl_examples.sh`
- [ ] Write integration test for happy path + duplicate path (using testcontainers or real DB)

**Commit:** `feat: webhook ingestion endpoint with idempotency and validation`

---

## Iteration 6 — Kafka Producer + Async Worker Consumer

**Goal:** Events published to Kafka after ingestion, consumed asynchronously with retry + DLQ.

### Tasks

- [ ] Add Kafka client dependency (`github.com/segmentio/kafka-go` or `github.com/twmb/franz-go`)
- [ ] Implement `internal/integrations/kafka/producer.go`:
  - `Publish(ctx, topic, key, value) error`
  - Instrumented with OTel span + metric
  - Timeout enforced via context
- [ ] Wire producer into webhook handler: publish after successful DB insert
- [ ] Implement `internal/worker/event_processor.go`:
  - Consume from `events` topic
  - Parse message, load event from DB, transition status to `processing`
  - Call business logic (placeholder: simulate processing with sleep/log)
  - On success: update status to `processed`, write to ClickHouse (next iteration)
  - On failure: increment retry counter; after max retries, publish to `events.dlq` topic
- [ ] Implement `internal/worker/dlq_handler.go`:
  - Consume from `events.dlq`
  - Log poison message details, update event status to `failed`
- [ ] Implement graceful shutdown for consumer: drain in-flight messages on SIGTERM
- [ ] Update `queue_lag_messages` gauge via periodic offset check goroutine
- [ ] Add `cmd/worker/main.go` — separate binary for the consumer

**Commit:** `feat: kafka producer, async event worker, retry logic, dead letter queue`

---

## Iteration 7 — ClickHouse Analytics Layer

**Goal:** Processed events written to ClickHouse; analytical query exposed via API.

### Tasks

- [ ] Write ClickHouse migrations in `migrations/clickhouse/`:
  - `001_create_events_log.up.sql` — `events_log` table (ReplacingMergeTree, columns: event_id, source, event_type, status, processed_at, date)
  - `001_create_events_log.down.sql`
  - `002_create_daily_stats_mv.up.sql` — materialized view `daily_event_stats` aggregating by date + source + event_type
- [ ] Implement `internal/storage/clickhouse/` — ClickHouse connection, batch insert helper
- [ ] Implement `internal/analytics/event_writer.go` — write processed event row to ClickHouse from worker
- [ ] Implement `internal/analytics/query.go`:
  - `DailyEvents(ctx, from, to time.Time) ([]DailyEventStat, error)`
- [ ] Implement `GET /analytics/daily-events?from=YYYY-MM-DD&to=YYYY-MM-DD`:
  - Validate date params
  - Query ClickHouse materialized view
  - Return JSON array of daily stats
- [ ] Add OTel span for ClickHouse query
- [ ] Add `examples/analytics_query.sh`

**Commit:** `feat: clickhouse analytics layer, daily-events endpoint, materialized view`

---

## Iteration 8 — External Integrations Client

**Goal:** Reusable HTTP client abstraction with retry, timeout, circuit breaker.

### Tasks

- [ ] Implement `internal/integrations/httpclient/`:
  - `Client` struct wrapping `net/http` with configurable timeout, base URL, headers
  - `Do(ctx, req) (*Response, error)` — core method
  - Automatic retry with exponential backoff + jitter (max 3 attempts by default)
  - Context-based timeout enforcement
  - OTel span per outbound request with HTTP attributes
  - Prometheus counter for outbound requests (labels: target, method, status)
- [ ] Implement circuit breaker using `sony/gobreaker` or hand-written state machine:
  - States: Closed → Open → Half-Open
  - Config: failure threshold, timeout, half-open probe count
  - Return `ErrCircuitOpen` when open; log state transitions
- [ ] Wire client into a stub external notification integration (`internal/integrations/notifier/`)
  as example of the client being used
- [ ] Write unit tests for retry logic and circuit breaker state transitions

**Commit:** `feat: external http client with retry, timeout, circuit breaker`

---

## Iteration 9 — Tests, Docs & Final Polish

**Goal:** Solid test coverage on critical paths, complete README, production-ready compose.

### Tasks

- [ ] Write unit tests:
  - Idempotency store
  - Retry/backoff logic
  - Circuit breaker state machine
  - Webhook handler (table-driven, httptest)
  - Analytics query builder
- [ ] Write integration tests:
  - Webhook → DB → Kafka publish flow (testcontainers-go)
  - Duplicate webhook returns cached response
  - ClickHouse insert + query round-trip
- [ ] Add `golangci-lint` config (`.golangci.yml`) with relevant linters
- [ ] Add `Makefile` target: `make lint`, `make test`, `make test-integration`
- [ ] Complete `README.md`:
  - Project purpose
  - What this repository demonstrates
  - Architecture diagram (ASCII)
  - Request lifecycle (numbered steps)
  - Observability section (traces, metrics, logs)
  - Analytics with ClickHouse section
  - Idempotency and retries section (honest about exactly-once limitations)
  - Local setup (step by step)
  - API examples (curl)
  - Useful Makefile commands table
  - Testing section
  - Possible production improvements
- [ ] Finalize `docker-compose.yml`:
  - Named volumes, healthchecks for all services
  - Depends-on with condition: service_healthy
  - Grafana pre-provisioned datasource + dashboard
- [ ] Add `deployments/grafana/dashboards/events.json` — pre-built dashboard
- [ ] Review all TODOs in code; remove or convert to documented decisions
- [ ] Final pass: no dead code, no commented-out blocks, no placeholder strings

**Commit:** `feat: integration tests, complete readme, finalized docker-compose`

---

## Iteration 10 — GitHub Presentation Layer

**Goal:** Repo looks polished and professional on GitHub.

### Tasks

- [ ] Add `.github/workflows/ci.yml` — lint + test on push/PR (Go 1.22+)
- [ ] Add `.gitignore` — binaries, `.env`, IDE files, test cache
- [ ] Add `LICENSE` (MIT)
- [ ] Tag release `v0.1.0`
- [ ] Verify `go vet ./...` passes clean
- [ ] Verify `docker compose up` brings all services healthy from scratch
- [ ] Smoke-test all API endpoints manually against local compose

**Commit:** `chore: ci workflow, gitignore, license, release tag v0.1.0`

---

## Dependency Reference

| Package | Purpose |
|---|---|
| `github.com/caarlos0/env/v11` | Env config parsing |
| `github.com/jackc/pgx/v5` | PostgreSQL driver + pool |
| `github.com/ClickHouse/clickhouse-go/v2` | ClickHouse driver |
| `github.com/segmentio/kafka-go` | Kafka client |
| `go.opentelemetry.io/otel` | OTel SDK |
| `go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp` | OTLP exporter |
| `github.com/prometheus/client_golang` | Prometheus metrics |
| `github.com/sony/gobreaker` | Circuit breaker |
| `github.com/google/uuid` | UUID generation |
| `github.com/testcontainers/testcontainers-go` | Integration test containers |
| `golang.org/x/exp/slog` | Structured logging (or stdlib slog Go 1.21+) |

---

## Iteration Summary

| # | Name | Key Deliverable |
|---|---|---|
| 1 | Skeleton | Project structure, Docker Compose, health endpoints |
| 2 | Logging | Structured logs, request_id middleware, pprof |
| 3 | Observability | OTel traces, Prometheus metrics, Grafana |
| 4 | PostgreSQL | Migrations, event repository, idempotency store |
| 5 | Webhook API | POST /webhooks/events with deduplication |
| 6 | Kafka + Worker | Async consumer, retry, DLQ |
| 7 | ClickHouse | Analytics write path, daily-events query |
| 8 | HTTP Client | Retry, timeout, circuit breaker |
| 9 | Tests + Docs | Full README, integration tests, lint |
| 10 | GitHub Polish | CI, license, release tag |
