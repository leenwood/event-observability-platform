# Event Observability Platform

[🇷🇺 Русский](README.ru.md)

A production-oriented Go backend service for webhook ingestion, asynchronous event processing,
and analytics. Built to demonstrate real-world patterns in observability, distributed systems,
and data pipelines.

---

## What This Repository Demonstrates

- **OpenTelemetry tracing** — distributed trace propagation across HTTP and async workers
- **Prometheus metrics** — RED metrics, queue lag, event processing counters
- **Structured logging** — JSON logs with `request_id`, `trace_id`, and `correlation_id` on every line
- **Idempotent webhook processing** — duplicate requests are safely absorbed, not double-processed
- **Kafka-based event pipeline** — decoupled ingestion from processing with retry and DLQ semantics
- **ClickHouse analytics** — high-throughput event storage with materialized view aggregations
- **Circuit breaker + retry** — resilient outbound HTTP client for external integrations
- **Graceful shutdown** — in-flight requests and consumer commits are drained before exit

---

## Architecture Overview

```
                        ┌─────────────────────────────────────┐
                        │           HTTP Server                │
                        │  POST /webhooks/events               │
                        │  GET  /health  GET /ready            │
                        │  GET  /metrics GET /analytics/...    │
                        └────────────┬────────────────────────┘
                                     │ 1. validate + deduplicate
                                     │ 2. persist (PostgreSQL)
                                     │ 3. publish
                                     ▼
                        ┌────────────────────────┐
                        │    Kafka / Redpanda     │
                        │  topic: events          │
                        │  topic: events.retry    │
                        │  topic: events.dlq      │
                        └────────────┬────────────┘
                                     │ consume
                                     ▼
                        ┌────────────────────────┐
                        │    Async Worker         │
                        │  - process event        │
                        │  - update PostgreSQL    │
                        │  - write ClickHouse     │
                        │  - retry on failure     │
                        │  - DLQ after max retry  │
                        └────────────────────────┘
                                     │
               ┌─────────────────────┴──────────────────┐
               ▼                                         ▼
  ┌────────────────────┐                  ┌──────────────────────┐
  │     PostgreSQL     │                  │      ClickHouse       │
  │  events            │                  │  events_log           │
  │  idempotency_keys  │                  │  daily_event_stats MV │
  └────────────────────┘                  └──────────────────────┘

  Observability stack:
  Prometheus ← scrapes /metrics
  Grafana    ← dashboards from Prometheus
  OTLP       ← traces from server + worker
```

---

## Request Lifecycle

1. Client sends `POST /webhooks/events` with `Idempotency-Key` header or body field
2. Middleware assigns `request_id` (UUID) and starts OTel span
3. Handler validates payload fields
4. Idempotency store checks if key was already seen — returns cached response if so
5. Event persisted to PostgreSQL with status `pending`
6. Event published to Kafka topic `events`
7. Handler returns `202 Accepted` with `event_id` and `trace_id`
8. Worker consumes from Kafka, transitions event to `processing`
9. On success: status → `processed`, row written to ClickHouse
10. On failure: retry up to N times, then publish to `events.dlq`

---

## Observability

### Logs
All logs are JSON. Every request line includes:
```json
{"time":"...","level":"INFO","msg":"request","method":"POST","path":"/webhooks/events",
 "status":202,"latency":"1.2ms","request_id":"uuid","trace_id":"otel-trace-id"}
```

### Traces
OpenTelemetry spans cover:
- HTTP handler (root span)
- PostgreSQL queries
- Kafka publish / consume
- ClickHouse writes

### Metrics
| Metric | Type | Labels |
|---|---|---|
| `http_requests_total` | Counter | method, path, status |
| `http_request_duration_seconds` | Histogram | method, path |
| `events_processed_total` | Counter | source, status |
| `events_failed_total` | Counter | source, reason |
| `events_duplicate_total` | Counter | — |
| `queue_lag_messages` | Gauge | topic |

---

## Analytics with ClickHouse

Processed events are written to `events_log` (ReplacingMergeTree).
A materialized view `daily_event_stats` aggregates counts by date, source, and event type.

Query via API:
```
GET /analytics/daily-events?from=2024-01-01&to=2024-01-31
```

---

## Idempotency and Retries

**Idempotency:** Every webhook must carry an `idempotency_key`. The platform stores processed keys
with a configurable TTL (default 24h). Duplicate requests within the TTL window return the original
response without side effects.

**Retries:** Failed Kafka messages are requeued to `events.retry` with an incremented attempt
counter. After `KAFKA_MAX_RETRIES` attempts, the message is moved to `events.dlq`.

**Exactly-once caveat:** The platform provides at-least-once delivery. The idempotency key prevents
double-processing at the application layer, but there is no transactional guarantee across the
PostgreSQL write and Kafka publish. A crash between these two steps may result in a missed publish —
the event will remain `pending` and require reconciliation. This is a known trade-off; true
exactly-once would require a transactional outbox pattern.

---

## Local Setup

**Prerequisites:** Docker, Docker Compose, Go 1.22+, [go-task](https://taskfile.dev/installation/)

```bash
# Install go-task (once)
go install github.com/go-task/task/v3/cmd/task@latest

# 1. Clone and enter
git clone https://github.com/leenwood/event-observability-platform
cd event-observability-platform

# 2. Copy env
cp .env.example .env

# 3. Start infrastructure
task docker:up

# 4. Run migrations
task migrate:up

# 5. Run the http
task run
```

Services available locally:
| Service | URL |
|---|---|
| API server | http://localhost:8080 |
| Swagger UI | http://localhost:8080/swagger/index.html |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 (admin/admin) |
| Redpanda Console | http://localhost:18082 |
| ClickHouse HTTP | http://localhost:8123 |

---

## API Examples

```bash
# Health check
curl http://localhost:8080/health

# Readiness probe
curl http://localhost:8080/ready

# Ingest a webhook event
curl -X POST http://localhost:8080/webhooks/events \
  -H "Content-Type: application/json" \
  -d '{
    "idempotency_key": "order-shipped-12345",
    "source": "shopify",
    "event_type": "order.shipped",
    "payload": {"order_id": "12345", "tracking": "1Z999AA1"}
  }'

# Duplicate — returns same response, not processed twice
curl -X POST http://localhost:8080/webhooks/events \
  -H "Content-Type: application/json" \
  -d '{
    "idempotency_key": "order-shipped-12345",
    "source": "shopify",
    "event_type": "order.shipped",
    "payload": {"order_id": "12345", "tracking": "1Z999AA1"}
  }'

# Analytics query
curl "http://localhost:8080/analytics/daily-events?from=2024-01-01&to=2024-01-31"

# Prometheus metrics
curl http://localhost:8080/metrics
```

---

## Useful Task Commands

Run `task --list` to see all available commands.

| Command | Description |
|---|---|
| `task build` | Build server and worker binaries |
| `task build:server` | Build server binary only |
| `task build:worker` | Build worker binary only |
| `task run` | Run HTTP server locally |
| `task run:worker` | Run async event worker |
| `task test` | Run unit tests with race detector |
| `task test:integration` | Run integration tests (requires Docker) |
| `task test:cover` | Tests with HTML coverage report |
| `task lint` | Run golangci-lint |
| `task vet` | Run go vet |
| `task fmt` | Format code with gofmt + goimports |
| `task docker:up` | Start all infrastructure containers |
| `task docker:down` | Stop containers |
| `task docker:reset` | Stop containers and wipe volumes |
| `task docker:logs` | Stream container logs |
| `task migrate:up` | Apply all migrations |
| `task migrate:down` | Revert all migrations |
| `task migrate:create -- <name>` | Create new migration file pair |
| `task docs` | Regenerate Swagger docs from annotations |
| `task deps` | Download and tidy Go dependencies |

---

## Testing

Unit tests cover: idempotency store, retry/backoff logic, circuit breaker state machine,
webhook handler (table-driven with `httptest`), and analytics query builder.

Integration tests use `testcontainers-go` to spin up real PostgreSQL and ClickHouse instances,
verifying the full webhook → DB → Kafka → worker → ClickHouse flow.

```bash
task test
task test:integration
```

---

## Possible Production Improvements

- **Transactional outbox** — write event to an `outbox` table in the same PostgreSQL transaction,
  then have a relay process publish to Kafka. Eliminates the write/publish split window.
- **Schema registry** — enforce Avro/Protobuf schemas on Kafka topics to prevent payload drift.
- **Rate limiting** — per-source webhook rate limits to prevent ingestion spikes.
- **Alerting rules** — Prometheus alerting for queue lag > threshold, error rate spike, DLQ growth.
- **Horizontal scaling** — stateless HTTP server scales trivially; worker scales by Kafka partition count.
- **Key rotation** — idempotency key store backed by Redis with TTL for distributed deployments.
- **Audit log** — append-only PostgreSQL table tracking every status transition per event.
