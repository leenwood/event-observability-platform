BINARY_SERVER=bin/server
BINARY_WORKER=bin/worker
MODULE=github.com/leenwood/event-observability-platform
GO=go
GOFLAGS=-trimpath

.PHONY: all build build-server build-worker run run-worker test test-integration lint fmt vet \
        docker-up docker-down docker-logs migrate-up migrate-down migrate-create help

all: build

## Build

build: build-server build-worker

build-server:
	$(GO) build $(GOFLAGS) -o $(BINARY_SERVER) ./cmd/server

build-worker:
	$(GO) build $(GOFLAGS) -o $(BINARY_WORKER) ./cmd/worker 2>/dev/null || echo "worker binary not yet implemented"

## Run

run:
	$(GO) run ./cmd/server

run-worker:
	$(GO) run ./cmd/worker

## Test

test:
	$(GO) test -race -count=1 -timeout=60s ./...

test-integration:
	$(GO) test -race -count=1 -timeout=120s -tags=integration ./...

test-cover:
	$(GO) test -race -count=1 -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

## Code quality

lint:
	golangci-lint run ./...

fmt:
	$(GO) fmt ./...
	goimports -w .

vet:
	$(GO) vet ./...

## Docker

docker-up:
	docker compose up -d postgres clickhouse redpanda redpanda-init prometheus grafana

docker-down:
	docker compose down

docker-reset:
	docker compose down -v

docker-logs:
	docker compose logs -f

## Database migrations

migrate-up:
	@echo "Running PostgreSQL migrations..."
	@for f in migrations/postgres/*.up.sql; do \
		echo "  -> $$f"; \
		PGPASSWORD=events psql -h localhost -U events -d events -f "$$f"; \
	done
	@echo "Running ClickHouse migrations..."
	@for f in migrations/clickhouse/*.up.sql; do \
		echo "  -> $$f"; \
		curl -s "http://localhost:8123/?user=events&password=events" --data-binary @"$$f"; \
	done
	@echo "Migrations done."

migrate-down:
	@echo "Reverting PostgreSQL migrations (reverse order)..."
	@for f in $$(ls -r migrations/postgres/*.down.sql); do \
		echo "  -> $$f"; \
		PGPASSWORD=events psql -h localhost -U events -d events -f "$$f"; \
	done

migrate-create:
	@read -p "Migration name (e.g. add_user_table): " name; \
	ts=$$(date +%03d); \
	touch "migrations/postgres/$${ts}_$${name}.up.sql" "migrations/postgres/$${ts}_$${name}.down.sql"; \
	echo "Created migrations/postgres/$${ts}_$${name}.{up,down}.sql"

## Misc

deps:
	$(GO) mod download
	$(GO) mod tidy

help:
	@echo ""
	@echo "Usage: make <target>"
	@echo ""
	@echo "Build:"
	@echo "  build            Build server and worker binaries"
	@echo "  build-server     Build server binary only"
	@echo "  build-worker     Build worker binary only"
	@echo ""
	@echo "Run:"
	@echo "  run              Run HTTP server locally"
	@echo "  run-worker       Run async event worker locally"
	@echo ""
	@echo "Test:"
	@echo "  test             Run unit tests"
	@echo "  test-integration Run integration tests (requires Docker)"
	@echo "  test-cover       Run tests with coverage report"
	@echo ""
	@echo "Code quality:"
	@echo "  lint             Run golangci-lint"
	@echo "  fmt              Format code"
	@echo "  vet              Run go vet"
	@echo ""
	@echo "Docker:"
	@echo "  docker-up        Start infrastructure (postgres, clickhouse, kafka, prometheus, grafana)"
	@echo "  docker-down      Stop all containers"
	@echo "  docker-reset     Stop containers and remove volumes"
	@echo "  docker-logs      Stream container logs"
	@echo ""
	@echo "Migrations:"
	@echo "  migrate-up       Apply all pending migrations"
	@echo "  migrate-down     Revert all migrations"
	@echo "  migrate-create   Create new migration file pair"
