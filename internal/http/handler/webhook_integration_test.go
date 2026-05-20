//go:build integration

package handler

import (
	"context"
	"testing"
	"time"

	"github.com/leenwood/event-observability-platform/internal/idempotency"
	"github.com/leenwood/event-observability-platform/internal/metrics"
	pgstore "github.com/leenwood/event-observability-platform/internal/storage/postgres"
	tcontainers "github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"log/slog"
	"net/http"
	"os"
)

const handlerSchemaSQL = `
CREATE TABLE IF NOT EXISTS webhook_events (
	id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	idempotency_key VARCHAR(255) NOT NULL,
	source          VARCHAR(100) NOT NULL,
	event_type      VARCHAR(100) NOT NULL,
	payload         JSONB        NOT NULL DEFAULT '{}',
	status          VARCHAR(20)  NOT NULL DEFAULT 'pending'
	                    CHECK (status IN ('pending','processing','processed','failed')),
	retry_count     INTEGER      NOT NULL DEFAULT 0,
	created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
	processed_at    TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS idempotency_keys (
	key        VARCHAR(255) PRIMARY KEY,
	event_id   UUID         NOT NULL REFERENCES webhook_events (id) ON DELETE CASCADE,
	response   JSONB        NOT NULL DEFAULT '{}',
	created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
	expires_at TIMESTAMPTZ  NOT NULL
);`

func startPostgresForHandler(t *testing.T) *pgstore.DB {
	t.Helper()
	ctx := context.Background()

	ctr, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("events"),
		tcpostgres.WithUsername("events"),
		tcpostgres.WithPassword("events"),
		tcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = tcontainers.TerminateContainer(ctr) })

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("get connection string: %v", err)
	}

	db, err := pgstore.New(ctx, dsn, 5, 1, 5*time.Minute)
	if err != nil {
		t.Fatalf("connect to postgres: %v", err)
	}
	t.Cleanup(db.Close)

	_, err = db.Pool.Exec(ctx, handlerSchemaSQL)
	if err != nil {
		t.Fatalf("create schema: %v", err)
	}
	return db
}

func TestWebhookHandler_Integration_HappyPath(t *testing.T) {
	db := startPostgresForHandler(t)
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	repo := pgstore.NewEventRepository(db)
	store := idempotency.NewPostgresStore(db.Pool)
	h := NewWebhookHandler(repo, store, noopPublisher{}, "events", metrics.New(), log, 24*time.Hour)

	rr := postJSON(t, h, validPayload())
	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rr.Code, rr.Body.String())
	}
	if rr.Header().Get("Content-Type") != "application/json" {
		t.Error("expected Content-Type: application/json")
	}
}

func TestWebhookHandler_Integration_DuplicateReturnsReplay(t *testing.T) {
	db := startPostgresForHandler(t)
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	repo := pgstore.NewEventRepository(db)
	store := idempotency.NewPostgresStore(db.Pool)
	h := NewWebhookHandler(repo, store, noopPublisher{}, "events", metrics.New(), log, 24*time.Hour)

	payload := map[string]any{
		"idempotency_key": "integration-idem-key",
		"source":          "stripe",
		"event_type":      "payment.captured",
		"payload":         map[string]any{"amount": 5000},
	}

	rr1 := postJSON(t, h, payload)
	if rr1.Code != http.StatusAccepted {
		t.Fatalf("first request: expected 202, got %d", rr1.Code)
	}

	rr2 := postJSON(t, h, payload)
	if rr2.Code != http.StatusOK {
		t.Fatalf("duplicate: expected 200, got %d: %s", rr2.Code, rr2.Body.String())
	}
	if rr2.Header().Get("X-Idempotent-Replayed") != "true" {
		t.Error("expected X-Idempotent-Replayed: true on duplicate")
	}
}
