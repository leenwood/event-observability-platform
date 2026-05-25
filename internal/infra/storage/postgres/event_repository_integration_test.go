//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"

	tcontainers "github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/leenwood/event-observability-platform/internal/core/domain"
	"github.com/leenwood/event-observability-platform/internal/core/dto"
)

const (
	pgUser = "events"
	pgPass = "events"
	pgDB   = "events"
)

const schemaSQL = `
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

func startPostgres(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()

	ctr, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase(pgDB),
		tcpostgres.WithUsername(pgUser),
		tcpostgres.WithPassword(pgPass),
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

	db, err := New(ctx, dsn, 5, 1, 5*time.Minute)
	if err != nil {
		t.Fatalf("connect to postgres: %v", err)
	}
	t.Cleanup(db.Close)

	_, err = db.Pool.Exec(ctx, schemaSQL)
	if err != nil {
		t.Fatalf("create schema: %v", err)
	}

	return db
}

func sampleEvent() *domain.Event {
	return &domain.Event{
		ID:             "00000000-0000-0000-0000-000000000001",
		IdempotencyKey: "idem-key-1",
		Source:         "shopify",
		EventType:      "order.shipped",
		Payload:        []byte(`{"order_id":"123"}`),
		Status:         domain.EventStatusPending,
		CreatedAt:      time.Now().UTC().Truncate(time.Millisecond),
	}
}

func TestEventRepository_InsertAndFind(t *testing.T) {
	db := startPostgres(t)
	repo := NewEventRepository(db)
	ctx := context.Background()

	ev := sampleEvent()
	if err := repo.Insert(ctx, ev); err != nil {
		t.Fatalf("insert: %v", err)
	}

	found, err := repo.FindByID(ctx, ev.ID)
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if found.ID != ev.ID {
		t.Errorf("id mismatch: got %q want %q", found.ID, ev.ID)
	}
	if found.Source != ev.Source {
		t.Errorf("source mismatch: got %q want %q", found.Source, ev.Source)
	}
	if found.Status != domain.EventStatusPending {
		t.Errorf("status: got %q want pending", found.Status)
	}
}

func TestEventRepository_FindNotFound(t *testing.T) {
	db := startPostgres(t)
	repo := NewEventRepository(db)
	ctx := context.Background()

	_, err := repo.FindByID(ctx, "00000000-0000-0000-0000-000000000099")
	if err != domain.ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestEventRepository_UpdateStatus(t *testing.T) {
	db := startPostgres(t)
	repo := NewEventRepository(db)
	ctx := context.Background()

	ev := sampleEvent()
	_ = repo.Insert(ctx, ev)

	if err := repo.UpdateStatus(ctx, ev.ID, domain.EventStatusProcessed); err != nil {
		t.Fatalf("update status: %v", err)
	}

	found, _ := repo.FindByID(ctx, ev.ID)
	if found.Status != domain.EventStatusProcessed {
		t.Errorf("status: got %q want processed", found.Status)
	}
	if found.ProcessedAt == nil {
		t.Error("expected processed_at to be set after status=processed")
	}
}

func TestEventRepository_IncrementRetry(t *testing.T) {
	db := startPostgres(t)
	repo := NewEventRepository(db)
	ctx := context.Background()

	ev := sampleEvent()
	_ = repo.Insert(ctx, ev)

	for i := 1; i <= 3; i++ {
		if err := repo.IncrementRetry(ctx, ev.ID); err != nil {
			t.Fatalf("increment retry %d: %v", i, err)
		}
	}

	found, _ := repo.FindByID(ctx, ev.ID)
	if found.RetryCount != 3 {
		t.Errorf("retry_count: got %d want 3", found.RetryCount)
	}
}

func TestEventRepository_ListByStatus(t *testing.T) {
	db := startPostgres(t)
	repo := NewEventRepository(db)
	ctx := context.Background()

	ids := []string{
		"00000000-0000-0000-0000-000000000011",
		"00000000-0000-0000-0000-000000000012",
		"00000000-0000-0000-0000-000000000013",
	}
	for _, id := range ids {
		ev := sampleEvent()
		ev.ID = id
		ev.IdempotencyKey = "idem-" + id
		_ = repo.Insert(ctx, ev)
	}

	list, err := repo.ListByStatus(ctx, domain.EventStatusPending, 10)
	if err != nil {
		t.Fatalf("list by status: %v", err)
	}
	if len(list) != 3 {
		t.Errorf("expected 3 pending events, got %d", len(list))
	}
}

func TestIdempotencyStore_SetAndGet(t *testing.T) {
	db := startPostgres(t)
	ctx := context.Background()

	repo := NewEventRepository(db)
	ev := sampleEvent()
	_ = repo.Insert(ctx, ev)

	store := NewIdempotencyStore(db)

	entry := &dto.Entry{
		EventID:   ev.ID,
		Response:  []byte(`{"event_id":"` + ev.ID + `"}`),
		ExpiresAt: time.Now().Add(24 * time.Hour),
	}

	if err := store.Set(ctx, "idem-key-1", entry); err != nil {
		t.Fatalf("set: %v", err)
	}

	got, err := store.Get(ctx, "idem-key-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got == nil {
		t.Fatal("expected entry, got nil")
	}
	if got.EventID != ev.ID {
		t.Errorf("event_id: got %q want %q", got.EventID, ev.ID)
	}
}

func TestIdempotencyStore_MissingKeyReturnsNil(t *testing.T) {
	db := startPostgres(t)
	ctx := context.Background()

	store := NewIdempotencyStore(db)

	got, err := store.Get(ctx, "no-such-key")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for missing key, got %+v", got)
	}
}
