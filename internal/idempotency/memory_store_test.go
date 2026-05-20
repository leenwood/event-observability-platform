package idempotency

import (
	"context"
	"testing"
	"time"
)

func TestMemoryStore_GetMissing(t *testing.T) {
	s := NewMemoryStore()
	entry, err := s.Get(context.Background(), "nonexistent")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if entry != nil {
		t.Fatalf("expected nil entry, got %+v", entry)
	}
}

func TestMemoryStore_SetAndGet(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	want := &Entry{
		EventID:   "event-123",
		Response:  []byte(`{"event_id":"event-123"}`),
		ExpiresAt: time.Now().Add(time.Hour),
	}

	if err := s.Set(ctx, "key-1", want); err != nil {
		t.Fatalf("set: %v", err)
	}

	got, err := s.Get(ctx, "key-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got == nil {
		t.Fatal("expected entry, got nil")
	}
	if got.EventID != want.EventID {
		t.Errorf("event_id: got %q, want %q", got.EventID, want.EventID)
	}
}

func TestMemoryStore_SetDoesNotOverwrite(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	first := &Entry{EventID: "first", ExpiresAt: time.Now().Add(time.Hour)}
	second := &Entry{EventID: "second", ExpiresAt: time.Now().Add(time.Hour)}

	_ = s.Set(ctx, "key", first)
	_ = s.Set(ctx, "key", second)

	got, _ := s.Get(ctx, "key")
	if got.EventID != "first" {
		t.Errorf("expected first entry to be preserved, got %q", got.EventID)
	}
}

func TestMemoryStore_ExpiredEntryNotReturned(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	entry := &Entry{
		EventID:   "old-event",
		ExpiresAt: time.Now().Add(-time.Second),
	}

	_ = s.Set(ctx, "stale-key", entry)

	got, err := s.Get(ctx, "stale-key")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil for expired entry, got %+v", got)
	}
}

func TestMemoryStore_ConcurrentSafety(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	done := make(chan struct{})
	for i := 0; i < 50; i++ {
		go func(_ int) {
			key := "shared-key"
			_ = s.Set(ctx, key, &Entry{EventID: "ev", ExpiresAt: time.Now().Add(time.Hour)})
			_, _ = s.Get(ctx, key)
			done <- struct{}{}
		}(i)
	}
	for i := 0; i < 50; i++ {
		<-done
	}
}
