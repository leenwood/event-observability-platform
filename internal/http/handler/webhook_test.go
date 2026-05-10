package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/leenwood/event-observability-platform/internal/app"
	"github.com/leenwood/event-observability-platform/internal/idempotency"
	"github.com/leenwood/event-observability-platform/internal/metrics"
)

// mockEventRepo satisfies app.EventRepository using zero-value defaults.
type mockEventRepo struct {
	insertErr error
}

func (m *mockEventRepo) Insert(_ context.Context, _ *app.Event) error {
	return m.insertErr
}
func (m *mockEventRepo) FindByID(_ context.Context, _ string) (*app.Event, error) {
	return nil, app.ErrNotFound
}
func (m *mockEventRepo) UpdateStatus(_ context.Context, _ string, _ app.EventStatus) error {
	return nil
}
func (m *mockEventRepo) IncrementRetry(_ context.Context, _ string) error { return nil }
func (m *mockEventRepo) ListByStatus(_ context.Context, _ app.EventStatus, _ int) ([]*app.Event, error) {
	return nil, nil
}

func newTestHandler(repo app.EventRepository, store idempotency.Store) *WebhookHandler {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return NewWebhookHandler(repo, store, metrics.New(), log, 24*time.Hour)
}

func postJSON(t *testing.T, h *WebhookHandler, body any) *httptest.ResponseRecorder {
	t.Helper()
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/events", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.HandleEvent(rr, req)
	return rr
}

func validPayload() map[string]any {
	return map[string]any{
		"idempotency_key": "key-001",
		"source":          "shopify",
		"event_type":      "order.shipped",
		"payload":         map[string]any{"order_id": "123"},
	}
}

func TestWebhookHandler_HappyPath(t *testing.T) {
	h := newTestHandler(&mockEventRepo{}, idempotency.NewMemoryStore())
	rr := postJSON(t, h, validPayload())

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rr.Code, rr.Body.String())
	}

	var resp webhookResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.EventID == "" {
		t.Error("expected non-empty event_id")
	}
}

func TestWebhookHandler_DuplicateRequest(t *testing.T) {
	store := idempotency.NewMemoryStore()
	h := newTestHandler(&mockEventRepo{}, store)

	rr1 := postJSON(t, h, validPayload())
	if rr1.Code != http.StatusAccepted {
		t.Fatalf("first request: expected 202, got %d", rr1.Code)
	}

	var first webhookResponse
	_ = json.NewDecoder(rr1.Body).Decode(&first)

	rr2 := postJSON(t, h, validPayload())
	if rr2.Code != http.StatusOK {
		t.Fatalf("duplicate request: expected 200, got %d", rr2.Code)
	}
	if rr2.Header().Get("X-Idempotent-Replayed") != "true" {
		t.Error("expected X-Idempotent-Replayed: true header")
	}

	var second webhookResponse
	_ = json.NewDecoder(rr2.Body).Decode(&second)
	if first.EventID != second.EventID {
		t.Errorf("expected same event_id on replay: got %q vs %q", first.EventID, second.EventID)
	}
}

func TestWebhookHandler_MissingFields(t *testing.T) {
	h := newTestHandler(&mockEventRepo{}, idempotency.NewMemoryStore())

	cases := []struct {
		name string
		body map[string]any
	}{
		{"missing idempotency_key", map[string]any{"source": "s", "event_type": "t", "payload": map[string]any{}}},
		{"missing source", map[string]any{"idempotency_key": "k", "event_type": "t", "payload": map[string]any{}}},
		{"missing event_type", map[string]any{"idempotency_key": "k", "source": "s", "payload": map[string]any{}}},
		{"missing payload", map[string]any{"idempotency_key": "k", "source": "s", "event_type": "t"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rr := postJSON(t, h, tc.body)
			if rr.Code != http.StatusUnprocessableEntity {
				t.Errorf("expected 422, got %d", rr.Code)
			}
		})
	}
}

func TestWebhookHandler_InvalidJSON(t *testing.T) {
	h := newTestHandler(&mockEventRepo{}, idempotency.NewMemoryStore())

	req := httptest.NewRequest(http.MethodPost, "/webhooks/events", bytes.NewBufferString("{bad json"))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.HandleEvent(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
}

func TestWebhookHandler_WrongContentType(t *testing.T) {
	h := newTestHandler(&mockEventRepo{}, idempotency.NewMemoryStore())

	req := httptest.NewRequest(http.MethodPost, "/webhooks/events", bytes.NewBufferString("{}"))
	req.Header.Set("Content-Type", "text/plain")
	rr := httptest.NewRecorder()
	h.HandleEvent(rr, req)

	if rr.Code != http.StatusUnsupportedMediaType {
		t.Errorf("expected 415, got %d", rr.Code)
	}
}
