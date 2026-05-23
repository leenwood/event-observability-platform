package httpclient

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

func newTestClient(baseURL string, maxRetries int) *Client {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := Config{
		BaseURL:    baseURL,
		Timeout:    2 * time.Second,
		MaxRetries: maxRetries,
		Target:     "test",
		Breaker:    BreakerConfig{MaxFailures: 10, OpenTimeout: time.Second, HalfOpenProbes: 2},
	}
	return New(cfg, nil, log)
}

func TestClient_HappyPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	client := newTestClient(srv.URL, 0)
	resp, err := client.Do(context.Background(), http.MethodGet, "/", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestClient_RetriesOnServerError(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := newTestClient(srv.URL, 3)
	resp, err := client.Do(context.Background(), http.MethodGet, "/", nil)
	if err != nil {
		t.Fatalf("expected success after retries, got: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
	if attempts.Load() != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts.Load())
	}
}

func TestClient_NoRetryOn4xx(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	client := newTestClient(srv.URL, 3)
	resp, err := client.Do(context.Background(), http.MethodPost, "/", []byte(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
	if attempts.Load() != 1 {
		t.Errorf("4xx must not be retried: got %d attempts", attempts.Load())
	}
}

func TestClient_ContextCancelStopsRetry(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	client := newTestClient(srv.URL, 5)
	_, err := client.Do(ctx, http.MethodGet, "/", nil)
	if err == nil {
		t.Error("expected error on cancelled context")
	}
}

func TestClient_CircuitBreakerPreventsRequests(t *testing.T) {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := Config{
		BaseURL:    "http://127.0.0.1:1",
		Timeout:    100 * time.Millisecond,
		MaxRetries: 0,
		Target:     "test",
		Breaker:    BreakerConfig{MaxFailures: 1, OpenTimeout: time.Minute, HalfOpenProbes: 1},
	}
	client := New(cfg, nil, log)

	_, _ = client.Do(context.Background(), http.MethodGet, "/", nil)

	_, err := client.Do(context.Background(), http.MethodGet, "/", nil)
	if err == nil {
		t.Fatal("expected ErrCircuitOpen")
	}
}
