package middleware

import (
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	"github.com/leenwood/event-observability-platform/internal/pkg/platform/logger"
	"github.com/leenwood/event-observability-platform/internal/pkg/platform/metrics"
)

func Chain(h http.Handler, middlewares ...func(http.Handler) http.Handler) http.Handler {
	for i := len(middlewares) - 1; i >= 0; i-- {
		h = middlewares[i](h)
	}
	return h
}

func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get("X-Request-ID")
		if requestID == "" {
			requestID = uuid.NewString()
		}

		ctx := logger.WithRequestID(r.Context(), requestID)
		w.Header().Set("X-Request-ID", requestID)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func Logger(base *slog.Logger, m *metrics.Metrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rw := &responseWriter{ResponseWriter: w, status: http.StatusOK}

			// Inject trace_id from OTel span before the handler runs
			// so all log calls within the handler carry it automatically.
			ctx := r.Context()
			if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
				ctx = logger.WithTraceID(ctx, span.SpanContext().TraceID().String())
				r = r.WithContext(ctx)
			}

			next.ServeHTTP(rw, r)

			duration := time.Since(start)

			logger.FromContext(ctx, base).InfoContext(ctx, "request",
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", rw.status),
				slog.Duration("latency", duration),
				slog.String("remote_addr", r.RemoteAddr),
			)

			if m != nil {
				statusStr := strconv.Itoa(rw.status)
				m.HTTPRequestsTotal.WithLabelValues(r.Method, r.URL.Path, statusStr).Inc()
				m.HTTPRequestDuration.WithLabelValues(r.Method, r.URL.Path).Observe(duration.Seconds())
			}
		})
	}
}

func Recover(base *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					logger.FromContext(r.Context(), base).ErrorContext(r.Context(), "panic recovered",
						slog.Any("panic", rec),
						slog.String("path", r.URL.Path),
						slog.String("stack", string(debug.Stack())),
					)
					w.Header().Set("Content-Type", "application/json")
					http.Error(w, `{"error":"internal http error"}`, http.StatusInternalServerError)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

func MaxBodySize(maxBytes int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
			next.ServeHTTP(w, r)
		})
	}
}

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(status int) {
	rw.status = status
	rw.ResponseWriter.WriteHeader(status)
}
