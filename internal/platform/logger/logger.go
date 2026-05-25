package logger

import (
	"context"
	"log/slog"
	"os"
)

type contextKey string

const (
	keyRequestID contextKey = "request_id"
	keyTraceID   contextKey = "trace_id"
)

func New(level, format string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: lvl}

	var handler slog.Handler
	if format == "text" {
		handler = slog.NewTextHandler(os.Stdout, opts)
	} else {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	}

	return slog.New(handler)
}

func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, keyRequestID, requestID)
}

func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, keyTraceID, traceID)
}

func RequestIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(keyRequestID).(string)
	return v
}

func TraceIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(keyTraceID).(string)
	return v
}

func Fields(ctx context.Context) []any {
	var fields []any

	if rid := RequestIDFromContext(ctx); rid != "" {
		fields = append(fields, slog.String("request_id", rid))
	}

	if tid := TraceIDFromContext(ctx); tid != "" {
		fields = append(fields, slog.String("trace_id", tid))
	}

	return fields
}

func FromContext(ctx context.Context, base *slog.Logger) *slog.Logger {
	fields := Fields(ctx)
	if len(fields) == 0 {
		return base
	}
	return base.With(fields...)
}
