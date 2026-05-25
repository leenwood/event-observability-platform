package handler

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/leenwood/event-observability-platform/internal/core/dto"
	"github.com/leenwood/event-observability-platform/internal/platform/logger"
	"github.com/leenwood/event-observability-platform/internal/platform/metrics"
	"github.com/leenwood/event-observability-platform/internal/core/port"
	"github.com/leenwood/event-observability-platform/internal/core/service"
	"github.com/leenwood/event-observability-platform/internal/core/usecase"
)

var webhookTracer = otel.Tracer("http/handler/webhook")

type WebhookHandler struct {
	ingestEvent *usecase.IngestEvent
	idem        port.IdempotencyStore
	metrics     *metrics.Metrics
	log         *slog.Logger
	idemTTL     time.Duration
}

func NewWebhookHandler(
	ingestEvent *usecase.IngestEvent,
	idem port.IdempotencyStore,
	m *metrics.Metrics,
	log *slog.Logger,
	idemTTL time.Duration,
) *WebhookHandler {
	return &WebhookHandler{
		ingestEvent: ingestEvent,
		idem:        idem,
		metrics:     m,
		log:         log,
		idemTTL:     idemTTL,
	}
}

type webhookRequest struct {
	IdempotencyKey string          `json:"idempotency_key"`
	Source         string          `json:"source"`
	EventType      string          `json:"event_type"`
	Payload        json.RawMessage `json:"payload"`
}

func (r *webhookRequest) validate() error {
	var missing []string
	if strings.TrimSpace(r.IdempotencyKey) == "" {
		missing = append(missing, "idempotency_key")
	}
	if strings.TrimSpace(r.Source) == "" {
		missing = append(missing, "source")
	}
	if strings.TrimSpace(r.EventType) == "" {
		missing = append(missing, "event_type")
	}
	if len(r.Payload) == 0 || string(r.Payload) == "null" {
		missing = append(missing, "payload")
	}
	if len(missing) > 0 {
		return errors.New("missing required fields: " + strings.Join(missing, ", "))
	}
	return nil
}

type webhookResponse struct {
	EventID string `json:"event_id"`
	TraceID string `json:"trace_id,omitempty"`
}

// HandleEvent ingests a webhook event.
//
// @Summary  Ingest webhook event
// @Description Validates and deduplicates the request, persists the event to PostgreSQL,
// @Description then publishes it to the Kafka processing queue.
// @Description Duplicate requests (same idempotency_key within TTL) return the original response.
// @Tags     webhooks
// @Accept   json
// @Produce  json
// @Param    request  body      webhookRequest   true  "Webhook event payload"
// @Success  202      {object}  webhookResponse  "Event accepted for processing"
// @Success  200      {object}  webhookResponse  "Duplicate — replayed from idempotency store (X-Idempotent-Replayed: true)"
// @Failure  400      {object}  errorResponse    "Invalid JSON"
// @Failure  415      {object}  errorResponse    "Unsupported Content-Type"
// @Failure  422      {object}  errorResponse    "Missing required fields"
// @Failure  500      {object}  errorResponse    "Internal http error"
// @Router   /webhooks/events [post]
// Requires Content-Type: application/json.
func (h *WebhookHandler) HandleEvent(w http.ResponseWriter, r *http.Request) {
	ctx, span := webhookTracer.Start(r.Context(), "WebhookHandler.HandleEvent")
	defer span.End()

	log := logger.FromContext(ctx, h.log)

	if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		respondError(w, http.StatusUnsupportedMediaType, "INVALID_CONTENT_TYPE",
			"Content-Type must be application/json")
		return
	}

	var req webhookRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "invalid json")
		respondError(w, http.StatusBadRequest, "INVALID_JSON", "request body is not valid JSON")
		return
	}

	if err := req.validate(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "validation failed")
		respondError(w, http.StatusUnprocessableEntity, "VALIDATION_ERROR", err.Error())
		return
	}

	span.SetAttributes(
		attribute.String("event.source", req.Source),
		attribute.String("event.type", req.EventType),
		attribute.String("event.idempotency_key", req.IdempotencyKey),
	)

	// Idempotency check — transport-level concern.
	existing, err := h.idem.Get(ctx, req.IdempotencyKey)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "idempotency check failed")
		log.ErrorContext(ctx, "idempotency check failed", slog.String("error", err.Error()))
		respondError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to process request")
		return
	}

	if existing != nil {
		h.metrics.EventsDuplicateTotal.Inc()
		span.SetAttributes(attribute.Bool("event.duplicate", true))
		log.InfoContext(ctx, "duplicate event absorbed",
			slog.String("idempotency_key", req.IdempotencyKey),
			slog.String("event_id", existing.EventID),
		)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Idempotent-Replayed", "true")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(existing.Response)
		return
	}

	// Business logic — delegate to the use case.
	out, err := h.ingestEvent.Execute(ctx, usecase.IngestEventInput{
		IdempotencyKey: req.IdempotencyKey,
		Source:         req.Source,
		EventType:      req.EventType,
		Payload:        []byte(req.Payload),
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "ingest failed")
		log.ErrorContext(ctx, "failed to ingest event", slog.String("error", err.Error()))
		respondError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to process request")
		return
	}

	traceID := trace.SpanFromContext(ctx).SpanContext().TraceID().String()
	resp := webhookResponse{EventID: out.EventID, TraceID: traceID}
	respBytes, _ := json.Marshal(resp)

	// Cache response for future duplicate detection — transport-level concern.
	if err := h.idem.Set(ctx, req.IdempotencyKey, &dto.Entry{
		EventID:   out.EventID,
		Response:  respBytes,
		ExpiresAt: service.TTL(h.idemTTL),
	}); err != nil {
		log.WarnContext(ctx, "failed to store idempotency key",
			slog.String("error", err.Error()),
			slog.String("idempotency_key", req.IdempotencyKey),
		)
	}

	h.metrics.EventsProcessedTotal.WithLabelValues(req.Source, "accepted").Inc()

	span.SetAttributes(attribute.String("event.id", out.EventID))
	span.SetStatus(codes.Ok, "")

	log.InfoContext(ctx, "event accepted",
		slog.String("event_id", out.EventID),
		slog.String("source", req.Source),
		slog.String("event_type", req.EventType),
	)

	writeJSON(w, http.StatusAccepted, resp)
}
