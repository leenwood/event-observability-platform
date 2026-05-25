package httpclient

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/leenwood/event-observability-platform/internal/platform/metrics"
)

var clientTracer = otel.Tracer("outbound/httpclient")

// Response is the simplified result of an outbound HTTP call.
type Response struct {
	StatusCode int
	Body       []byte
	Headers    http.Header
}

// Config configures the Client.
type Config struct {
	BaseURL        string
	Timeout        time.Duration
	MaxRetries     int
	DefaultHeaders map[string]string
	Breaker        BreakerConfig
	// Target is a short label used in metrics (e.g. "notifications-api").
	Target string
}

func DefaultConfig(baseURL, target string) Config {
	return Config{
		BaseURL:    baseURL,
		Target:     target,
		Timeout:    10 * time.Second,
		MaxRetries: 3,
		Breaker:    defaultBreakerConfig(),
	}
}

// Client is a resilient HTTP client with retry, circuit breaker, and OTel tracing.
type Client struct {
	http       *http.Client
	baseURL    string
	headers    map[string]string
	maxRetries int
	breaker    *CircuitBreaker
	metrics    *metrics.Metrics
	target     string
	log        *slog.Logger
}

func New(cfg Config, m *metrics.Metrics, log *slog.Logger) *Client {
	cb := NewCircuitBreaker(cfg.Breaker, func(from, to string) {
		log.Warn("circuit breaker state changed",
			slog.String("target", cfg.Target),
			slog.String("from", from),
			slog.String("to", to),
		)
	})

	return &Client{
		http:       &http.Client{Timeout: cfg.Timeout},
		baseURL:    cfg.BaseURL,
		headers:    cfg.DefaultHeaders,
		maxRetries: cfg.MaxRetries,
		breaker:    cb,
		metrics:    m,
		target:     cfg.Target,
		log:        log,
	}
}

// Do performs method on path (relative to BaseURL) with optional body.
// It retries on transient errors using exponential backoff with jitter,
// and enforces the circuit breaker before each attempt.
func (c *Client) Do(ctx context.Context, method, path string, body []byte) (*Response, error) {
	ctx, span := clientTracer.Start(ctx, fmt.Sprintf("%s %s", method, path),
		trace.WithSpanKind(trace.SpanKindClient),
	)
	defer span.End()

	url := c.baseURL + path
	span.SetAttributes(
		attribute.String("http.method", method),
		attribute.String("http.url", url),
		attribute.String("http.target", c.target),
	)

	var (
		resp *Response
		err  error
	)

	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			delay := backoff(attempt - 1)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}

		if cbErr := c.breaker.Allow(); cbErr != nil {
			span.RecordError(cbErr)
			span.SetStatus(codes.Error, "circuit open")
			return nil, cbErr
		}

		resp, err = c.doOnce(ctx, method, url, body)
		if err != nil {
			c.breaker.RecordFailure()
			if attempt == c.maxRetries {
				break
			}
			c.log.WarnContext(ctx, "request failed, will retry",
				slog.String("target", c.target),
				slog.String("error", err.Error()),
				slog.Int("attempt", attempt+1),
			)
			continue
		}

		if retryableStatus(resp.StatusCode) && attempt < c.maxRetries {
			c.breaker.RecordFailure()
			c.log.WarnContext(ctx, "retryable status, will retry",
				slog.String("target", c.target),
				slog.Int("status", resp.StatusCode),
				slog.Int("attempt", attempt+1),
			)
			continue
		}

		c.breaker.RecordSuccess()
		break
	}

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		c.recordMetric(method, "error")
		return nil, fmt.Errorf("http client %s %s: %w", method, url, err)
	}

	statusStr := strconv.Itoa(resp.StatusCode)
	span.SetAttributes(attribute.Int("http.status_code", resp.StatusCode))
	c.recordMetric(method, statusStr)

	return resp, nil
}

func (c *Client) doOnce(ctx context.Context, method, url string, body []byte) (*Response, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	for k, v := range c.headers {
		req.Header.Set(k, v)
	}

	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	raw, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = raw.Body.Close() }()

	respBody, err := io.ReadAll(raw.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	return &Response{
		StatusCode: raw.StatusCode,
		Body:       respBody,
		Headers:    raw.Header,
	}, nil
}

func (c *Client) recordMetric(method, status string) {
	if c.metrics != nil {
		c.metrics.OutboundRequestsTotal.WithLabelValues(c.target, method, status).Inc()
	}
}
