package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

type Metrics struct {
	HTTPRequestsTotal   *prometheus.CounterVec
	HTTPRequestDuration *prometheus.HistogramVec

	EventsProcessedTotal *prometheus.CounterVec
	EventsFailedTotal    *prometheus.CounterVec
	EventsDuplicateTotal prometheus.Counter

	QueueLagMessages *prometheus.GaugeVec

	OutboundRequestsTotal *prometheus.CounterVec

	Registry *prometheus.Registry
}

func New() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	m := &Metrics{
		Registry: reg,

		HTTPRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "platform",
			Subsystem: "http",
			Name:      "requests_total",
			Help:      "Total HTTP requests partitioned by method, path and status code.",
		}, []string{"method", "path", "status"}),

		HTTPRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "platform",
			Subsystem: "http",
			Name:      "request_duration_seconds",
			Help:      "HTTP request latency in seconds.",
			Buckets:   []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
		}, []string{"method", "path"}),

		EventsProcessedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "platform",
			Subsystem: "events",
			Name:      "processed_total",
			Help:      "Total events successfully processed, partitioned by source and status.",
		}, []string{"source", "status"}),

		EventsFailedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "platform",
			Subsystem: "events",
			Name:      "failed_total",
			Help:      "Total events that failed processing, partitioned by source and reason.",
		}, []string{"source", "reason"}),

		EventsDuplicateTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "platform",
			Subsystem: "events",
			Name:      "duplicate_total",
			Help:      "Total duplicate webhook events absorbed by idempotency check.",
		}),

		OutboundRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "platform",
			Subsystem: "http",
			Name:      "outbound_requests_total",
			Help:      "Total outbound HTTP requests made to external services.",
		}, []string{"target", "method", "status"}),

		QueueLagMessages: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "platform",
			Subsystem: "queue",
			Name:      "lag_messages",
			Help:      "Current consumer lag in number of messages, partitioned by topic.",
		}, []string{"topic"}),
	}

	reg.MustRegister(
		m.HTTPRequestsTotal,
		m.HTTPRequestDuration,
		m.EventsProcessedTotal,
		m.EventsFailedTotal,
		m.EventsDuplicateTotal,
		m.QueueLagMessages,
		m.OutboundRequestsTotal,
	)

	return m
}
