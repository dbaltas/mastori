package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	Registry        *prometheus.Registry
	RequestsTotal   *prometheus.CounterVec
	RequestDuration *prometheus.HistogramVec
	EnqueueSeconds  prometheus.Histogram
	JobsEnqueued    prometheus.Counter
	EnqueueErrors   prometheus.Counter
	Inflight        prometheus.Gauge
}

func New() *Metrics {
	reg := prometheus.NewRegistry()

	m := &Metrics{
		Registry: reg,
		RequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "mastori",
				Subsystem: "api",
				Name:      "requests_total",
				Help:      "Total HTTP requests by method, route, and status.",
			},
			[]string{"method", "route", "status"},
		),
		RequestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "mastori",
				Subsystem: "api",
				Name:      "request_duration_seconds",
				Help:      "HTTP request latency in seconds.",
				Buckets:   []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
			},
			[]string{"method", "route"},
		),
		EnqueueSeconds: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "mastori",
				Subsystem: "api",
				Name:      "enqueue_seconds",
				Help:      "Time spent enqueueing a job into the Redis stream.",
				Buckets:   []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1},
			},
		),
		JobsEnqueued: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: "mastori",
				Subsystem: "api",
				Name:      "jobs_enqueued_total",
				Help:      "Total jobs successfully enqueued.",
			},
		),
		EnqueueErrors: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: "mastori",
				Subsystem: "api",
				Name:      "enqueue_errors_total",
				Help:      "Total enqueue failures.",
			},
		),
		Inflight: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "mastori",
				Subsystem: "api",
				Name:      "inflight_requests",
				Help:      "Number of requests currently waiting for a result.",
			},
		),
	}

	reg.MustRegister(
		m.RequestsTotal,
		m.RequestDuration,
		m.EnqueueSeconds,
		m.JobsEnqueued,
		m.EnqueueErrors,
		m.Inflight,
	)

	return m
}
