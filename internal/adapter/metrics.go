package adapter

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics synthesizes the vLLM Prometheus metric surface so dashboards and
// scrapers built for a real vLLM server keep working against the adapter.
// Metric names and labels follow vLLM's OpenAI server.
type Metrics struct {
	Registry              *prometheus.Registry
	TTFTSeconds           *prometheus.HistogramVec
	TimePerOutputToken    *prometheus.HistogramVec
	E2ERequestLatency     *prometheus.HistogramVec
	PromptTokensTotal     *prometheus.CounterVec
	GenerationTokensTotal *prometheus.CounterVec
	RequestSuccessTotal   *prometheus.CounterVec
	NumRequestsRunning    *prometheus.GaugeVec
	NumRequestsWaiting    *prometheus.GaugeVec
}

func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()

	m := &Metrics{
		Registry: reg,
		TTFTSeconds: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "vllm",
				Name:      "time_to_first_token_seconds",
				Help:      "Time from request received to first generated token, in seconds.",
				Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120},
			},
			[]string{"model_name"},
		),
		TimePerOutputToken: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "vllm",
				Name:      "time_per_output_token_seconds",
				Help:      "Time per output generation token, in seconds.",
				Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
			},
			[]string{"model_name"},
		),
		E2ERequestLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "vllm",
				Name:      "e2e_request_latency_seconds",
				Help:      "End-to-end request latency, in seconds.",
				Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300},
			},
			[]string{"model_name"},
		),
		PromptTokensTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "vllm",
				Name:      "prompt_tokens_total",
				Help:      "Number of prefill tokens processed.",
			},
			[]string{"model_name"},
		),
		GenerationTokensTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "vllm",
				Name:      "generation_tokens_total",
				Help:      "Number of generated output tokens.",
			},
			[]string{"model_name"},
		),
		RequestSuccessTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "vllm",
				Name:      "request_success_total",
				Help:      "Count of successfully processed requests.",
			},
			[]string{"model_name"},
		),
		NumRequestsRunning: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "vllm",
				Name:      "num_requests_running",
				Help:      "Number of requests currently being processed.",
			},
			[]string{"model_name"},
		),
		NumRequestsWaiting: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "vllm",
				Name:      "num_requests_waiting",
				Help:      "Number of requests waiting to be processed.",
			},
			[]string{"model_name"},
		),
	}

	m.NumRequestsRunning.WithLabelValues("").Set(0)
	m.NumRequestsWaiting.WithLabelValues("").Set(0)

	reg.MustRegister(
		m.TTFTSeconds,
		m.TimePerOutputToken,
		m.E2ERequestLatency,
		m.PromptTokensTotal,
		m.GenerationTokensTotal,
		m.RequestSuccessTotal,
		m.NumRequestsRunning,
		m.NumRequestsWaiting,
	)

	return m
}
