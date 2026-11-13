package scheduler

import "github.com/prometheus/client_golang/prometheus"

// Metrics are the scheduler's Prometheus instruments, on a private registry.
type Metrics struct {
	Registry *prometheus.Registry

	Dispatched        *prometheus.CounterVec // by worker
	DispatchErrors    *prometheus.CounterVec // by reason: no_worker, redis
	JobsDropped       *prometheus.CounterVec // by reason: expired, malformed
	SchedulingSeconds prometheus.Histogram   // read from queue -> dispatched
	QueueWaitSeconds  prometheus.Histogram   // job created_at -> dispatched
	QueueDepth        prometheus.Gauge       // jobs waiting for a worker
	WorkersHealthy    prometheus.Gauge       // workers with a live heartbeat
	WorkerQueueDepth  *prometheus.GaugeVec   // by worker: dispatched, unfinished
}

var latencyBuckets = []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}

func NewMetrics() *Metrics {
	m := &Metrics{
		Registry: prometheus.NewRegistry(),
		Dispatched: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "mastori", Subsystem: "scheduler", Name: "dispatched_total",
			Help: "Jobs dispatched, by selected worker.",
		}, []string{"worker"}),
		DispatchErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "mastori", Subsystem: "scheduler", Name: "dispatch_errors_total",
			Help: "Failed dispatch attempts, by reason.",
		}, []string{"reason"}),
		JobsDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "mastori", Subsystem: "scheduler", Name: "jobs_dropped_total",
			Help: "Jobs removed from the queue without being dispatched, by reason.",
		}, []string{"reason"}),
		SchedulingSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "mastori", Subsystem: "scheduler", Name: "scheduling_seconds",
			Help:    "Time from the scheduler reading a job to dispatching it to a worker.",
			Buckets: latencyBuckets,
		}),
		QueueWaitSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "mastori", Subsystem: "scheduler", Name: "queue_wait_seconds",
			Help:    "Time from job creation by the API to dispatch to a worker.",
			Buckets: latencyBuckets,
		}),
		QueueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "mastori", Subsystem: "scheduler", Name: "queue_depth",
			Help: "Jobs in the jobs stream that have not been dispatched yet.",
		}),
		WorkersHealthy: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "mastori", Subsystem: "scheduler", Name: "workers_healthy",
			Help: "Workers with a live heartbeat.",
		}),
		WorkerQueueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "mastori", Subsystem: "scheduler", Name: "worker_queue_depth",
			Help: "Jobs dispatched to a worker that it has not finished yet.",
		}, []string{"worker"}),
	}
	m.Registry.MustRegister(
		m.Dispatched, m.DispatchErrors, m.JobsDropped,
		m.SchedulingSeconds, m.QueueWaitSeconds,
		m.QueueDepth, m.WorkersHealthy, m.WorkerQueueDepth,
	)
	return m
}
