# Mastori

A benchmarking platform for LLM inference on GPU infrastructure.

Run the same LLM workload on identical GPU resources across different providers — AWS, Azure, Nebius, Verda, or any other — and compare throughput, latency, utilization, and cost.

> **Status: early design phase.** Nothing is implemented yet. See the [design document](gpu-inference-benchmarking-platform.md) for the full plan.

## What it will do

- Serve LLM workloads on Kubernetes with vLLM workers
- Route requests through a Go API, Redis queue, and a Go scheduler with pluggable policies (round-robin, least-loaded, queue-aware)
- Generate configurable traffic (rate, duration, workload mix) for repeatable experiments
- Collect metrics from the API, scheduler, queue, vLLM, and GPUs via Prometheus and Grafana
- Abstract the provider behind a common interface so experiments run unchanged on different GPU clouds

## Planned experiments

- Baseline serving and saturation point
- Mixed workloads and their p95/p99 impact
- Scheduler policy comparison
- GPU scaling (1 → 2 → 4 GPUs)
- Quantization tradeoffs (FP16 vs FP8: performance, memory, quality, cost)
- The same experiments across providers

## Roadmap

1. Local stack: Go API → Redis → Go scheduler → mock workers
2. Kubernetes with Prometheus and Grafana
3. Real local inference workers
4. Traffic generator and workload profiles
5. Cloud deployment (CPU + GPU pools) with IaC
6. Cross-provider experiments

## License

[MIT](LICENSE)
