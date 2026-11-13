# Mastori

A benchmarking platform for LLM inference on GPU infrastructure.

Run the same LLM workload on identical GPU resources across different providers — AWS, Azure, Nebius, Verda, or any other — and compare throughput, latency, utilization, and cost.

> **Status: early implementation.** The local stack runs end to end with mock workers: a Go API with synchronous and streaming generation, a Redis job queue, a traffic generator (`cmd/traffic`), dataset fetch/validation (`benchmark/workloads`), Prometheus metrics, and Grafana dashboards — deployable on Docker Compose or a local kind cluster. An optional inference profile adds real local generation: a vLLM-compatible adapter (`cmd/adapter`) backed by Ollama plus a real inference worker (`cmd/vllmworker`) that speaks the same API surface a cloud vLLM server will. Scheduling policies and cloud providers are still ahead. See the [design document](gpu-inference-benchmarking-platform.md) for the full plan.

![Grafana dashboard](docs/grafana-overview.png)

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

1. ~~Local stack: Go API → Redis → Go scheduler → mock workers~~
2. ~~Kubernetes with Prometheus and Grafana~~
3. ~~Traffic generator and workload profiles~~
4. ~~Real local inference workers (Ollama behind a vLLM-compatible adapter)~~
5. Cloud deployment (CPU + GPU pools) with IaC
6. Cross-provider experiments

## Running locally

All host ports are in the 7000–7999 range.

### Docker Compose

```sh
make compose-up
```

Runs the API (7080), Redis (7079), the scheduler, two mock workers, and Prometheus (7090). Note: the kind cluster also maps API 7080, so only one stack can own it at a time.

### Real local inference (Ollama)

```sh
make compose-up-inference
```

Adds the `inference` profile on top of the mock stack: Ollama, the vLLM-compatible adapter (`inference-adapter` on 7082), and `vllmworker` instances that pull jobs from the same Redis queue and generate through the adapter. The adapter serves `POST /v1/completions`, `POST /v1/chat/completions`, `GET /v1/models`, `GET /health`, and a `/metrics` endpoint with synthesized `vllm:*` Prometheus metrics (TTFT, TPOT, e2e latency, token counters), so anything written against a vLLM OpenAI server works unchanged. Set `INFERENCE_MODEL=<model>` to pick the model (default `qwen2.5:0.5b`). Mock and inference workers can run side by side; the scheduler routes jobs to both like any other registered worker.

```sh
curl -s localhost:7082/v1/completions -d '{"prompt":"Explain Redis streams","max_tokens":32}' | jq
```

Local runs are functional tests and relative comparisons only — numbers are not comparable to cloud vLLM results and are excluded from cross-provider benchmarks.

### kind

```sh
make cluster-up        # kind create cluster (extraPortMappings: API 7080, Grafana 7081)
make cluster-deploy    # build + load image, apply deploy/kubernetes/
```

The cluster runs API (2 replicas), Redis, the scheduler, mock workers, Prometheus, and Grafana in the `mastori` namespace, plus the OTel collector and Grafana Tempo in `observability`. Dashboards and the Tempo datasource are provisioned automatically; open http://localhost:7081 (anonymous admin login).

### Scheduler

Workers no longer pull from the shared `jobs` stream. The scheduler (`cmd/scheduler`) is its only reader: it picks a live worker with the configured policy and moves the job to that worker's private stream (`jobs:worker:<id>`) in one atomic Redis transaction. Workers read only their own stream and delete an entry when the job finishes.

```
API --XADD--> jobs --scheduler--> jobs:worker:<id> --> worker --XADD--> jobs:chunks:<job_id> --> API
```

- **Registry.** Workers heartbeat a hash `workers:<id>` with a 3-heartbeat TTL (`WORKER_HEARTBEAT_MS`, default 1s). A worker that stops heartbeating drops out of scheduling when the key expires. A gracefully stopped worker deregisters immediately. `WORKER_MODELS` (comma-separated) restricts which models a worker serves; empty serves any.
- **Policies.** `SCHEDULER_POLICY` selects `round-robin` (default) or `least-loaded`. Load is the length of the worker's stream: queued plus running jobs.
- **No workers.** Jobs stay in `jobs` until a worker is available. Jobs older than `SCHEDULER_JOB_TTL` seconds (default 60, 0 disables) are dropped, since the API has already timed out.
- **Metrics** (`:8081/metrics`): `mastori_scheduler_dispatched_total{worker}`, `dispatch_errors_total{reason}`, `jobs_dropped_total{reason}`, `scheduling_seconds`, `queue_wait_seconds`, `queue_depth`, `workers_healthy`, `worker_queue_depth{worker}`.
- **Not yet handled.** Jobs already dispatched to a worker that crashes are not reclaimed or re-dispatched, and a dead worker's stream is not cleaned up. Multiple scheduler replicas are safe (shared consumer group), but each worker-list read is cached for 250ms, so policies see slightly stale membership.

### Tests

```sh
make test              # unit tests (miniredis, no external services)
make test-integration  # API -> Redis -> scheduler -> workers against a real Redis (needs Docker)
```

To add real inference to the kind cluster, run Ollama on the host (`ollama serve` + `ollama pull qwen2.5:0.5b`) and deploy the inference layer:

```sh
make cluster-deploy-inference   # apply deploy/inference/ (adapter + vllmworker)
```

The adapter reaches host Ollama via `host.docker.internal` (see `deploy/inference/adapter.yaml`). The scheduler routes jobs to the `vllmworker` like any other registered worker.

### Metrics

The API exposes Prometheus metrics on `/metrics`: request rate and latency by route, time to first token, enqueue latency, jobs enqueued/errors, in-flight requests, chunks streamed, and busy rejections. The provisioned "Mastori API Overview" dashboard covers all of them.

### Traces

The API emits OpenTelemetry traces to the OTel collector (`deploy/otel/collector-config.yaml`), which forwards them to Grafana Tempo (`deploy/tempo/tempo.yaml`, local storage). In the kind cluster both run in the `observability` namespace (`deploy/kubernetes/observability.yaml`); the cluster Grafana has a provisioned Tempo datasource: open http://localhost:7081 → Explore → Tempo and search for traces from `mastori-api` (e.g. the `api.generate` root span), or paste a `trace_id` from the API logs.

### Datasets

Benchmark prompts live in `benchmark/workloads/datasets.yaml`. Fetch and validate them with:

```sh
go run ./cmd/datasets fetch
go run ./cmd/datasets validate
```

### Traffic generation

`cmd/traffic` replays dataset prompts against the API at a controlled rate and writes per-request results as JSONL plus a run summary:

```sh
go run ./cmd/traffic --workload short --rate 10 --duration 5m
go run ./cmd/traffic --workload mixed --short-percentage 80 --rate 50 --duration 10m
go run ./cmd/traffic --workload mixed --steps "10:60s,50:60s,10:60s"
```

Open loop (`--rate`, constant or poisson arrivals, optional `--ramp-end-rate` or `--steps` patterns) or closed loop (`--concurrency`), streaming SSE by default (`--stream=false` for blocking requests), with warmup, seed, and per-request timeout controls.

## License

[MIT](LICENSE)
