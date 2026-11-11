# GPU Inference Benchmarking Platform

The project has two goals:

1. Learn GPU/Kubernetes inference infrastructure hands-on.
2. Build an open-source, reproducible tool that can benchmark the same workload across different GPU providers.

The initial set of target providers includes AWS, Azure, Nebius, and Verda, with the design kept open to any other provider.

Eventually the project should be able to say:

> "Run this exact LLM workload on 2 GPUs at AWS, then run it on 2 GPUs at Nebius, Verda, or any other provider, and compare throughput, latency, utilization, and cost."

## 1. The overall architecture

```
                        Internet
                           │
                           ▼
                 ┌──────────────────┐
                 │ Public API / LB  │
                 └────────┬─────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │ Go Inference API │
                 └────────┬─────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │      Redis       │
                 │   request queue  │
                 └────────┬─────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │  Go Scheduler    │
                 └────────┬─────────┘
                          │
                ┌─────────┴─────────┐
                ▼                   ▼
         ┌─────────────┐     ┌─────────────┐
         │ vLLM Pod 1  │     │ vLLM Pod 2  │
         │ GPU Node 1  │     │ GPU Node 2  │
         └─────────────┘     └─────────────┘
                │                   │
                ▼                   ▼
             NVIDIA GPU          NVIDIA GPU
```

And alongside all of this:

```
Go API ───────┐
Scheduler ────┤
Redis ────────┤
vLLM ─────────┤──► Prometheus ──► Grafana
GPU metrics ──┘

              OpenTelemetry
                   │
                   ▼
                 Traces
```

## 2. Kubernetes

Kubernetes manages the infrastructure, not individual prompts.

A Pod is not created for every request.

Instead, there are long-running vLLM Pods:

```
GPU node 1
└── vLLM Pod
    └── Model loaded in GPU memory

GPU node 2
└── vLLM Pod
    └── Model loaded in GPU memory
```

Kubernetes makes sure those Pods stay alive and are placed on GPU-capable nodes.

If a vLLM Pod dies:

```
vLLM dies
   ↓
Kubernetes notices
   ↓
Pod restarted/rescheduled
   ↓
model loaded again
```

Individual prompts go directly to already-running vLLM workers.

## 3. vLLM

vLLM is the inference engine.

It stays running and handles many concurrent requests.

It has its own internal request scheduling and continuous batching.

So there are two levels:

```
The system
────────────────────
Redis
   ↓
Go scheduler
   ↓
vLLM replica
   ↓
vLLM's internal scheduler
   ↓
GPU
```

The scheduler doesn't micromanage individual GPU operations.

Its job is more like:

> "Which healthy vLLM replica should receive this request?"

vLLM's job is:

> "How do I efficiently execute these requests on this GPU?"

## 4. The Go API

The API is the public entry point.

Something simple like:

```
POST /generate
```

with:

```json
{
  "prompt": "Explain Kubernetes scheduling",
  "model": "my-model"
}
```

The API:

1. Generates a request/trace ID.
2. Records metrics/tracing.
3. Puts the job into Redis.
4. Waits for the result.
5. Returns the response.

It doesn't need to know anything about GPUs.

## 5. Redis

Redis is the application-level queue.

For V1, Redis Streams is a good fit.

A job could contain:

- `job_id`
- `trace_id`
- `prompt`
- `model`
- `created_at`
- `priority`

Redis is where requests wait before the scheduler dispatches them.

This is separate from vLLM's internal queue.

## 6. The Go scheduler

This is probably the most interesting part of the project.

It is not the Kubernetes scheduler.

There are two schedulers:

- **Kubernetes scheduler** — answers: *"Which Kubernetes node should this Pod run on?"*
- **The project's scheduler** — answers: *"Which inference worker should receive this request?"*

Initially, keep the scheduling policy very simple.

### V1: Round-robin among healthy workers

```
request 1 → vLLM 1
request 2 → vLLM 2
request 3 → vLLM 1
request 4 → vLLM 2
```

### V2: Least-loaded / queue-aware

```
vLLM 1 → 18 active
vLLM 2 →  4 active

next request → vLLM 2
```

Then the policies can be compared experimentally.

## 7. How does the scheduler know worker state?

Several signals are available.

Kubernetes gives Pod state:

- Running
- Ready
- NotReady

Health/readiness endpoints on vLLM can also be used.

The scheduler can maintain something like:

```
Worker       Health       Load
--------------------------------
vLLM-1       healthy       18
vLLM-2       healthy        4
```

Initially, don't try to perfectly determine whether a GPU is "free."

vLLM itself handles concurrency and batching.

Later, the scheduler can become more sophisticated using metrics such as active requests, queue depth, KV-cache utilization, etc.

## 8. Local development

A huge amount can be done locally before paying for GPUs.

On a local machine:

```
Kind / Minikube
│
├── Go API
├── Go Scheduler
├── Redis
├── Prometheus
├── Grafana
└── Mock workers / local LLM
```

Start with mock workers.

For example, a fake worker can pretend:

```
request received
wait 100 ms
generate 500 tokens
return response
```

That lets the following be developed:

- API
- Redis
- scheduler
- Kubernetes
- health checks
- retries
- scaling
- metrics
- tracing
- dashboards
- traffic generation

...without spending money.

A locally served LLM can be added on top of this as well.

The important thing is that the scheduler doesn't depend on NVIDIA-specific functionality.

## 9. Cloud architecture

When moving to AWS, Nebius, Verda, or another GPU provider, use separate CPU and GPU worker pools.

```
                 Kubernetes
                     │
       ┌─────────────┴──────────────┐
       │                            │
       ▼                            ▼
 CPU worker pool               GPU worker pool
       │                            │
       ├── Go API                   ├── vLLM
       ├── Scheduler                ├── vLLM
       ├── Redis                    └── ...
       ├── Prometheus
       └── Grafana
```

Don't waste GPU resources on Prometheus, Grafana, the API, etc.

And don't expose GPU nodes directly to the Internet.

## 10. Networking

Something like:

```
                 Internet
                    │
                    ▼
             Public Load Balancer
                    │
                    ▼
                 Go API
                    │
             private network
                    │
        ┌───────────┼───────────┐
        ▼           ▼           ▼
     Redis      Scheduler     vLLM
                              GPU 1
                              GPU 2
```

Only the public API needs to be externally accessible.

GPU nodes can have private IPs.

For administration, use appropriate secure access such as a VPN, bastion, or cloud-native management tooling rather than exposing SSH everywhere.

## 11. Observability

This should be a major part of the project.

### Prometheus

Collects time-series metrics from:

- Go API
- Go scheduler
- Redis
- vLLM
- Kubernetes
- GPUs

### Grafana

Visualizes them.

### OpenTelemetry

Provides distributed traces.

A request could look like:

```
trace abc123

Go API              4 ms
   │
Redis wait        240 ms
   │
Scheduler            3 ms
   │
vLLM queue         180 ms
   │
TTFT               420 ms
   │
Generation        1.82 s
   │
Total             2.66 s
```

That is much more valuable than simply looking at GPU utilization.

## 12. Metrics to collect

At minimum:

### API

- requests/sec
- errors/sec
- request latency
- p50/p95/p99

### Queue

- queue depth
- queue wait time
- jobs processed
- jobs failed

### Scheduler

- scheduling latency
- dispatch count
- selected worker
- worker failures

### Inference

- TTFT
- generation latency
- input tokens/sec
- output tokens/sec
- total tokens/sec
- concurrent requests

### GPU

- GPU utilization
- GPU memory
- temperature/power if available
- GPU-level throughput metrics

And then correlate them.

## 13. Traffic generator

The benchmark traffic is generated by a configurable client.

For example:

```yaml
rate: 50
duration: 300

workload:
  type: mixed
  short_percentage: 80
  long_percentage: 20
```

And support different workloads:

- tiny
- short
- medium
- long
- mixed
- burst

Varyable parameters:

- input tokens
- output tokens
- request rate
- concurrency
- duration

This lets the project simulate things like:

- 10 req/s × short prompts
- 50 req/s × short prompts
- 10 req/s × long prompts
- 80% short + 20% long
- sudden 10x traffic burst

## 14. Experiments

This is where the project becomes genuinely useful.

For example:

### Experiment 1 — baseline

- 2 GPU nodes
- 1 model replica per GPU
- round-robin
- 20 req/s

Measure everything.

### Experiment 2 — increase traffic

20 → 40 → 80 → 160 req/s

Find the saturation point.

### Experiment 3 — mixed workload

- 80% short
- 20% long

Measure impact on p95 latency.

### Experiment 4 — scheduler comparison

Round robin vs Least loaded vs Queue-aware

Compare:

- throughput
- p95
- p99
- GPU utilization
- queue wait

### Experiment 5 — scaling

1 GPU → 2 GPUs → 4 GPUs

Measure how throughput and latency change.

### Experiment 6 — quantization

Compare something like:

- FP16
- FP8 / quantized variant

while measuring both:

- performance
- memory
- quality

## 15. Quality benchmarking

Datasets such as MMLU and similar benchmarks are also relevant.

### Where to get input prompts / test data

- [MMLU](https://huggingface.co/datasets/cais/mmlu) — multiple-choice knowledge questions; good for quality evaluation
- [ShareGPT](https://huggingface.co/datasets/anon8231489123/sharegpt_vicuna) — real multi-turn conversations; good for realistic serving workloads
- [LMSYS-Chat-1M](https://huggingface.co/datasets/lmsys/lmsys-chat-1m) — 1M real-world prompts and chat sessions; good for traffic replay
- [MT-Bench](https://huggingface.co/datasets/philschmid/mt-bench) — 80 multi-turn questions across 8 categories; good for quick quality checks
- [OpenOrca / Alpaca](https://huggingface.co/datasets/Open-Orca/OpenOrca) — instruction-following prompts; good for generating varied prompt lengths

For performance benchmarking, the input prompts and expected output token counts come from the workload profiles (§13); for quality benchmarking, the above datasets are used.

Keep quality evaluation separate from the performance benchmark.

The performance system asks:

> How fast and efficiently can this workload be served?

The quality benchmark asks:

> Did changing precision/model configuration materially affect model quality?

The results can be combined later:

```
                 FP16          FP8
---------------------------------------
tokens/sec       1,200        1,750
GPU memory       20 GB        13 GB
p95 latency      1.8 s        1.2 s
quality           82%          81%
cost / 1M tok    $X           $Y
```

That becomes a very interesting engineering tradeoff.

## 16. The provider abstraction

This is a particularly valuable part of the idea.

The benchmark shouldn't care whether infrastructure comes from AWS, Azure, Nebius, Verda, or any other provider.

Conceptually:

```
                 Benchmark
                     │
              Provider interface
               /       |        \
             AWS   Nebius    Verda ...
              │       │          │
          Terraform Terraform Terraform
              │       │          │
              └───────┴────┬─────┘
                           ▼
                      Kubernetes
```

An experiment configuration could eventually look like:

```yaml
provider: aws

cluster:
  gpu_count: 2

model:
  name: ...

workload:
  type: mixed
  short_percentage: 80
  long_percentage: 20

load:
  rate: 50
  duration: 3600

scheduler:
  policy: queue-aware
```

Then change:

```yaml
provider: nebius
```

or:

```yaml
provider: verda
```

and run the same experiment.

That gives reproducibility.

## 17. The eventual CLI

Don't implement this first, but this is the direction to aim for:

```bash
mastori provision \
  --provider aws \
  --gpu-count 2
```

Then:

```bash
mastori benchmark \
  --experiment experiments/mixed-workload.yaml
```

Then:

```bash
mastori results
```

And finally:

```bash
mastori destroy
```

The ideal experience is:

> one command → infrastructure → Kubernetes → model → observability → traffic → results → teardown.

## 18. Repository structure

A good starting point:

```
mastori/
│
├── cmd/
│   ├── api/
│   ├── scheduler/
│   └── benchmark/
│
├── internal/
│   ├── api/
│   ├── scheduler/
│   ├── queue/
│   ├── workers/
│   ├── metrics/
│   └── tracing/
│
├── deploy/
│   ├── kubernetes/
│   ├── prometheus/
│   ├── grafana/
│   └── vllm/
│
├── infrastructure/
│   ├── aws/
│   ├── azure/
│   ├── nebius/
│   └── verda/
│
├── benchmark/
│   ├── workloads/
│   └── traffic-generator/
│
├── experiments/
│
├── dashboards/
│
└── README.md
```

Don't worry if this evolves. The important thing is keeping provider infrastructure, Kubernetes deployment, scheduling, inference, and benchmarking separate.

## 19. Most importantly: don't build all of this at once

Follow this sequence.

### Phase 0 — completely local

```
Go API
   ↓
Redis
   ↓
Go Scheduler
   ↓
2 fake workers
```

Get requests flowing.

### Phase 1 — Kubernetes

Put all of that into a local Kubernetes cluster.

Add:

- Prometheus
- Grafana

and make the dashboards work.

### Phase 2 — real local inference

Use a locally served model where practical.

Replace:

```
fake worker
```

with:

```
real inference worker
```

### Phase 3 — benchmark system

Build the traffic generator and workload profiles.

### Phase 4 — cloud

Provision:

- CPU workers
- GPU workers

with IaC.

Deploy:

- Go API
- Scheduler
- Redis
- Prometheus
- Grafana
- vLLM

### Phase 5 — real experiments

Run:

- 1 GPU → 2 GPU → 4 GPU

and:

- round-robin → least-loaded → queue-aware

### Phase 6 — second provider

Run the exact same experiments somewhere else.

That's when the provider abstraction starts paying off.

## And the end goal

A repository that can be cloned and used like:

```bash
mastori provision --provider aws --gpus 2

mastori run experiments/mixed-workload.yaml

mastori results

mastori destroy
```

and that produces a Grafana dashboard plus a reproducible benchmark report, is a serious demonstration of real GPU infrastructure capability.

It demonstrates much more than "I know Kubernetes" or "I ran an LLM."

It demonstrates understanding of:

- Kubernetes
- GPU infrastructure
- inference serving
- scheduling
- distributed systems
- observability
- benchmarking
- performance analysis
- cloud infrastructure
- cost/performance tradeoffs
