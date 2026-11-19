# Todos

Notes on state: revised 2026-11-18 to match `docs/cloud-k8s-prep.md` (revised
plan). Items 1–3 were confirmed open in code at `49100ff`; they were surfaced
by the 2026-11-15 mockworker-scaling experiment
(`benchmark/experiments/20261115-mockworker-scaling.yaml`). Items are grouped
by milestone; every fix ships with unit tests covering happy path and
error/edge cases.

## Milestone 0 — local prerequisites (kind, no cloud spend)

### Redis hygiene (from the 2026-11-15 experiment)

- [x] **Expire finished chunk streams.** `internal/worker/runner.go` writes
  `jobs:chunks:<job_id>` with only `MaxLen: 10000`; nothing ever deletes or
  expires the stream. ~80MB/run accumulated; 1.74GB / 238K keys invalidated
  afternoon measurements and OOMKilled Redis mid-run. Fix: `EXPIRE
  jobs:chunks:<job_id>` (5–10 min) when the worker publishes the final `done`
  chunk (or when the API finishes streaming). Per-key TTL, safe during
  concurrent runs. Optional: a `make redis-flush` escape hatch (manual, between
  runs only — kills in-flight jobs and consumer groups). Done 2026-11-18:
  `ChunksTTL` on the runner (default 10m, `WORKER_CHUNKS_TTL_SECONDS`, 0 →
  default, negative disables), `make redis-flush` target added.
- [x] **Scheduler recreates its consumer group.** `scheduler.go` calls
  `XGroupCreateMkStream` once at startup; after a FLUSHDB the scheduler keeps
  erroring on `read jobs` until restarted. Detect `NOGROUP` on the read loop
  and recreate the group (or recreate on every iteration — MkStream is idempotent).
  Done 2026-11-18: `ensureGroup` helper, `NOGROUP` detection recreates the
  group, `dispatch_error{nogroup}` metric.

### Dead-worker reclaim

- [x] Jobs dispatched to a worker that crashes are never reclaimed or
  re-dispatched (`jobs:worker:<id>` streams accumulate; clients hang until
  timeout). Look at `XAUTOCLAIM`/pending inspection on worker streams, driven
  by registry liveness. Done 2026-11-18: minimal reclaim — scheduler pass
  (1s ticker) finds `jobs:worker:*` streams whose registry heartbeat is gone,
  re-dispatches stranded entries via the shared MULTI `place()`, deletes the
  dead stream, `mastori_scheduler_reclaims_total` metric, sets `run:interrupted`.
  Failure behavior still to be covered by the Failure CI kind tests below.

### Run status + results schema v0.1 (decided 2026-11-18)

- [x] Add `status: running|valid|invalid` to the run/result format. A mid-run
  Redis restart, scheduler crash, or unreclaimed worker loss ends the
  experiment with status `invalid`. No Redis PVC on cloud; restart voids the
  run by design. Done 2026-11-18: summary written as `running` at run start,
  rewritten at finalize; `invalid` on any measured-request failure or the
  scheduler's `run:interrupted` flag (warmup-only failures keep the run valid).
- [x] Add `schema_version: 0.1` to every exported result and run manifest.
  Done 2026-11-18: on both JSONL records and the run summary.
- [x] Update the HF results repo writer (benchmark submodule) to carry
  version `0.1` in the same change. Done 2026-11-18: benchmark submodule
  pointer moved to `423ae0f` (schema v0.1 doc on the HF repo, pushed).
- [x] Unit tests: happy path → `valid`, each interruption path → `invalid`,
  version field present in all exports. Done 2026-11-18:
  `internal/traffic/status_test.go`, reclaim tests in `internal/scheduler`.

### Kustomize refactor + pool placement

- [ ] Split `deploy/kubernetes/` into kustomize base + `local` overlay.
  NodePort + `extraPortMappings` stay in the `local` overlay only;
  `make cluster-up && make cluster-deploy` must behave identically on kind.
- [ ] Add `nodeSelector`/tolerations/topology spread to every workload (API,
  Redis, scheduler, mock workers, observability). GPU nodes get the
  `mastori/gpu: true` taint; only inference workloads tolerate it. Verify
  placement on kind with the existing labeled nodes.

### Export / teardown loop

- [ ] Define and implement the export-before-destroy checklist: JSONL
  results, run manifest (provider, instance types, driver, image digests,
  node info), Prometheus snapshot, traces → git results repo. Make targets
  for provision → run → export → destroy.

### Integration tests (scope agreed 2026-11-18)

- [ ] **Fast CI:** render every kustomize overlay, validate schemas, reject
  mutable image tags.
- [ ] **kind CI:** deploy the local overlay, run the existing
  API→Redis→scheduler→worker flow (extend current integration tests).
- [ ] **Failure CI:** restart Redis, scheduler, and workers; assert defined
  recovery behavior and `invalid` statuses.

### Datasets in-cluster cache reuse

- [ ] The traffic Job's `datasets` initContainer re-fetches the full dataset
  cache from Hugging Face on **every run** (~15–25s of per-run overhead over
  the NAT) into a fresh `emptyDir`. Add cache reuse across runs: either a
  small read-only PVC seeded once (a fetcher Job with `if-not-exists`
  semantics), or a S3-backed warm cache, or a built-in image layer refresh
  when the manifest SHA changes. Keyed on the `datasets.yaml` manifest SHA so
  a manifest change invalidates the cache; keep the no-data-on-nodes rule
  (nothing persistent lands on the node filesystem).

### Structured environment capture

- [ ] **Automate provenance capture at experiment start.** The 2026-11-19
  EKS environment snapshot was a manual scrape under time pressure (image
  digest, api-configmap, deployments, node type/capacity grabbed from the
  live cluster right before destroy — the failed-NAT lesson made the window
  unpredictable). Add a `make cluster-env-capture`-style step to the run
  workflow that runs **before the first run of an experiment** and writes
  `results/<experiment>/environment/`: image digests per workload,
  deployment/configmap manifests, node instance types + capacity + labels,
  cluster version/topology, dataset manifest SHA. Generate `PROVENANCE.md`
  from that instead of hand-writing it, so every experiment starts with
  complete provenance by construction (GPU driver/CUDA info joins in
  Milestone 2).

### Local quality (pre-cloud, not blocking)

- [ ] 3 workers regressed vs 2 at 20K requests (dispatch/Redis contention);
  3 workers won on throughput only at 30K. Investigate whether the bottleneck
  is the scheduler loop, worker-list caching (250ms stale), or Redis itself;
  add dispatch-rate metrics if needed.
- [ ] Queue-aware (V2) policy: least-loaded uses stream length only; consider
  queue depth + in-flight-aware dispatch before cloud experiments.
- [ ] **CPU-capped local knee check.** The first EKS run showed the knee
  arriving earlier than on the host (p95 2163ms at 10K/30s vs 1243ms on
  kind; everything shares one m6g.xlarge vs a spread-out M4 Pro). Test the
  contention hypothesis: cap the kind node to m6g.xlarge shape
  (`docker update --cpus 4 --memory 16g kind-control-plane`) and rerun the
  1K/5K/10K loop. If p95 at 10K moves from ~1.2s toward ~2.2s, CPU
  contention is confirmed as the cause; revert with `--cpus 0 --memory 0g`.
- [ ] Rerun the knee sweep with the vllmworker/Ollama inference layer
  (it was 0 replicas in the 2026-11-15 run) so mock findings have a
  real-inference comparison. Same experiment config shape.

## Milestone 1 — AWS, CPU/mock-worker EKS, private (eu-north-1)

- [ ] **Request GPU quota on day 1**, in parallel with everything below
  (new accounts start at zero; approvals can take days).

- [ ] **Verify claims before committing spend:** EKS free-tier terms, GPU
  instance availability and pricing in eu-north-1, with dated calculator links.
- [x] **Terraform (one module, no provider abstraction):** VPC (2 AZ subnets,
  workloads in one AZ), NAT + EIP, EKS control plane, one **on-demand**
  Graviton managed node group (private subnet, no public IPs), security
  groups, IAM roles (cluster, nodes, IRSA). No load balancer — private
  access via port-forward/tunnel, access steps documented in the run checklist.
  Done 2026-11-18 in `2606353`; node group carries the `mastori/pool=cpu`
  label every workload selects (`eks.tf`, 2026-11-18). `terraform plan/apply`
  still to be exercised against a real account.
- [ ] **ECR + CI:** multi-arch image builds (`buildx`), push to ECR,
  deployments pin images by digest; provenance recorded in the run manifest.
  Partial 2026-11-18: multi-arch buildx images publish to **GHCR**
  (`3ab82c7`) — the ECR move (to bound NAT cost, with VPC endpoints) and
  digest pinning are still open; the `aws` overlay meanwhile rewrites to
  `ghcr.io/dbaltas/mastori:latest` with `imagePullPolicy: Always`.
- [x] **`aws` overlay:** IRSA annotations, node sizes, region. Nothing
  app-specific in the overlay. Done 2026-11-18:
  `deploy/kubernetes/overlays/aws` — image rewrite + `Always` pull policy
  only; IRSA deferred (nothing in Milestone 1 calls AWS APIs), node sizes
  stay in Terraform tfvars. The traffic Job lives in
  `overlays/aws/traffic/traffic-job.yaml` and is applied by
  `make aws-run-traffic`; kind still applies the same file raw.
- [x] **In-cluster traffic Job:** `cmd/traffic` as a Job on the CPU pool
  calling the API ClusterIP; limits 1 CPU / 500 Mi. Done 2026-11-18 in
  `bd4aa68` (TRAFFIC_* env config, stdout records, summary provenance SHA).
- [x] **Datasets in-cluster:** fetch dataset prompts inside the cluster
  (fetcher Job on the CPU pool or preloaded), so no data lands on nodes and
  the traffic Job has prompts to replay. Done 2026-11-18: datasets
  fetch + validate initContainers inside the traffic Job (`bd4aa68`).
- [ ] **Cost guardrails:** AWS Budgets alert, documented `make destroy`
  teardown target.
- [ ] **Acceptance:** EKS up, one CPU node, API + Redis + scheduler + mock
  workers healthy, one benchmark run via the in-cluster traffic Job, results
  + manifest + metrics snapshot pushed to the results repo, cluster destroyed,
  results intact. 2026-11-18: `make aws-deploy` / `aws-status` /
  `aws-run-traffic` targets and the fast manifests+terraform CI workflow
  landed; everything else above still needs a real EKS run.

## Milestone 2 — GPU / vLLM on EKS (gated: quota + capacity confirmed)

- [ ] **NVIDIA stack:** GPU Operator (or device plugin), pinned driver/CUDA
  versions, `dcgm-exporter` metrics, DCGM pre-run diagnostics before a GPU
  enters a comparison.
- [ ] **Real vLLM server Deployment:** model weights, `nvidia.com/gpu`
  requests, HF token secret, model cache on PVC or object storage (not
  re-downloaded per pod), GPU startup/readiness probes. `cmd/vllmworker`
  stays a CPU-pool bridge (or sidecar) in front of it.
- [ ] **GPU node group** scales to zero; Budgets alert on; spot allowed only
  with the interrupted-run-is-invalid rule.
- [ ] Run the knee sweep end to end; verify GPU metrics are scraped and the
  run manifest records GPU model, driver, CUDA, instance type.

## Milestone 3 — Azure, concrete

- [ ] Port the same base + `azure` overlay + Terraform for AKS (on-demand
  CPU node group, workload-identity service accounts, ACR). No shared
  abstraction yet.

## Later

- [ ] Extract the provider abstraction from three concrete implementations
  (AWS, Azure, third), then Nebius (GPU-heavy), OVH, and Verda — re-checking
  Verda's pilot status at that point.
- [ ] Cross-provider experiment matrix. The experiment-config format in
  `benchmark/experiments/` and the site/stories pipeline are the
  reproducibility backbone — keep pinning image SHAs, dataset revisions, and
  cluster state per run.
