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

- [x] Split `deploy/kubernetes/` into kustomize base + `local` overlay.
  NodePort + `extraPortMappings` stay in the `local` overlay only;
  `make cluster-up && make cluster-deploy` must behave identically on kind.
  Done 2026-11-19: `deploy/kubernetes/base` + `overlays/{local,inference,aws}`.
- [x] Add `nodeSelector`/tolerations/topology spread to every workload (API,
  Redis, scheduler, mock workers, observability). GPU nodes get the
  `mastori/gpu: true` taint; only inference workloads tolerate it. Verify
  placement on kind with the existing labeled nodes. Done 2026-11-19
  (kustomize refactor commit); GPU-pool taints applied by `make cluster-up`.
- [x] Fake NVIDIA devices for the kind GPU pool: `cmd/fakegpu` +
  `internal/fakegpu` DaemonSet (`overlays/inference/fakegpu.yaml`) patches
  node status allocatable with `nvidia.com/gpu` and re-asserts it every
  2s (kubelet rewrites node status ~every 10s and drops extended resources
  it does not manage); `vllmworker` requests/limits
  `nvidia.com/gpu: 1`, replicas 1. Unit tests in `internal/fakegpu`;
  fast CI renders the overlay (`make kustomize-verify`). Done 2026-11-19.

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

- [x] The traffic Job's `datasets` initContainer re-fetches the full dataset
  cache from Hugging Face on **every run** (~15–25s of per-run overhead over
  the NAT) into a fresh `emptyDir`. Done 2026-11-21 (S3-backed remote cache):
  `/datasets prepare` restores a single tar.gz pack from the private S3
  bucket keyed on the `datasets.yaml` manifest SHA
  (`datasets/<manifestSHA>/cache.tar.gz` + `.sha256` sidecar); on a miss it
  fetches from HF, packs, and uploads (first run pays the fetch once). S3 VPC
  gateway endpoint in Terraform keeps in-region pulls free of NAT processing
  charges; IRSA role for the `traffic` ServiceAccount (read+write, this
  bucket only); 90-day expiry reclaims stale manifest versions. Kind/local
  runs keep the direct fetch (`DATASETS_S3_BUCKET` unset). Unit tests with an
  in-memory store (`internal/datasets/store_test.go`) and an S3-API
  integration test against throwaway LocalStack (`make test-integration`,
  `test/integration/datasets_cache_test.go`).

### Structured environment capture

- [x] **Automate provenance capture at experiment start.** The 2026-11-19
  EKS environment snapshot was a manual scrape under time pressure (image
  digest, api-configmap, deployments, node type/capacity grabbed from the
  live cluster right before destroy — the failed-NAT lesson made the window
  unpredictable). Done as `make cluster-env-capture EXPERIMENT=<name>`:
  raw kubectl dumps (nodes, pods, configmaps, image digests, kubectl
  version, host info, dataset manifest SHA) into
  `benchmark/results/<experiment>/environment/`, plus a generated
  `PROVENANCE.md` skeleton. `cluster-run-traffic`/`aws-run-traffic` call it
  automatically when `EXPERIMENT` is set and no `PROVENANCE.md` exists yet.
  Still hand-finished: the context notes (co-location caveats, Redis state,
  image build source). GPU driver/CUDA info joins in Milestone 2.

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
- [x] **Introduce ECR (with VPC endpoints) to control image-download costs.**
  Motivated by the 2026-11-20 g5.xlarge smoke test: `vllm/vllm-openai` is
  ~25GB uncompressed (~10–12GB compressed over the wire from Docker Hub
  through the NAT), plus 3.5–16GB model weights per model. Every GPU node
  pull pays NAT data processing + egress from Docker Hub; a same-region ECR
  pull with VPC gateway endpoints is free of NAT processing charges.
  Done 2026-11-21: long-lived module `infra/aws-ecr` (own state, survives
  `aws-destroy`) with repos `mastori`, `mastori/vllm`, `mastori/weights`;
  `infra/aws` reads the repo via data source and creates `ecr.api`/`ecr.dkr`
  interface endpoints (azs[0] only) + the free S3 gateway endpoint; `aws`/
  `aws/traffic`/`aws-inference` overlays rewritten to ECR refs. Validated
  live: base workloads pulled from ECR with **zero** NAT-gateway bytes
  during the pull window (pulls bypass NAT via the endpoints). Heavy images
  mirrored once from inside the cluster (`overlays/aws/mirror-jobs.yaml`,
  `make aws-mirror-images`): `vllm-openai:v0.34.0` 11.3GB, Qwen3-4B weights
  6.3GB — one-time NAT in ≈ $0.83 + endpoint processing ≈ $0.19; registry
  storage ≈ $1.80/mo. Digest pinning (above) stays a separate item.
  Open: switch `docker-publish.yml` CI to ECR (needs AWS creds in Actions;
  GHCR remains the fallback), and re-check the NAT CloudWatch metrics
  anomaly (datapoints stayed empty even during the mirror window — routing
  leaves only the NAT as internet egress, so topology is the proof).
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
  enters a comparison. Partial 2026-11-20 (groundwork): GPU node group
  Terraform-side (`infra/aws/eks.tf`, `AL2023_x86_64_NVIDIA` AMI, scale-to-
  zero, `mastori/pool=gpu` label + `mastori/gpu` taint); NVIDIA device
  plugin + dcgm-exporter via Helm (`make aws-nvidia`, tolerations for the
  mastori/gpu taint, GFD labels on); Prometheus scrapes dcgm on port 9400
  (RBAC glue in `overlays/aws-nvidia`). Still open: verify driver/CUDA on a
  live node, pin versions, DCGM pre-run diagnostics.
- [ ] **Real vLLM server Deployment:** model weights, `nvidia.com/gpu`
  requests, HF token secret, model cache on PVC or object storage (not
  re-downloaded per pod), GPU startup/readiness probes. `cmd/vllmworker`
  stays a CPU-pool bridge (or sidecar) in front of it. Partial 2026-11-20:
  `overlays/aws-inference` deploys adapter + vllmworker with
  `nvidia.com/gpu: 1` on the GPU pool (GHCR images); vllmworker stays
  Pending until the GPU pool is scaled (`make aws-gpu-scale`). The adapter
  backend URL still points at the kind-local Ollama endpoint.
  Partial 2026-11-21: real vLLM server wired in `overlays/aws-inference`
  (`vllm-server.yaml`: ECR `mastori/vllm:v0.34.0`, Qwen3-4B weights exported
  from the ECR weights image by an initContainer into an emptyDir, served
  OpenAI-compatible on `vllm:8000`; bridge `vllmworker` calls it directly,
  GPU-unclaimed). Not yet verified end to end: first bring-up was killed by
  the root-volume bug (see GPU node learnings) and the stack was torn down
  before the fix could prove out. Next run order: `aws-apply`, `aws-nvidia`,
  `aws-gpu-scale GPU_NODES=1`, `aws-deploy-inference`, then verify
  `vllm` /health + one `/v1/chat/completions` before any traffic.
  **GPU node learnings (2026-11-21 run 1) — first-thing check-list:**
  - g5.* is NOT offered in eu-north-1a (11-20 smoke test ran elsewhere);
    default is g6.xlarge (L4, 24GB) — and g6.xlarge is 4 vCPU / 16GB, same
    shape as g5.xlarge (verified via describe-instance-types; don't assume
    8/32). Node allocatable: ~3920m CPU, ~14.4Gi — size pods for that.
  - LT root volume: AL2023 EKS AMI roots on /dev/xvda; a /dev/sda1 mapping
    attaches an unused second disk instead of growing root → 20GB ephemeral
    and kubelet evictions. Fixed in `infra/aws/eks.tf` (LT v2, /dev/xvda
    120GB) but UNVERIFIED — confirm `kubectl get node <gpu>` capacity ≈ 120GB
    before re-running anything heavy.
  - NFD worker (nvdp chart) must tolerate the mastori/gpu taint or the GPU
    labels never land and the device-plugin DaemonSet never schedules
    (chicken-and-egg). Fixed via `nfd.worker.tolerations` in `make aws-nvidia`.
  - dcgm-exporter needs `serviceMonitor.enabled=false` (no Prometheus-Operator
    CRDs in this stack). Fixed in `make aws-nvidia`.
  - The vllmworker bridge must NOT request nvidia.com/gpu (JSON6902 unclaim
    in overlays/aws-inference) — the GPU belongs to the vllm server pod.
  - Pods cannot reach IMDS (hop limit 1): ECR auth inside pods comes from the
    `ecr-push-creds` secret (12h token; `make aws-mirror-images` refreshes).
  - Rolling updates of GPU-pod deployments deadlock (new pod waits for the
    GPU the old pod holds): recycle with `scale --replicas=0` → delete pods
    → `scale --replicas=1` instead of waiting on a hung rollout.
- [ ] **GPU node group** scales to zero; Budgets alert on; spot allowed only
  with the interrupted-run-is-invalid rule. Node group done 2026-11-20
  (`desired=min=0` default, ON_DEMAND; spot via `gpu_capacity_type`).
  Budgets alert + `make destroy` teardown doc still open (Milestone 1 item).
- [ ] Run the knee sweep end to end; verify GPU metrics are scraped and the
  run manifest records GPU model, driver, CUDA, instance type.
- [ ] Day-1 gates before first GPU run: request GPU quota, verify g5.xlarge
  availability in eu-north-1, dated pricing/calculator check.
  Update 2026-11-21: gate resolved by substitution — g5.* unavailable in
  1a, GPU default is g6.xlarge (L4); dated calculator check still open
  (~$0.80/h on-demand L4, re-verify before relying on it). Knee-cap
  experiment plumbing is ready: one GPU node
  (`make aws-gpu-scale GPU_NODES=1`), vllm + qwen3-4b weights already in
  ECR (no HF/Docker Hub downloads on nodes), `overlays/aws/traffic`
  rewritten to ECR. Run recipe: scale GPU to 1, `make aws-deploy-inference`,
  scale mockworker to 0 (`kubectl -n mastori scale deploy/mockworker
  --replicas=0` — its empty WORKER_MODELS registry entry matches EVERY
  model and would steal GPU traffic), then
  `make aws-run-traffic TRAFFIC_MODEL=qwen3-4b TRAFFIC_WORKLOAD=short
  TRAFFIC_STEPS="5:60s,10:60s,20:60s,40:60s,80:60s" TRAFFIC_WARMUP=1m`
  (TRAFFIC_STEPS is supported by cmd/traffic applyEnv). Scale GPU back to
  0 right after. Open: INFERENCE_MAX_TOKENS cap so worker outputs match
  the mock token budgets used by the knee analysis.
- [ ] **Get the AWS account ID out of git.** `273354666694` is hardcoded in
  the overlays (ECR refs in `kustomization.yaml` files, `mirror-jobs.yaml`,
  `vllm-server.yaml`, traffic IRSA role/bucket) and in `infra/aws-ecr`. The
  2026-11-22 hardcode of the traffic placeholders was reverted; the rest
  predates it. Durable fix: generate ECR registry / bucket / role ARN from
  `terraform output` at deploy time (small make target), then scrub history
  before the repo goes public.
- [ ] **Clean up `make aws-run-traffic`.** The target grew ad-hoc
  (TRAFFIC_* env passthrough, model/workload/steps wiring, the mockworker
  scale-down dance); restructure it before the knee sweep so the run recipe
  above is one command instead of a checklist.
- [ ] **Queue before vLLM + 429 handling at high request rates.** Question
  to answer during the knee sweep: does the queue-before-inference path
  hold at 1k req/s, and does the adapter/worker return 429s the traffic
  runner handles (retry/backoff) instead of failing runs? 2026-11-22 GPU
  samples showed full-GPU bursts then idle — measure whether request
  queuing smooths that or dumps bursts straight onto the GPU.
- [ ] **GPU/vLLM knee sweep 2026-11-22 (exploratory) — done, findings in
  `benchmark/results/20261122-eks-gpu-vllm-knee/runs/MANIFEST.md`.**
  Real vLLM + Qwen3-4B on g6.xlarge L4: flat floor through ~1-2K offered
  (p50 ~630ms, TTFT ~36ms), knee between 10K and 20K (completion 100%→~50%),
  no strong forward/reverse hysteresis; L4 ceiling ~19K prefill / ~12.2K
  decode tok/s (client-side); failure taxonomy mapped (429 admission cap,
  503 Redis pool starvation, dial-refused bursts, deep-queue OOMs).
  Open items from it:
  - [ ] Deploy REDIS_POOL_SIZE (code committed `1d056d6`, needs push → CI →
    GHCR → api rollout restart) and rerun the reversed ladder error-free
    (20K/30K points: no 429, no pool-starvation 503, no OOM).
  - [ ] Add a Prometheus scrape for `vllm:8000/metrics` (engine counters:
    `vllm:prompt_tokens_total` / `generation_tokens_total`) — the one
    telemetry gap; today only client-side + DCGM + api metrics were scraped.
  - [ ] Fix `make aws-gpu-scale GPU_NODES=0` (it sets maxSize=0, invalid;
    must clamp maxSize=1 while min/desired go to 0).
  - [ ] Formal experiment dir: `PROVENANCE.md` hand-finished, results
    submodule committed/pushed, story written; then teardown.
  - [ ] Decide the admission-cap story for the formal run: 429-at-1000
    (knee under admission control) vs open admission at 5000 (pure
    saturation latency profile) — today's data supports either; pick one
    methodology and record it.
- [ ] **Architecture code review.** Full pass over the system boundaries
  (traffic → API → Redis → scheduler → worker/adapter → vLLM, plus the
  dataset cache and provider layout) before Milestone 3 porting: interfaces
  worth extracting vs. the "three concrete implementations first" rule,
  error/recovery paths, coupling that the kustomize overlays hide.
- [ ] **Golang code review.** Idiomatic pass over `cmd/` + `internal/`
  (traffic runner, scheduler, worker, datasets, fakegpu): context/timeout
  handling, goroutine lifetimes, error wrapping, test coverage gaps
  flagged in the local-quality items above.

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
