# Cloud preparation: from kind to managed Kubernetes

Status: **plan only — nothing here is implemented yet.** Revised 2026-11-18. Decisions are recorded below; open questions and claims to verify are called out explicitly.

## Goal

Run the same Kubernetes manifests on managed Kubernetes across providers, in this order: **AWS → Azure → Nebius → Verda → others (OVH, ...)**. Keep as much as possible inside the cluster so deployment stays identical per provider; the cloud only supplies control plane, nodes, and identity.

Provider candidates (maturity to be re-checked at integration time, not assumed):

| Provider | Managed K8s | Notes |
|---|---|---|
| AWS | EKS | First deployment — concrete, no abstraction |
| Azure | AKS | Second deployment — also concrete |
| Nebius | Managed Service for Kubernetes | GPU focus; abstraction starts here |
| OVHcloud | MKS | Cheap commodity option, later |
| Verda | Managed Kubernetes | Alpha/pilot — last; status re-checked when we get there |

## Decided (2026-11-18)

1. **Kustomize over Helm.** Fits the existing plain manifests; `configMapGenerator` replaces the imperative Grafana-dashboards step in `make cluster-deploy`.
2. **Gateway API / Envoy Gateway deferred.** No public exposure at all in the first milestones. Access is private (`kubectl port-forward` / tunnel), which needs no LB, no annotations, no TLS, and behaves identically on every provider. Gateway API returns when there is a real multi-service public surface (e.g. a shared results UI) — and then only with auth in front of API and Grafana.
3. **Traffic generator runs in-cluster** as a Job on the CPU pool, calling the API's `ClusterIP` directly. Provider LB/network latency never enters the measurement. Resource limits for the traffic Job: **1 CPU / 500 Mi memory**. The traffic generator shares resources with the System Under Test — not what we want in the long run, but at this moment it saves time, cost, and complexity.
4. **AWS concrete, Azure concrete, abstraction after provider #3.** No shared provider abstraction until three clouds are implemented; let the third provider inform it.
5. **Milestone 1 is CPU/mock-worker only.** Real GPU inference is a separate, gated milestone.
6. **Redis durability: restart voids the run.** No PVC for Redis; cloud Redis is ephemeral in-cluster. A mid-run Redis or scheduler restart ends the experiment with **status `invalid`**. This requires:
   - A **run status** field (`running` / `valid` / `invalid`) recorded with results, set to `invalid` on detected interruption (Redis restart, scheduler crash, worker loss without reclaim).
   - **Results schema versioning**, starting at **version `0.1`**. Every exported result carries `schema_version: 0.1`.
   - The HF-hosted benchmark results repo is updated in the same change to also carry version `0.1`.
7. **Results survive teardown in the git results repo.** Before `destroy`: export JSONL results, run manifest (provider, instance types, driver, image digests, node info), and a Prometheus snapshot; push to the results repo. A destroyed cluster must not erase an experiment.
8. **Cost guardrails:** GPU node group scales to zero, AWS Budgets alert, explicit `make destroy` teardown target, and the provision → run → export → destroy loop is a documented, repeatable make target sequence.
9. **Spot discipline:** CPU pool (API, Redis, scheduler, observability) runs **on-demand** — it is the benchmark control plane and holds in-memory state. Spot GPUs are allowed only with the rule that an interrupted run is discarded (status `invalid`).
10. **No NetworkPolicy by design (2026-11-24).** Redis and the API are reachable from every pod in the cluster. The cluster is private (no LB/ingress, nodes in private subnets, endpoint restricted to admin /32), so the blast radius is pod compromise — acceptable for an ephemeral benchmark cluster; revisit when the stack becomes long-lived or shared.
11. **PDB only on the api Deployment (2026-11-24).** The api has 2 replicas, so a voluntary disruption budget (`api-pdb.yaml`) keeps drain-capacity. Every other singleton (redis, scheduler, vllm, prometheus) deliberately has NO PDB: a PDB on a singleton blocks all node drains (nothing can ever be evicted) — with Recreate strategies the single replica is allowed to go down for its own rollouts.

## Claims to verify before relying on them

- "No EKS free tier for new accounts" vs "credit-based free tier" — check current AWS free tier terms; do not budget on a control-plane discount until confirmed.
- Single-GPU H100 on AWS (`p5.4xlarge`, added 2025?) — verify availability and quota in eu-north-1.
- GPU spot pricing (~A100 $0.54/GPU-hr, H100 range) — re-check with a dated calculator link before the GPU milestone.
- Every cost figure needs an "as of" date and calculator inputs (NAT, NLB, EBS, logs, ECR, public IPv4, data transfer).

## Milestone 0 — local prerequisites (kind, no cloud spend)

Gate: `make cluster-up && make cluster-deploy` still behaves identically on kind after every change.

1. **Kustomize refactor:** split `deploy/kubernetes/` into base + `local` overlay. NodePort + `extraPortMappings` stay in the `local` overlay only.
2. **Real pool placement:** add `nodeSelector`/tolerations/topology spread to every workload (API, Redis, scheduler, workers, observability). GPU nodes get the `mastori/gpu: true` taint; only inference workloads tolerate it. Verify placement on kind with the existing labeled nodes.
3. **Redis reliability fixes** (also open `todos.md` items; unit tests for each):
   - Chunk-stream retention / TTL so Redis stays bounded under a long run.
   - Dead-worker job reclaim (heartbeat expiry → requeue or mark job failed).
   - Consumer-group pending-entry recovery after ungraceful scheduler restart.
   - Document and test: Redis restart mid-run → run ends with status `invalid`.
4. **Results schema v0.1 + run status:**
   - Add `schema_version: 0.1` and `status: running|valid|invalid` to the result/run manifest format.
   - Update the HF results repo writer to carry version `0.1`.
   - Unit tests: happy path, interruption → `invalid`, version field present in exports.
5. **Run/teardown checklist:** define exactly what is exported before destroy (JSONL results, run manifest, Prometheus snapshot, traces) and the make targets that do it.
6. **Integration test tiers** (agreed scope; implementation lands with the code it tests):
   - **Fast CI:** render every kustomize overlay, validate schemas, reject mutable image tags.
   - **kind CI:** deploy the local overlay, run the existing API→Redis→scheduler→worker flow.
   - **Failure CI:** restart Redis, scheduler, and workers; assert defined recovery behavior and `invalid` statuses.

## Milestone 1 — AWS, CPU/mock-worker EKS, private (first cloud spend)

**Acceptance check:** EKS up, one on-demand CPU node, API + Redis + scheduler + mock workers healthy, one benchmark run executed by the in-cluster traffic Job, results + manifest + metrics snapshot pushed to the results repo, cluster destroyed, results intact.

Day 1, in parallel with infrastructure: request GPU quota (new accounts start at zero; approvals can take days).

Terraform (one module, no provider abstraction yet) for eu-north-1:

- VPC: 2 AZ subnets for the EKS requirement (private, with tags); workloads in one AZ.
- NAT Gateway + EIP for outbound only (image pulls, API calls). Keep traffic in-region (ECR) to bound NAT cost.
- EKS control plane; one **on-demand** managed node group (Graviton, e.g. m6g.xlarge, private subnet, no public IPs).
- Security groups (control plane ↔ nodes); IAM roles for cluster, nodes, and IRSA service accounts.
- **No load balancer.** Nothing is internet-facing. Access via `kubectl port-forward` / tunnel; document the access steps in the run checklist.

ECR + CI:

- CI builds multi-arch images (Graviton CPU pool vs future x86 GPU nodes) with `buildx`, pushes to ECR, and deployments pin images by **digest**. Provenance (digest, repo, CI run) lands in the run manifest.

Overlay glue (`aws` overlay only): IRSA annotations, node sizes, region. Nothing app-specific.

Datasets: fetch dataset prompts inside the cluster (fetcher Job on the CPU pool or preloaded), so no data lands on nodes and the in-cluster traffic Job has prompts to replay.

## Milestone 2 — GPU / vLLM on EKS (gated on quota + capacity confirmed)

Only starts when: GPU quota approved, instance availability verified in eu-north-1, pricing re-checked with dated sources.

1. **NVIDIA stack:** GPU Operator (or device plugin), pinned driver/CUDA versions, `dcgm-exporter` metrics, DCGM pre-run diagnostics before a GPU enters a comparison. Partial 2026-11-20 (groundwork, no cloud spend yet): GPU node group in Terraform (`infra/aws/eks.tf`, `AL2023_x86_64_NVIDIA` AMI — driver/CUDA/container toolkit ship in the AMI, scale-to-zero), NVIDIA device plugin + `dcgm-exporter` via Helm (`make aws-nvidia`), Prometheus scrape job for dcgm telemetry, `aws-inference` overlay (`overlays/aws-inference`) scheduling the adapter + vllmworker on the GPU pool. Not done: pinned driver/CUDA verification on a live node, DCGM pre-run diagnostics.
2. **Real vLLM server Deployment:** model weights, `nvidia.com/gpu` requests, HF token secret, model cache on a PVC or object storage (not re-downloaded per pod), GPU startup/readiness probes. `cmd/vllmworker` stays a CPU-pool bridge (or sidecar) in front of it.
3. **GPU node group** scales to zero; Budgets alert on; spot allowed with the interrupted-run-is-invalid rule. Node group done 2026-11-20 (`desired=min=0`, `make aws-gpu-scale` for runs; spot available via `gpu_capacity_type`); Budgets alert still open.
4. **Traffic generator unchanged:** same in-cluster Job calling the API ClusterIP; per-provider comparison stays network-neutral.
5. Run the knee sweep end to end; verify GPU metrics are scraped and the run manifest records GPU model, driver, CUDA, and instance type.

## Milestone 3 — Azure, concrete

Port the same base + a new `azure` overlay + Terraform for AKS (on-demand CPU node group, identity via workload service accounts, ACR). No shared abstraction yet. Lessons from the second concrete implementation feed the abstraction design, not the reverse.

## After provider #3

Extract the provider abstraction (Terraform module interfaces, overlay contract, image/registry conventions) from three concrete implementations. Then Nebius (GPU-heavy), OVH, and Verda — re-checking Verda's pilot status at that point.

## Cost notes (to be re-verified with dated calculator links)

- Private-only access removes the NLB line item entirely; remaining lean AWS run cost is roughly the control plane + one on-demand Graviton node + NAT, on the order of ~$150–250/mo while running (verify; re-check at Milestone 1 with real inputs).
- NAT egress is the biggest hidden cost: keep images in ECR, same region.
- A forgotten GPU node costs more over a weekend than the whole CPU budget — hence scale-to-zero + Budgets alert as hard guardrails.
