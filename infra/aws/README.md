# AWS / EKS (Milestone 1)

Single concrete Terraform module for the CPU/mock-worker EKS cluster in
`eu-north-1`. No provider abstraction (see `docs/cloud-k8s-prep.md`, decision 4).

This is the **ephemeral** layer: destroy between experiment series. The image
registry lives in the separate long-lived module `infra/aws-ecr` (own state)
and is read here via a data source (`ecr.tf`); `terraform destroy` in this
directory never touches it.

## What it creates

- VPC with one private subnet per AZ in `eu-north-1b`/`eu-north-1a`. The CPU
  pool and the workload AZ is azs[0] = `eu-north-1b` (1a is regularly dry for
  GPU types, observed 2026-11-21/23); the **GPU pool spans both AZs** —
  g6.xlarge capacity is per-AZ, so the ASG can also launch in 1a. A GPU node
  in 1a pulls ECR/weights cross-AZ and its measured-path traffic to the CPU
  pool also crosses AZs (priced); GPU-to-CPU traffic is same-AZ while GPU
  nodes land in 1b. Endpoint pairs in 1a were considered and dropped for
  cost. Record the GPU node's AZ in the experiment provenance
  (`make cluster-env-capture` prints the pool topology).
- One NAT Gateway + EIP for outbound (image pulls, AWS APIs). Image pulls from
  ECR **bypass the NAT gateway** via VPC endpoints (`ecr.api` + `ecr.dkr`
  interface endpoints in azs[0] only, plus the free S3 gateway endpoint —
  image layers are stored in S3). Without the endpoints a same-region ECR
  pull would still bill NAT data processing, defeating the registry's purpose.
- EKS control plane with API/audit logging. No load balancer, nothing
  internet-facing.
- One **on-demand** Graviton node group (`m6g.xlarge`), private subnet, no
  public IPs, autoscaling 1–2 nodes.
- One **GPU** node group (`g6.xlarge` by default — **g5.\* is not offered in
  eu-north-1a**, verified via instance-type offerings 2026-11-21; g6.xlarge
  is 4 vCPU / 16GB, the same shape as g5.xlarge, do not assume 8/32), private
  subnet, labeled `mastori/pool=gpu` and tainted `mastori/gpu=true:NoSchedule`,
  using the EKS-optimized AL2023 NVIDIA AMI (NVIDIA driver + CUDA user-mode
  driver + container toolkit preinstalled; `nvidia.com/gpu` is advertised by
  the NVIDIA device plugin, installed via Helm with `make aws-nvidia`). The
  GPU launch template maps a 120GB gp3 root (500 MB/s, 6000 IOPS, encrypted)
  to **/dev/xvda** — the AL2023 EKS AMI roots there; a /dev/sda1 mapping
  attaches an unused second disk instead of growing root (observed live
  2026-11-21). The node groups follow the launch template's latest version,
  so LT edits roll out on the next apply. The pool **scales to zero**
  (`desired=min=0` in Terraform, with `ignore_changes` on both: scaling is
  done out-of-band via `make aws-gpu-scale` and an apply must never drain a
  node mid-run). Nothing bills while no run is active.
- IAM roles for cluster and nodes, plus the OIDC provider for IRSA
  (use `irsa_oidc_issuer` output in trust policies; annotate SAs with
  `eks.amazonaws.com/role-arn`).
- **S3 dataset cache** (`s3.tf`): a private bucket keyed by the datasets.yaml
  manifest SHA-256 (`datasets/<manifestSHA>/cache.tar.gz` + `.sha256`
  sidecar), an S3 VPC gateway endpoint on the private route tables
  (in-region S3 traffic bypasses the NAT gateway and its data-processing
  charges), and the `mastori-datasets-cache` IRSA role for the traffic
  ServiceAccount (S3 read + write on this bucket only: restore on hit, seed
  on miss). `force_destroy` is on — the cache is re-seedable, not source
  data; objects expire after 90 days (a changed datasets.yaml re-seeds under
  a new SHA prefix, so stale prefixes are dead weight).

### Wiring the S3 dataset cache

After `terraform apply`, print the values and paste them into the two
overlay files:

```
make aws-datasets-cache-config
# -> paste the role ARN into deploy/kubernetes/overlays/aws/traffic/service-account.yaml
#    (eks.amazonaws.com/role-arn)
# -> paste DATASETS_S3_BUCKET and AWS_REGION into
#    deploy/kubernetes/overlays/aws/traffic/s3.yaml
```

Then `make aws-run-traffic` prepares the cache in the datasets initContainer
(`/datasets prepare`): HEAD the pack for the current manifest SHA; on a hit,
download, verify the `.sha256` sidecar, and unpack into the emptyDir (~
seconds in-region); on a miss, fetch from the source service, pack, and
upload (first run pays the Hugging Face fetch once). The bucket name and
region are also consumed as `DATASETS_S3_BUCKET` / `AWS_REGION` by
`mastori-datasets seed|restore|prepare` locally.

## Access (private, per decision 2)

No public exposure of any workload. The **supported access path is the public
EKS endpoint restricted to your IP**: set `admin_cidrs = ["<your-ip>/32"]` in
terraform.tfvars (get it from `curl https://checkip.amazonaws.com`). There is
no bastion, SSM or VPN fallback, so with `admin_cidrs = []` (private-only
endpoint) the cluster has no access path — only use that if you already have
a tunnel into the VPC. Cluster access itself is granted via EKS access
entries (`admin_principals` in tfvars → `AmazonEKSClusterAdminPolicy`), not
the legacy aws-auth ConfigMap; see the migration note under "Apply / destroy".
Then:

```
aws eks update-kubeconfig --name mastori --region eu-north-1
kubectl -n mastori port-forward svc/api 8080:8080
kubectl -n mastori port-forward svc/grafana 7030:3000
```

Gotchas seen live: `admin_cidrs` must hold the *current* public IP
(`curl https://checkip.amazonaws.com`) — a stale IP looks like timeouts; and
on DNS64/IPv6-capable local networks kubectl may dial the DNS64-synthesized
`64:ff9b::` address that `publicAccessCidrs` cannot allow-list (force IPv4
via hosts entry or another network when `kubectl` stalls but `curl -4` to
the endpoint works).

## Apply / destroy

```
cd infra/aws
cp terraform.tfvars.example terraform.tfvars   # set admin_cidrs + admin_principals
terraform init
terraform plan
terraform apply
```

### State (local; S3 backend is prepared)

State is local today (`.gitignore`d, with backups and plan files that hold
the cluster CA, account ARNs and your admin /32 — do not commit or share
them). The S3 backend block is prepared but **commented out** in
`versions.tf`, because activating it is a coordinated change:

1. Create the state bucket once, outside this stack: versioned, encrypted,
   block-public-access, e.g.
   `aws s3api create-bucket --bucket mastori-tfstate-<acct> --region eu-north-1 --create-bucket-configuration LocationConstraint=eu-north-1`
   plus `put-public-access-block` and `put-bucket-versioning`.
2. Uncomment the `backend "s3"` block in `versions.tf` (and in
   `infra/aws-ecr/versions.tf` for the registry state — that one is the
   critical loss: it is the only record of the long-lived repositories).
3. Run `terraform init -migrate-state` in each directory (needs Terraform
   >= 1.10 for `use_lockfile` native locking; no DynamoDB table needed).
4. Re-run `terraform plan` and confirm zero unexpected changes.

Do all of this when no other session is running terraform commands against
the stack.

### Scale config ownership

`make aws-gpu-scale` sets desired/min/max of the GPU group out-of-band
(≤ 1 GPU node; `GPU_NODES=0` clamps maxSize to 1 — AWS rejects maxSize=0).
Both node groups `ignore_changes` on desired/min, so `terraform apply` never
undoes a manual scale mid-run; `max_size` stays governed by Terraform. The
CPU pool has the same protection.

## Deploy and run (from the repo root)

After `make aws-kubeconfig` (current kubectl context = the EKS cluster):

```
make aws-deploy          # apply deploy/kubernetes/overlays/aws (GHCR images)
make aws-status          # rollout status + pods; all healthy before traffic
make aws-run-traffic     # datasets initContainer + traffic Job vs API ClusterIP
make cluster-traffic-results   # recover JSONL + summary from the Job logs
```

### GPU bring-up (Milestone 2 prep)

The GPU pool exists from the first `terraform apply` but scales to zero, so
it costs nothing until a run. Sequence for a GPU benchmark run:

```
make aws-deploy            # base workloads (prometheus SA for the RBAC glue)
make aws-nvidia            # RBAC glue + NVIDIA device plugin + dcgm-exporter (Helm)
make aws-gpu-scale         # GPU_NODES=1 (default); bill starts when nodes join
make aws-deploy-inference  # vllm (GPU node) + vllmworker (CPU pool)
```

`aws-gpu-scale GPU_NODES=0` when the run is done — a forgotten GPU node
bills continuously (`maxSize` is clamped to 1 for the 0 case; AWS rejects
`maxSize=0`). The device plugin/dcgm-exporter DaemonSets tolerate the
`mastori/gpu` node taint and run node-selector `mastori/pool: gpu`; with the
pool at zero they show 0 desired pods (normal). GPU metrics (`nvidia-dcgm`
job, port 9400) land in the in-cluster Prometheus; the env-capture
(`make cluster-env-capture EXPERIMENT=<name>`) picks up driver/CUDA info via
node labels from GPU Feature Discovery.

The vLLM server Deployment is verified serving (2026-11-22/23 knee sweeps
against real vLLM + Qwen3-4B on the L4; findings in
`benchmark/results/20261122-eks-gpu-vllm-knee/`). Bring-up order and known
pitfalls stay in the GPU node learnings checklist in todos.md. The
`/vllmworker` bridge calls the vLLM Service directly on EKS (it runs on the
CPU pool — it only proxies HTTP, so it must not compete with vLLM for the
GPU node's 4 vCPU).

`aws-run-traffic` takes the same `TRAFFIC_*` overrides as
`cluster-run-traffic` (e.g. `make aws-run-traffic TRAFFIC_WORKLOAD=short
TRAFFIC_RATE=5 TRAFFIC_DURATION=2m`). The traffic Job pins no image tag: the
`aws` overlay rewrites `mastori/api:latest` to `ghcr.io/dbaltas/mastori:latest`
with `imagePullPolicy: Always`; digest pinning is a Milestone 1 item.

Observability and API access stay private (port-forward):

```
kubectl -n mastori port-forward svc/api 8080:8080
kubectl -n mastori port-forward svc/grafana 7030:3000
```

The node group is labeled `mastori/pool=cpu` in Terraform (`eks.tf`) — every
workload node-selects that label; without it nothing schedules.

Destroy before walking away — a forgotten node bills continuously:

```
terraform destroy
```

Export results first: JSONL results (`make cluster-traffic-results` writes
them to `benchmark/results/cluster/`), run manifest, Prometheus snapshot, and
traces go to the git results repo (decision 7); the export make targets land
with the overlay glue work.

## Cost notes

Roughly EKS control plane + one `m6g.xlarge` + NAT per month, on the order of
~$150–250/mo while running (verify with a dated calculator before relying on
it). Configure an AWS Budgets alert (guardrail, done outside Terraform for
now). The GPU node group scales to zero, so it adds nothing while idle; a
single `g6.xlarge` (L4) running is roughly ~$0.80/h on-demand (re-check with
a dated calculator at Milestone 2; 2026-11-21).
