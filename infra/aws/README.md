# AWS / EKS (Milestone 1)

Single concrete Terraform module for the CPU/mock-worker EKS cluster in
`eu-north-1`. No provider abstraction (see `docs/cloud-k8s-prep.md`, decision 4).

This is the **ephemeral** layer: destroy between experiment series. The image
registry lives in the separate long-lived module `infra/aws-ecr` (own state)
and is read here via a data source (`ecr.tf`); `terraform destroy` in this
directory never touches it.

## What it creates

- VPC with one private subnet per AZ in `eu-north-1a`/`eu-north-1b`; workloads
  run in `a`, subnet `b` exists to satisfy the EKS 2-AZ requirement.
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
  GPU launch template maps a 120GB gp3 root to **/dev/xvda** — the AL2023 EKS
  AMI roots there; a /dev/sda1 mapping attaches an unused second disk instead
  of growing root (observed live 2026-11-21; fixed in LT v2, pin
  `version = "2"`). The pool **scales to zero** (`desired=min=0`): nothing
  bills while no run is active.
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

No public exposure of any workload. With `admin_cidrs` empty the EKS endpoint
is private-only; reach it from inside the VPC (tunnel) or set `admin_cidrs`
to your IP to use the public endpoint. Then:

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
cp terraform.tfvars.example terraform.tfvars   # set admin_cidrs
terraform init
terraform plan
terraform apply
```

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
make aws-deploy-inference  # adapter + vllmworker; vllmworker waits for a GPU
```

`aws-gpu-scale GPU_NODES=0` when the run is done — a forgotten GPU node
bills continuously. The device plugin/dcgm-exporter DaemonSets tolerate the
`mastori/gpu` node taint and run node-selector `mastori/pool: gpu`; with the
pool at zero they show 0 desired pods (normal). GPU metrics (`nvidia-dcgm`
job, port 9400) land in the in-cluster Prometheus; the env-capture
(`make cluster-env-capture EXPERIMENT=<name>`) picks up driver/CUDA info via
node labels from GPU Feature Discovery.

Still gated before the first real GPU run (docs/cloud-k8s-prep.md):
GPU quota approval, dated pricing check (g6.xlarge ~$0.80/h, re-verify), and
an end-to-end verification of the vLLM server Deployment
(`overlays/aws-inference/vllm-server.yaml` — manifest written 2026-11-21,
not yet verified serving; see the GPU node learnings checklist in
todos.md for the bring-up order and known pitfalls). The `/vllmworker`
bridge calls the vLLM Service directly on EKS (the Ollama adapter from
components/inference is deployed but unused in the cloud path).

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
