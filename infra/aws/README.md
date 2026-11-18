# AWS / EKS (Milestone 1)

Single concrete Terraform module for the CPU/mock-worker EKS cluster in
`eu-north-1`. No provider abstraction (see `docs/cloud-k8s-prep.md`, decision 4).

## What it creates

- VPC with one private subnet per AZ in `eu-north-1a`/`eu-north-1b`; workloads
  run in `a`, subnet `b` exists to satisfy the EKS 2-AZ requirement.
- One NAT Gateway + EIP for outbound (image pulls, AWS APIs). Keep images in
  ECR in-region to bound NAT cost.
- EKS control plane with API/audit logging. No load balancer, nothing
  internet-facing.
- One **on-demand** Graviton node group (`m6g.xlarge`), private subnet, no
  public IPs, autoscaling 1–2 nodes.
- IAM roles for cluster and nodes, plus the OIDC provider for IRSA
  (use `irsa_oidc_issuer` output in trust policies; annotate SAs with
  `eks.amazonaws.com/role-arn`).

## Access (private, per decision 2)

No public exposure of any workload. With `admin_cidrs` empty the EKS endpoint
is private-only; reach it from inside the VPC (tunnel) or set `admin_cidrs`
to your IP to use the public endpoint. Then:

```
aws eks update-kubeconfig --name mastori --region eu-north-1
kubectl -n mastori port-forward svc/api 8080:8080
kubectl -n mastori port-forward svc/grafana 7030:3000
```

## Apply / destroy

```
cd infra/aws
cp terraform.tfvars.example terraform.tfvars   # set admin_cidrs
terraform init
terraform plan
terraform apply
```

Destroy before walking away — a forgotten node bills continuously:

```
terraform destroy
```

Export results first: JSONL results, run manifest, Prometheus snapshot, and
traces go to the git results repo (decision 7); the export make targets land
with the overlay glue work.

## Cost notes

Roughly EKS control plane + one `m6g.xlarge` + NAT per month, on the order of
~$150–250/mo while running (verify with a dated calculator before relying on
it). Configure an AWS Budgets alert (guardrail, done outside Terraform for
now). GPU node groups with scale-to-zero come in Milestone 2.
