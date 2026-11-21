# AWS ECR (long-lived layer)

Separate Terraform root module (`infra/aws-ecr`) with its own local state, so
the container registry survives `terraform destroy` in `infra/aws`. The EKS
stack is torn down between experiment series; the registry is destroyed only
deliberately (`make aws-ecr-destroy`), so images and image-cache history
survive normal stack churn.

`prevent_destroy` on the repositories is a seatbelt: an accidental `destroy`
here fails instead of deleting images. It is not a substitute for the state
split — the point is that the EKS stack lives in `infra/aws` and never
touches this state.

## What it creates

- Repositories (variables `repositories`): `mastori` (API/worker images),
  `mastori/vllm` (vLLM server image, mirrored `vllm/vllm-openai:v0.34.0`),
  `mastori/weights` (per-model-tag weights images; `qwen3-4b` today). All
  scan-on-push, `force_delete = false`.
- A lifecycle policy on each repo: expire untagged images after 7 days
  (rule 1), keep the last 30 images (rule 2 — `tagStatus: any` MUST be the
  lowest-priority rule per ECR validation).
- **Weights image layout gotcha:** the weights image holds the model files
  at the image ROOT (`crane append` of the weights tar), not under
  `models/` — the vllm-server initContainer validates
  `/models/model.safetensors.index.json` and vLLM serves `--model /models`.

## IAM (operator identity, deliberately outside Terraform)

The ECR grants live in the `mastori-eks` inline policy on the `dimitris` IAM
user (account 273354666694), not in any TF state — destroying and recreating
stacks never touches it. It covers repo-scoped push/pull actions on
`repository/mastori*` + `ecr:GetAuthorizationToken` (must stay `*`), and
pull-through-cache rule actions (also `*`: they are not resource-scopable).
Two operational notes learned live:

- IAM changes take 1–2 minutes to propagate; transient `AccessDeniedException`
  right after `put-user-policy` is expected — retry the apply.
- ECR lifecycle policies validate strictly (`tagPattern` unsupported with
  count-based rules; `tagStatus: any` must sort last).

## Apply / destroy

```
cd infra/aws-ecr
terraform init
terraform apply        # or: make aws-ecr-apply
```

Destroy (empty the repositories first, e.g. via the ECR console):

```
make aws-ecr-destroy
```

## Use

- Push: from CI or locally after `aws ecr get-login-password`
  (`docker login <account>.dkr.ecr.eu-north-1.amazonaws.com`), tag as
  `<repository_url>:<tag>`.
- Pull: the `infra/aws` stack reads the `mastori` repository with a
  `aws_ecr_repository` data source (no cross-state reference), and EKS nodes
  pull from it directly — in-region, no extra auth for the cluster (ECR token
  handled by kubelet; the node role carries
  `AmazonEC2ContainerRegistryReadOnly`).
- **NAT cost**: a pull over the ECR *public* endpoint still traverses the NAT
  gateway ($0.045/GB), same as a GHCR pull. The saving only materializes
  because `infra/aws` creates VPC endpoints (`ecr.api`, `ecr.dkr`, and the
  free S3 gateway endpoint) so pulls go over the AWS backbone and skip NAT
  entirely. Bake the heavy weights (e.g. Qwen3-4B ~7.5 GB) into image layers
  so every re-pull across stack rebuilds stays in that free path. Validated
  live 2026-11-21: zero NAT-gateway bytes during a window with real ECR pulls.
- **Pods can't pull with node creds**: IMDS is unreachable from containers
  (hop limit 1), so any in-pod registry access (`crane`, `skopeo`) uses the
  `ecr-push-creds` k8s secret — a 12h `get-login-password` token refreshed by
  `make aws-mirror-images`. Pulls inside pods (weights export for vllm) use
  it too, read-only.
- **In-cluster mirroring**: heavy images (vllm, weights) are fetched once by
  Jobs on the CPU pool (`deploy/kubernetes/overlays/aws/mirror-jobs.yaml`,
  `make aws-mirror-images`), not from the workstation — uplink from local
  machines can't finish 10+ GB pushes. One-time NAT cost ≈ $0.8–1 per image
  set; every later node pull is NAT-free. Pull-through cache was considered
  and dropped: Docker Hub upstreams require a Secrets Manager secret and
  vLLM is not on ECR Public/Quay/GHCR (comment block in `ecr.tf`).
- Image tags: same scheme as GHCR (`sha-<short>` per commit, `latest` on
  main); weights use model-name tags (`qwen3-4b`). Digest pinning of
  deployed tags remains a Milestone 1 item.

## Cost

Billed per GB stored + data transfer. Current contents ~17.6 GB ≈ $1.80/mo
(vllm 11.3 + weights 6.3). With the 30-image lifecycle cap the bill stays
bounded — check the console after a series of pushes and rely on the AWS
Budgets alert as guardrail.
