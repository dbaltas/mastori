# EKS control plane + one on-demand Graviton CPU node group (Milestone 1)
# + one scale-to-zero GPU node group (Milestone 2 prep, AL2023 NVIDIA AMI).
# No load balancer: nothing is internet-facing. Cluster access via the
# public endpoint restricted to admin_cidrs (or private endpoint + tunnel
# when admin_cidrs is empty).

data "aws_caller_identity" "current" {}

resource "aws_cloudwatch_log_group" "cluster" {
  name              = "/aws/eks/${var.cluster_name}/cluster"
  retention_in_days = 1

  tags = {
    Name = "${var.cluster_name}-logs"
  }
}

resource "aws_eks_cluster" "main" {
  name     = var.cluster_name
  role_arn = aws_iam_role.cluster.arn
  version  = var.cluster_version

  vpc_config {
    subnet_ids              = aws_subnet.private[*].id
    endpoint_public_access  = length(var.admin_cidrs) > 0
    endpoint_private_access = true
    # null (omitted) when there is no public endpoint: an explicit empty list
    # makes AWS report its 0.0.0.0/0 default back and diff forever.
    public_access_cidrs = length(var.admin_cidrs) > 0 ? var.admin_cidrs : null
  }

  # EKS access management (review T-M1): retire the legacy aws-auth ConfigMap.
  # bootstrap_cluster_creator_admin_permissions = false: admin rights must
  # come from the declared access entries below, not from "whoever ran apply".
  # API_AND_CONFIG_MAP is the safe migration step for the live cluster (both
  # auth paths honored, so access entries can be applied before the flip);
  # for a fresh cluster set cluster_authentication_mode = "API" in tfvars —
  # the access entries are created right after the cluster, so the window
  # without an admin is seconds on an ephemeral stack.
  access_config {
    authentication_mode                         = var.cluster_authentication_mode
    bootstrap_cluster_creator_admin_permissions = false
  }

  # Stay on STANDARD support: leaving standard support costs ~6x the control
  # plane rate (review T-M7) and this cluster is destroyed between series.
  upgrade_policy {
    support_type = "STANDARD"
  }

  enabled_cluster_log_types = ["api", "audit"]

  tags = {
    Name = var.cluster_name
  }

  depends_on = [
    aws_iam_role_policy_attachment.cluster_policy,
    aws_iam_role_policy_attachment.cluster_vpc_controller,
    # The log group must exist before the cluster reports log delivery
    # (review T-M6): the two are created concurrently otherwise and the
    # cluster apply can race the group into "already exists" vs "not found".
    aws_cloudwatch_log_group.cluster,
  ]
}

# Declared admin access, replacing the implicit creator-admin of the
# bootstrap flag (review T-M1). One entry per IAM principal ARN
# (user/role), each with cluster-admin permissions. Empty by default:
# set admin_principals in terraform.tfvars before applying API mode.
resource "aws_eks_access_entry" "admin" {
  for_each = toset(var.admin_principals)

  cluster_name      = aws_eks_cluster.main.name
  principal_arn     = each.value
  kubernetes_groups = []
}

resource "aws_eks_access_policy_association" "admin" {
  for_each = aws_eks_access_entry.admin

  cluster_name  = each.value.cluster_name
  policy_arn    = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"
  principal_arn = each.value.principal_arn

  access_scope {
    type = "cluster"
  }
}

resource "aws_iam_openid_connect_provider" "main" {
  url             = aws_eks_cluster.main.identity[0].oidc[0].issuer
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = ["9e99a48a9960b14926bb7f3b1e3537f6ed6c2c6f"]

  tags = {
    Name = "${var.cluster_name}-oidc"
  }
}

# Issuer domain for IRSA trust policies: "oidc.eks.<region>.amazonaws.com"
locals {
  irsa_oidc_issuer = replace(aws_iam_openid_connect_provider.main.url, "https://", "")
}

resource "aws_launch_template" "node" {
  name = "${var.cluster_name}-node"

  # The default 20GB root disk is too small for pod ephemeral storage on the
  # CPU pool: bake-weights (mirror-jobs.yaml) writes a 12Gi emptyDir there
  # and the mirror Jobs only carry the CPU pool's taint-free schedule
  # (review T-M3). 50GB gp3 leaves headroom over the ~8GB AMI + image layers.
  block_device_mappings {
    device_name = "/dev/xvda"

    ebs {
      volume_size           = 50
      volume_type           = "gp3"
      delete_on_termination = true
    }
  }

  # IMDS hop limit 1: pods cannot reach IMDS (the EKS AMI default is 2 in
  # some versions — pin it instead of relying on AMI defaults), and tokens
  # are required (review T-M3).
  metadata_options {
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }

  tag_specifications {
    resource_type = "instance"

    tags = {
      Name = "${var.cluster_name}-node"
    }
  }
}

resource "aws_launch_template" "gpu" {
  name = "${var.cluster_name}-gpu"

  # The AL2023 NVIDIA AMI's default root volume (~20GB) is far too small for
  # inference serving: measured on the 2026-11-20 g5.xlarge smoke instance,
  # the vllm/vllm-openai image alone is ~25GB uncompressed and docker storage
  # reached 29GB with Qwen3-1.7B weights. 120GB gp3 leaves headroom for an
  # 8B/14B-AWQ model (~16GB weights) without re-provisioning.
  # Device name: the AL2023 EKS AMI roots on /dev/xvda. A /dev/sda1 mapping
  # does not resize root — it attaches a second, unused 120GB disk (observed
  # live 2026-11-21: node showed 20GB ephemeral with the 120GB idle).
  block_device_mappings {
    device_name = "/dev/xvda"

    ebs {
      volume_size = 120
      volume_type = "gp3"
      # Defaults (3000 IOPS / 125 MB/s) throttle the pull of the ~11GB vLLM
      # image + ~8GB weights and that time is billed GPU time (review T-M3).
      throughput            = 500
      iops                  = 6000
      encrypted             = true
      delete_on_termination = true
    }
  }

  # Same IMDS hardening as the CPU template: hop limit 1 (pods cannot reach
  # IMDS) and required tokens.
  metadata_options {
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }

  tag_specifications {
    resource_type = "instance"

    tags = {
      Name = "${var.cluster_name}-gpu"
    }
  }
}

resource "aws_eks_node_group" "cpu" {
  cluster_name    = aws_eks_cluster.main.name
  node_group_name = "${var.cluster_name}-cpu"
  node_role_arn   = aws_iam_role.node.arn

  # Every workload node-selects mastori/pool: cpu (deploy/kubernetes/base/*);
  # without this label nothing schedules on EKS. kind labels its nodes itself;
  # the managed node group must carry the label in Terraform.
  labels = {
    "mastori/pool" = "cpu"
  }

  subnet_ids = [aws_subnet.private[0].id]

  # On-demand only: this pool is the benchmark control plane (API, Redis,
  # scheduler, observability) and holds in-memory state. Spot is disallowed
  # here by design (see docs/cloud-k8s-prep.md, decision 9).
  capacity_type  = "ON_DEMAND"
  instance_types = [var.node_instance_type]

  # First create is routinely >30 min in eu-north-1; the provider's default
  # 30m create timeout otherwise kills the apply while AWS keeps creating.
  timeouts {
    create = "60m"
  }

  # Graviton instances need the ARM64 AMI; without this EKS resolves
  # amiType to AL2023_x86_64_STANDARD and rejects m6g.* (observed live).
  ami_type = "AL2023_ARM_64_STANDARD"

  scaling_config {
    desired_size = var.node_count
    min_size     = var.node_min_count
    max_size     = var.node_max_count
  }

  # desired/min are scaled outside Terraform today (console/CLI during runs);
  # without this, any apply — even an unrelated tfvars tweak — drains the
  # nodes mid-run (review T-H1). max_size stays governed by Terraform.
  lifecycle {
    ignore_changes = [scaling_config[0].desired_size, scaling_config[0].min_size]
  }

  launch_template {
    id = aws_launch_template.node.id
    # Always roll out the newest template version: pinning a literal "1"
    # silently ignored every later LT edit (review T-M2). Referencing the
    # attribute (instead of the "$Latest" alias) resolves to a concrete
    # version, so it neither diffs forever nor pins stale templates.
    version = aws_launch_template.node.latest_version
  }

  tags = {
    Name = "${var.cluster_name}-cpu"
  }

  depends_on = [
    aws_iam_role_policy_attachment.node_worker,
    aws_iam_role_policy_attachment.node_cni,
    aws_iam_role_policy_attachment.node_ecr,
  ]
}

# Milestone 2 GPU pool (docs/cloud-k8s-prep.md). x86 (the AL2023 NVIDIA AMI
# has no ARM variant for g5.*), EKS-optimized AL2023 NVIDIA AMI ships the
# NVIDIA driver, CUDA user-mode driver, and container toolkit; the NVIDIA
# device plugin (Helm, `make aws-nvidia`) advertises nvidia.com/gpu.
resource "aws_eks_node_group" "gpu" {
  cluster_name    = aws_eks_cluster.main.name
  node_group_name = "${var.cluster_name}-gpu"
  node_role_arn   = aws_iam_role.node.arn

  # Workloads select mastori/pool: gpu (overlays/aws-inference/*).
  labels = {
    "mastori/pool" = "gpu"
  }

  # Only inference workloads tolerate this taint (decision: mastori/gpu=true).
  # The device plugin/dcgm-exporter DaemonSets tolerate it via their Helm
  # values (make aws-nvidia).
  taint {
    key    = "mastori/gpu"
    value  = "true"
    effect = "NO_SCHEDULE"
  }

  # GPU pool spans both private AZs: g6.xlarge capacity is per-AZ and 1a
  # regularly reports InsufficientInstanceCapacity, so the ASG must be able
  # to launch in 1b (azs[0] = eu-north-1b after the 2026-11-24 reorder — the
  # CPU pool runs there, so GPU-to-CPU traffic is same-AZ unless a GPU node
  # lands in 1a).
  subnet_ids = aws_subnet.private[*].id

  # Scale to zero (decision 8): desired=min=0 so an idle GPU pool bills
  # nothing; scale up for a run (make aws-gpu-scale) and back down after.
  capacity_type  = var.gpu_capacity_type
  instance_types = var.gpu_instance_types

  ami_type = "AL2023_x86_64_NVIDIA"

  timeouts {
    create = "60m"
  }

  scaling_config {
    desired_size = 0
    min_size     = 0
    max_size     = var.gpu_node_max_count
  }

  # Scale to zero is applied from the CLI/console (make aws-gpu-scale);
  # Terraform declaring desired=min=0 again would drain a GPU node mid-run
  # on any apply (review T-H1). max_size stays governed by Terraform.
  lifecycle {
    ignore_changes = [scaling_config[0].desired_size, scaling_config[0].min_size]
  }

  launch_template {
    id = aws_launch_template.gpu.id
    # Same rationale as the CPU group: follow the latest template version
    # (the template defines the 120GB gp3 root volume on /dev/xvda, the
    # actual root device — the older "/dev/sda1 attaches an unused second
    # disk" problem was fixed in the template definition, 2026-11-21).
    # The LT edits of 2026-11-24 (gp3 throughput/IOPS, encrypted root,
    # IMDS hop limit 1) roll out with the next apply via a node refresh.
    version = aws_launch_template.gpu.latest_version
  }

  tags = {
    Name = "${var.cluster_name}-gpu"
  }

  depends_on = [
    aws_iam_role_policy_attachment.node_worker,
    aws_iam_role_policy_attachment.node_cni,
    aws_iam_role_policy_attachment.node_ecr,
  ]
}
