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
    public_access_cidrs     = length(var.admin_cidrs) > 0 ? var.admin_cidrs : []
  }

  enabled_cluster_log_types = ["api", "audit"]

  tags = {
    Name = var.cluster_name
  }

  depends_on = [
    aws_iam_role_policy_attachment.cluster_policy,
    aws_iam_role_policy_attachment.cluster_vpc_controller,
  ]
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
      volume_size           = 120
      volume_type           = "gp3"
      delete_on_termination = true
    }
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

  launch_template {
    id = aws_launch_template.node.id
    # Pinned: EKS resolves "$Latest" to a concrete version at create time and
    # always reports it back, so "$Latest" would diff forever. Bump this when
    # the launch template changes.
    version = "1"
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

  subnet_ids = [aws_subnet.private[0].id]

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

  launch_template {
    id = aws_launch_template.gpu.id
    # Same pinning rationale as the CPU group above. The template itself
    # defines the 120GB gp3 root volume on /dev/xvda (the AMI's actual root
    # device); the older "/dev/sda1 attaches an unused second disk" problem
    # was fixed in the template definition (2026-11-21), so v1 is correct
    # since the destroy/recreate of 2026-11-22.
    version = "1"
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
