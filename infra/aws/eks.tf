# EKS control plane + one on-demand Graviton CPU node group.
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
