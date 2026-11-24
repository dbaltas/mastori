# Managed add-ons (review T-M5): vpc-cni, kube-proxy and coredns were
# previously self-managed defaults — installed by EKS at cluster create but
# invisible to Terraform and unversioned from the module's perspective.
# Managing them here makes their lifecycle explicit and lets
# bootstrap_cluster_creator_admin_permissions=false clusters keep working.
# The eks-pod-identity-agent is installed now so Pod Identity can replace
# IRSA plumbing later (review T-L2 alternative).
#
# Versions are not pinned: EKS resolves the latest compatible version for
# the cluster version. On a live cluster this apply adopts the running
# self-managed add-ons; resolve_conflicts=OVERWRITE lets the adoption
# succeed where an in-cluster edit differs. aws-node/coredns pods restart
# during adoption — do not run it mid-experiment.
resource "aws_eks_addon" "this" {
  for_each = toset([
    "vpc-cni",
    "kube-proxy",
    "coredns",
    "eks-pod-identity-agent",
  ])

  cluster_name                = aws_eks_cluster.main.name
  addon_name                  = each.key
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"

  tags = {
    Name = "${var.cluster_name}-${each.key}"
  }
}
