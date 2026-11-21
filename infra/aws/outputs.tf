output "cluster_name" {
  description = "EKS cluster name."
  value       = aws_eks_cluster.main.name
}

output "region" {
  description = "AWS region."
  value       = var.region
}

output "cluster_endpoint" {
  description = "EKS API endpoint."
  value       = aws_eks_cluster.main.endpoint
}

output "irsa_oidc_issuer" {
  description = "OIDC issuer domain for IRSA trust policies."
  value       = local.irsa_oidc_issuer
}

output "node_role_arn" {
  description = "IAM role used by node group instances."
  value       = aws_iam_role.node.arn
}

output "ecr_repository_url" {
  description = "URL of the long-lived ECR image repository (infra/aws-ecr)."
  value       = data.aws_ecr_repository.images.repository_url
}

output "vpc_id" {
  description = "VPC holding the cluster."
  value       = aws_vpc.main.id
}
