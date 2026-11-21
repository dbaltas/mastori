output "repository_url" {
  description = "URL of the mastori image repository (push from CI, pull from EKS)."
  value       = aws_ecr_repository.main["mastori"].repository_url
}

output "repository_urls" {
  description = "URLs of every managed repository."
  value       = { for k, r in aws_ecr_repository.main : k => r.repository_url }
}

output "repository_arn" {
  description = "ARN of the mastori repository (for IAM policies)."
  value       = aws_ecr_repository.main["mastori"].arn
}
