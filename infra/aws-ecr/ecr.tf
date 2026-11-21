resource "aws_ecr_repository" "main" {
  for_each = toset(var.repositories)

  name                 = each.key
  image_tag_mutability = var.image_tag_mutability
  force_delete         = false

  image_scanning_configuration {
    scan_on_push = true
  }

  lifecycle {
    prevent_destroy = true
  }
}

# NOTE: an ECR pull-through cache rule for Docker Hub was considered to
# mirror vllm/vllm-openai without touching the VPC, but Docker Hub upstreams
# require a Secrets Manager secret with Docker Hub credentials (anonymous
# upstreams are rejected) and vLLM is not on ECR Public/Quay/GHCR. If Docker
# Hub creds ever get provisioned, add:
#   resource "aws_ecr_pull_through_cache_rule" "dockerhub" {
#     ecr_repository_prefix = "dockerhub"
#     upstream_registry_url = "registry-1.docker.io"
#   }
# Until then the vllm image is mirrored once via a cluster Job (skopeo copy
# Docker Hub -> ECR, one-time NAT egress) — later node pulls are NAT-free.

resource "aws_ecr_lifecycle_policy" "main" {
  for_each = toset(var.repositories)

  repository = aws_ecr_repository.main[each.key].name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Expire untagged images after 7 days"
        selection = {
          tagStatus   = "untagged"
          countType   = "sinceImagePushed"
          countUnit   = "days"
          countNumber = 7
        }
        action = {
          type = "expire"
        }
      },
      {
        rulePriority = 2
        description  = "Keep last 30 images"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = 30
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}
