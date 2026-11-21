variable "region" {
  description = "AWS region for the long-lived registry."
  type        = string
  default     = "eu-north-1"
}

variable "repositories" {
  description = "ECR repositories to manage. 'mastori' holds the API/worker images; 'mastori/vllm' mirrors the vLLM server image; 'mastori/weights' holds per-model tag weights images (e.g. qwen3-4b)."
  type        = list(string)
  default = [
    "mastori",
    "mastori/vllm",
    "mastori/weights",
  ]
}

variable "image_tag_mutability" {
  description = "Tag mutability of the repository. IMMUTABLE forbids overwrites (digest pinning safe), MUTABLE allows latest-style tags."
  type        = string
  default     = "MUTABLE"
}
