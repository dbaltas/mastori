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
  description = "Tag mutability of the repositories. IMMUTABLE (review T-L8): a tag always resolves to the same image, so a tag reference in a manifest is an effective pin; republishing an arch-corrected image requires a NEW tag (e.g. v0.34.0-2), never a re-point. MUTABLE allows overwrites (the pre-2026-11-24 default)."
  type        = string
  default     = "IMMUTABLE"

  validation {
    condition     = contains(["IMMUTABLE", "MUTABLE"], var.image_tag_mutability)
    error_message = "Must be \"IMMUTABLE\" or \"MUTABLE\"."
  }
}
