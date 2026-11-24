variable "region" {
  description = "AWS region for the benchmark stack."
  type        = string
  default     = "eu-north-1"
}

variable "cluster_name" {
  description = "EKS cluster name."
  type        = string
  default     = "mastori"
}

variable "ecr_repository_name" {
  description = "Name of the ECR repository managed by infra/aws-ecr (long-lived, separate state)."
  type        = string
  default     = "mastori"
}

variable "cluster_version" {
  description = "EKS Kubernetes version."
  type        = string
  default     = "1.37"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC."
  type        = string
  default     = "10.0.0.0/16"
}

variable "azs" {
  description = "Availability zones. EKS requires subnets in 2 AZs; the CPU pool and the workload AZ is azs[0] = eu-north-1b (1a is regularly dry for GPU types). The GPU pool spans both (g6.xlarge capacity is per-AZ; see eks.tf)."
  type        = list(string)
  default     = ["eu-north-1b", "eu-north-1a"]
}

variable "admin_cidrs" {
  description = "CIDRs allowed to reach the EKS API endpoint over the public endpoint. No public endpoint access at all when empty."
  type        = list(string)
  default     = []
}

variable "cluster_authentication_mode" {
  description = "EKS access management mode. API_AND_CONFIG_MAP is the safe migration step for the live cluster (aws-auth keeps working while access entries are applied); set \"API\" for fresh clusters. See eks.tf and README."
  type        = string
  default     = "API_AND_CONFIG_MAP"

  validation {
    condition     = contains(["API", "API_AND_CONFIG_MAP"], var.cluster_authentication_mode)
    error_message = "Must be \"API\" or \"API_AND_CONFIG_MAP\"."
  }
}

variable "admin_principals" {
  description = "IAM principal ARNs (user/role) granted AmazonEKSClusterAdminPolicy via EKS access entries. Must be non-empty before moving to API-only mode."
  type        = list(string)
  default     = []
}

variable "node_instance_type" {
  description = "On-demand instance type for the CPU node group (Graviton)."
  type        = string
  default     = "m6g.xlarge"
}

variable "node_count" {
  description = "Desired node count for the CPU node group."
  type        = number
  default     = 1
}

variable "node_min_count" {
  description = "Minimum node count for the CPU node group."
  type        = number
  default     = 1
}

variable "node_max_count" {
  description = "Maximum node count for the CPU node group."
  type        = number
  default     = 2
}

variable "gpu_instance_types" {
  description = "Instance types for the GPU node group. More than one when gpu_capacity_type is SPOT (AWS recommends several types for spot). g5.* is NOT offered in eu-north-1a (verified 2026-11-21 via instance-type offerings); the default is g6.xlarge (L4, 24GB, bf16-capable), offered in 1a alongside g4dn.* and g6e.*."
  type        = list(string)
  default     = ["g6.xlarge"]
}

variable "gpu_node_max_count" {
  description = "Maximum node count for the GPU node group. The pool scales to zero (desired=min=0) per the cost guardrails (docs/cloud-k8s-prep.md, decision 8); raise min/desired manually for a run."
  type        = number
  default     = 1
}

variable "gpu_capacity_type" {
  description = "Capacity type for the GPU node group: ON_DEMAND or SPOT. Spot is allowed only under the interrupted-run-is-invalid rule (decision 9)."
  type        = string
  default     = "ON_DEMAND"
}
