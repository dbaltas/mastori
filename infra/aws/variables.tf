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

variable "cluster_version" {
  description = "EKS Kubernetes version."
  type        = string
  default     = "1.34"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC."
  type        = string
  default     = "10.0.0.0/16"
}

variable "azs" {
  description = "Availability zones. EKS requires subnets in 2 AZs; workloads run in one."
  type        = list(string)
  default     = ["eu-north-1a", "eu-north-1b"]
}

variable "pod_cidr" {
  description = "Secondary CIDR for pod networking on the VPC."
  type        = string
  default     = "100.64.0.0/16"
}

variable "admin_cidrs" {
  description = "CIDRs allowed to reach the EKS API endpoint over the public endpoint. No public endpoint access at all when empty."
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
  description = "Instance types for the GPU node group. More than one when gpu_capacity_type is SPOT (AWS recommends several types for spot)."
  type        = list(string)
  default     = ["g5.xlarge"]
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
