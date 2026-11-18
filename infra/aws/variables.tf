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
  default     = "1.33"
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
