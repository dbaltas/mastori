terraform {
  required_version = ">= 1.9"

  # Local state today; migrate to S3 before this state is shared or becomes
  # the only record of anything long-lived (review T-H2). The block is kept
  # commented because activating it is a coordinated change: any terraform
  # command fails until `terraform init -migrate-state` runs, and the state
  # bucket must exist first (create it outside this stack; see README).
  # backend "s3" {
  #   bucket       = "mastori-tfstate-<account-id>"
  #   key          = "aws/terraform.tfstate"
  #   region       = "eu-north-1"
  #   encrypt      = true
  #   use_lockfile = true # native S3 locking, Terraform >= 1.10
  # }

  required_providers {
    aws = {
      source = "hashicorp/aws"
      # ~> 6.0 (review T-L1): no upper bound before; the committed lock file
      # is on 6.73.0, so this constraint needs no re-init.
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project   = "mastori"
      Env       = "benchmark"
      ManagedBy = "terraform"
      Stack     = "ephemeral" # the aws-ecr stack is the long-lived one
    }
  }
}
