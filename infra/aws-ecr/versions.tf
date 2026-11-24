terraform {
  required_version = ">= 1.9"

  # This state is the only record of the long-lived registry (review T-H2):
  # losing terraform.tfstate means re-importing 3 repos + lifecycle policies.
  # Migrate to S3 with versioning + native locking (see infra/aws-ecr/README).
  # Kept commented: activating it fails every terraform command until the
  # bucket exists and `terraform init -migrate-state` runs.
  # backend "s3" {
  #   bucket       = "mastori-tfstate-<account-id>"
  #   key          = "aws-ecr/terraform.tfstate"
  #   region       = "eu-north-1"
  #   encrypt      = true
  #   use_lockfile = true # native S3 locking, Terraform >= 1.10
  # }

  required_providers {
    aws = {
      source = "hashicorp/aws"
      # ~> 6.0 (review T-L1): the committed lock file is on 6.x, so this
      # constraint needs no re-init.
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
      Stack     = "long-lived"
    }
  }
}
