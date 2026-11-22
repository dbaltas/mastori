# Remote dataset cache: an S3 bucket keyed by the datasets.yaml manifest
# SHA-256 and the IRSA role for the traffic Job's ServiceAccount. The S3
# VPC gateway endpoint lives in vpc.tf (shared with the ECR pull path).
#
# Data flow (cmd/datasets prepare): the traffic Job's datasets initContainer
# restores the cache pack from the bucket; on a miss it fetches from the
# source service and seeds the bucket. Object keys are SHA-derived
# (datasets/<manifestSHA>/cache.tar.gz + .sha256 sidecar), so a manifest
# change is a new prefix (automatic invalidation) and two concurrent seeders
# write identical bytes (last writer wins, harmless).

resource "aws_s3_bucket" "datasets_cache" {
  # Account ID suffix: bucket names are globally unique.
  bucket        = "${var.cluster_name}-datasets-cache-${data.aws_caller_identity.current.account_id}"
  force_destroy = true # re-seedable cache, not source data: teardown-friendly

  tags = {
    Name = "${var.cluster_name}-datasets-cache"
  }
}

resource "aws_s3_bucket_public_access_block" "datasets_cache" {
  bucket = aws_s3_bucket.datasets_cache.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "datasets_cache" {
  bucket = aws_s3_bucket.datasets_cache.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Expiry for stale manifest versions: a changed datasets.yaml re-seeds under a
# new SHA prefix, so old prefixes are dead weight after their window.
resource "aws_s3_bucket_lifecycle_configuration" "datasets_cache" {
  bucket = aws_s3_bucket.datasets_cache.id

  rule {
    id     = "expire-stale-manifest-versions"
    status = "Enabled"

    filter {
      prefix = "datasets/"
    }

    expiration {
      days = 90
    }
  }
}

# Trust policy: only the traffic ServiceAccount in the mastori namespace.
data "aws_iam_policy_document" "datasets_cache_assume" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.main.arn]
    }
    condition {
      test     = "StringEquals"
      variable = "${local.irsa_oidc_issuer}:sub"
      values   = ["system:serviceaccount:mastori:traffic"]
    }
  }
}

resource "aws_iam_role" "datasets_cache" {
  name               = "${var.cluster_name}-datasets-cache"
  assume_role_policy = data.aws_iam_policy_document.datasets_cache_assume.json

  tags = {
    Name = "${var.cluster_name}-datasets-cache"
  }
}

# Read + write: prepare restores on a hit and seeds on a miss. Scoped to this
# bucket only.
data "aws_iam_policy_document" "datasets_cache" {
  statement {
    actions   = ["s3:GetObject", "s3:HeadObject", "s3:PutObject"]
    resources = ["${aws_s3_bucket.datasets_cache.arn}/*"]
  }

  statement {
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.datasets_cache.arn]
  }
}

resource "aws_iam_role_policy" "datasets_cache" {
  name   = "${var.cluster_name}-datasets-cache"
  role   = aws_iam_role.datasets_cache.id
  policy = data.aws_iam_policy_document.datasets_cache.json
}
