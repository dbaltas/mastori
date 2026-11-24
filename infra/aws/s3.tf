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

# BucketOwnerEnforced: ACLs disabled — S3 object ownership stays with the
# bucket owner for every write, including cross-account writes (review T-L4).
resource "aws_s3_bucket_ownership_controls" "datasets_cache" {
  bucket = aws_s3_bucket.datasets_cache.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# TLS-only (review T-L4): deny any S3 API call that does not arrive over
# HTTPS. Data goes to the S3 gateway endpoint over TLS, but the policy makes
# plaintext access structurally impossible, not just unlikely.
resource "aws_s3_bucket_policy" "datasets_cache" {
  bucket = aws_s3_bucket.datasets_cache.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "DenyInsecureTransport"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:*"
        Resource = [
          aws_s3_bucket.datasets_cache.arn,
          "${aws_s3_bucket.datasets_cache.arn}/*",
        ]
        Condition = {
          Bool = { "aws:SecureTransport" = "false" }
        }
      }
    ]
  })

  depends_on = [aws_s3_bucket_public_access_block.datasets_cache]
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

  # Abort multipart uploads left open by a killed seeder (review T-L4):
  # incomplete MPU parts keep billing storage until aborted explicitly.
  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 3
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
    condition {
      # Restrict the audience to STS (review T-L2): without this the token
      # is valid for any OIDC audience registered on the provider.
      test     = "StringEquals"
      variable = "${local.irsa_oidc_issuer}:aud"
      values   = ["sts.amazonaws.com"]
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
# bucket only. s3:HeadObject is covered by s3:GetObject (HEAD is the same
# object-read permission); s3:AbortMultipartUpload covers the seeder's
# aborted uploads (review T-L3).
data "aws_iam_policy_document" "datasets_cache" {
  statement {
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:AbortMultipartUpload",
    ]
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
