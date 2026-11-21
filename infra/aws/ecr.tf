# The image repository is owned by the long-lived infra/aws-ecr module
# (separate state): it must survive destroy of this stack. Read it here.
data "aws_ecr_repository" "images" {
  name = var.ecr_repository_name
}