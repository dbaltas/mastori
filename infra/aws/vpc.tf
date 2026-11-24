# VPC: 2 AZs of private subnets (EKS requirement), everything private.
# Workloads run in azs[0] (eu-north-1b — 1a is regularly dry for GPU types);
# the second subnet exists only to satisfy EKS.
# Single NAT Gateway + EIP for outbound (image pulls, AWS API calls).

resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "${var.cluster_name}-vpc"
  }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "${var.cluster_name}-igw"
  }
}

resource "aws_subnet" "private" {
  count = length(var.azs)

  vpc_id            = aws_vpc.main.id
  cidr_block        = cidrsubnet(var.vpc_cidr, 4, count.index)
  availability_zone = element(var.azs, count.index)

  # Nodes get no public IPs; outbound goes through NAT.
  map_public_ip_on_launch = false

  # EKS-managed LBs / ENIs only in azs[0] — workloads stay in one AZ.
  tags = {
    Name                                        = "${var.cluster_name}-private-${element(var.azs, count.index)}"
    "kubernetes.io/cluster/${var.cluster_name}" = "shared"
    "kubernetes.io/role/internal-elb"           = 1
  }
}

# Public subnet for the NAT gateway only: a NAT gateway requires its own
# subnet to route 0.0.0.0/0 to the internet gateway. Workloads still run in
# azs[0] private; nothing else is placed here.
resource "aws_subnet" "public" {
  count = 1

  vpc_id            = aws_vpc.main.id
  cidr_block        = cidrsubnet(var.vpc_cidr, 4, length(var.azs))
  availability_zone = element(var.azs, 0)

  map_public_ip_on_launch = true

  tags = {
    Name = "${var.cluster_name}-public-${element(var.azs, 0)}"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = {
    Name = "${var.cluster_name}-public"
  }
}

resource "aws_route_table_association" "public" {
  count = length(aws_subnet.public)

  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_eip" "nat" {
  domain = "vpc"

  tags = {
    Name = "${var.cluster_name}-nat-eip"
  }
}

resource "aws_nat_gateway" "main" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public[0].id

  tags = {
    Name = "${var.cluster_name}-nat"
  }

  depends_on = [aws_internet_gateway.main]
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.main.id
  }

  tags = {
    Name = "${var.cluster_name}-private"
  }
}

resource "aws_route_table_association" "private" {
  count = length(aws_subnet.private)

  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# VPC endpoints for ECR pulls: image layers are stored in S3, so the free S3
# gateway endpoint + the two ECR interface endpoints let nodes pull images
# (and weights baked into images) without traversing the NAT gateway. This is
# what makes the long-lived infra/aws-ecr registry actually save NAT cost —
# a public-endpoint pull would still bill NAT data processing either way.
# Only azs[0] (the workload AZ) gets the interface endpoints; azs[1] hosts no
# pods, so paying for a second pair would buy nothing.
resource "aws_security_group" "endpoints" {
  name_prefix = "${var.cluster_name}-vpce-"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "HTTPS to the endpoints from inside the VPC"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  tags = {
    Name = "${var.cluster_name}-vpce"
  }
}

locals {
  ecr_endpoint_services = {
    api = "com.amazonaws.${var.region}.ecr.api"
    dkr = "com.amazonaws.${var.region}.ecr.dkr"
  }
}

resource "aws_vpc_endpoint" "ecr" {
  for_each = local.ecr_endpoint_services

  vpc_id              = aws_vpc.main.id
  service_name        = each.value
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.private[0].id]
  security_group_ids  = [aws_security_group.endpoints.id]
  private_dns_enabled = true

  tags = {
    Name = "${var.cluster_name}-ecr-${each.key}"
  }
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.private.id]

  tags = {
    Name = "${var.cluster_name}-s3"
  }
}
