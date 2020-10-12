locals {
  azs = [for s in data.aws_subnet.subnets : s.availability_zone]
}

module "redis" {
  source                     = "git::https://github.com/cloudposse/terraform-aws-elasticache-redis.git?ref=tags/0.25.0"
  subnets                    = data.aws_subnet_ids.subnets.ids
  name                       = "${var.name_prefix}-online"
  vpc_id                     = data.aws_vpc.selected.id
  allowed_security_groups    = [aws_security_group.all_worker_mgmt.id]
  availability_zones         = local.azs
  tags = var.tags
}
