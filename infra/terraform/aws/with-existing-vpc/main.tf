terraform {
  required_version = ">= 0.12.0"
}

provider "aws" {
  version = ">= 2.28.1"
  region  = var.region
}

provider "local" {
  version = "~> 1.2"
}

data "aws_vpc" "selected" {
  id = var.vpc_id
}

data "aws_subnet_ids" "subnets" {
  vpc_id = var.vpc_id
  tags = var.subnet_filter_tag
}

data "aws_subnet" "subnets" {
  for_each = data.aws_subnet_ids.subnets.ids
  id       = each.value
}

locals {
  azs = [for s in data.aws_subnet.subnets : s.availability_zone]
  cluster_name = "${var.name_prefix}-${random_string.suffix.result}"
}

resource "random_string" "suffix" {
  length  = 8
  special = false
}

module "feast" {
    source = "../modules/feast"
    name_prefix = var.name_prefix
    vpc_id = var.vpc_id
    azs = local.azs
    cluster_name = local.cluster_name
    subnets = tolist(data.aws_subnet_ids.subnets.ids)
    region = var.region
    postgres_db_name = var.postgres_db_name
    postgres_db_user = var.postgres_db_user
    map_accounts = var.map_accounts
    map_roles = var.map_roles
    use_persistent_emr_cluster = var.use_persistent_emr_cluster
    tags = var.tags
}