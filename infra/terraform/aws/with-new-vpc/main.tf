terraform {
  required_version = ">= 0.12.0"
}

provider "random" {
  version = "~> 2.1"
}

provider "local" {
  version = "~> 1.2"
}

locals {
  cluster_name = "${var.name_prefix}-${random_string.suffix.result}"
}

resource "random_string" "suffix" {
  length  = 8
  special = false
}

locals {
  public_subnet_tags ={
    "kubernetes.io/cluster/${local.cluster_name}" = "shared"
    "kubernetes.io/role/elb"                      = "1"
  }
  private_subnet_tags = {
    "kubernetes.io/cluster/${local.cluster_name}" = "shared"
    "kubernetes.io/role/internal-elb"             = "1"
  }
}

module "vpc" {
  source = "../modules/vpc"
  region = var.region
  name_prefix = var.name_prefix
  private_subnet_tags = merge(var.private_subnet_tags, local.private_subnet_tags)
  public_subnet_tags = merge(var.public_subnet_tags, local.public_subnet_tags)
  tags = var.tags
}

module "feast" {
    source = "../modules/feast"
    name_prefix = var.name_prefix
    vpc_id = module.vpc.vpc_id
    azs = module.vpc.azs
    cluster_name = local.cluster_name
    subnets = module.vpc.private_subnets
    region = var.region
    subnet_filter_tag = var.subnet_filter_tag
    postgres_db_name = var.postgres_db_name
    postgres_db_user = var.postgres_db_user
    map_accounts = var.map_accounts
    map_roles = var.map_roles
    use_persistent_emr_cluster = var.use_persistent_emr_cluster
    tags = var.tags
}