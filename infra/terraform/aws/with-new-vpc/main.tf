terraform {
  required_version = ">= 0.12.0"
}

provider "random" {
  version = "~> 2.1"
}

provider "local" {
  version = "~> 1.2"
}

provider "aws" {
  version = ">= 2.28.1"
  region  = var.region
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

data "aws_availability_zones" "available" {
}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "2.47.0"

  name                 = "${var.name_prefix}-vpc"
  cidr                 = "10.0.0.0/16"
  azs                  = data.aws_availability_zones.available.names
  private_subnets      = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
  public_subnets       = ["10.0.4.0/24", "10.0.5.0/24", "10.0.6.0/24"]
  enable_nat_gateway   = true
  single_nat_gateway   = true
  enable_dns_hostnames = true
  private_subnet_tags  = merge(var.private_subnet_tags, local.private_subnet_tags)
  public_subnet_tags   = merge(var.public_subnet_tags, local.public_subnet_tags)
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
    postgres_db_name = var.postgres_db_name
    postgres_db_user = var.postgres_db_user
    map_accounts = var.map_accounts
    map_roles = var.map_roles
    use_persistent_emr_cluster = var.use_persistent_emr_cluster
    tags = var.tags
}