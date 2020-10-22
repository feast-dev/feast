terraform {
  required_version = ">= 0.12.0"
}

provider "aws" {
  version = ">= 2.28.1"
  region  = var.region
}

provider "random" {
  version = "~> 2.1"
}

provider "local" {
  version = "~> 1.2"
}

provider "null" {
  version = "~> 2.1"
}

provider "template" {
  version = "~> 2.1"
}

data "aws_eks_cluster" "cluster" {
  name = module.eks.cluster_id
}

data "aws_eks_cluster_auth" "cluster" {
  name = module.eks.cluster_id
}

provider "kubernetes" {
  host                   = data.aws_eks_cluster.cluster.endpoint
  cluster_ca_certificate = base64decode(data.aws_eks_cluster.cluster.certificate_authority.0.data)
  token                  = data.aws_eks_cluster_auth.cluster.token
  load_config_file       = false
  version                = "~> 1.11"
}

resource "aws_security_group" "all_worker_mgmt" {
  name_prefix = "${var.name_prefix}-worker"
  tags = var.tags
  vpc_id      = var.vpc_id
}

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "12.2.0"

  cluster_name    = var.cluster_name
  cluster_version = "1.17"
  subnets         = var.subnets

  tags = var.tags

  vpc_id = var.vpc_id

  worker_groups = [
    {
      name                 = "worker-group-1"
      instance_type        = "r3.large"
      asg_desired_capacity = 2
    },
    {
      name                 = "worker-group-2"
      instance_type        = "r3.large"
      asg_desired_capacity = 1
    },
  ]

  worker_additional_security_group_ids = [aws_security_group.all_worker_mgmt.id]
  map_roles                            = var.map_roles
  map_accounts                         = var.map_accounts

  workers_additional_policies = [aws_iam_policy.worker_policy.id]
}