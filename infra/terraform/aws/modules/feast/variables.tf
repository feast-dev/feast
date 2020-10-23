variable "region" {
}

variable "name_prefix" {
    description = "Prefix to be used on all the resource names"
    type = string
}

variable "vpc_id" {
}

variable "postgres_db_name" {
  default = "feast"
}

variable "postgres_db_user" {
  default = "feast"
}

variable "map_accounts" {
  description = "Additional AWS account numbers to add to the aws-auth configmap."
  type        = list(string)

  default = [
  ]
}

variable "map_roles" {
  description = "Additional IAM roles to add to the aws-auth configmap."
  type = list(object({
    rolearn  = string
    username = string
    groups   = list(string)
  }))

  default = [

  ]
}

variable "use_persistent_emr_cluster" {
  description = "Create a persistent EMR cluster."
  default     = true
}

variable "tags" {
  description = "Tags"
  type        = map(string)

  default = {}
}

variable "subnets" {
  description = "A list of subnets for the resources"
  type = list(string)
}

variable "azs" {
  description = "A list of availability zones for resources"
  type = list(string)
}

variable "cluster_name" {
  description = "Eks cluster name"
  type = string
}