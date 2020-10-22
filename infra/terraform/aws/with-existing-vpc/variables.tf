variable "region" {
}

variable "name_prefix" {
}

variable "vpc_id" {
}

variable "subnet_filter_tag" {
  type = map(string)
  default = {
        #   Tier = "private"
        private=""
        }
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