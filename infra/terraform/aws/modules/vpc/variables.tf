variable "name_prefix" {
    default = "feast"
}

variable "region" {
}

variable "private_subnet_tags" {
    default = {
          Tier = "private"
        }
}

variable "public_subnet_tags" {
    default = {
          Tier = "public"
        }
}
variable "tags" {
  description = "Tags"
  type        = map(string)

  default = {}
}