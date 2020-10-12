variable "name_prefix" {
    default = "feast"
}

variable "region" {
}

variable "tags" {
  description = "Tags"
  type        = map(string)

  default = {}
}