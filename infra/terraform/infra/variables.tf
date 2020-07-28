variable "location" {
  type        = string
  default     = "westeurope"
  description = "The location of the created resources."
}

variable "project_name" {
  type        = string
  description = "A string used to generate globally unique resource names. Must comprise only lowercase letters."
}
