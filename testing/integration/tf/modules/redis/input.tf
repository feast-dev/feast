variable depends_on {
  default = []

  type = "list"
}

variable "project_name" {
  description = "project name"
}

variable "zone" {
  description = "zone to create the instance in"
}

variable "type" {
  default     = "n1-standard-1"
  description = "machine type. Allowed values are https://cloud.google.com/compute/docs/machine-types"
}

variable "internet_tag" {
  description = "default internet tag. E.g: allow-internet, default-to-internet"
  default     = "allow-internet"
}

variable "boot_disk_size" {
  description = "boot disk size"
  default     = "10"
}

variable "boot_disk_type" {
  description = "boot disk type. Allowed values are pd-standard,pd-ssd"
  default     = "pd-standard"
}

variable "boot_disk_image" {
  description = "boot disk image"
  default     = "ubuntu-os-cloud/ubuntu-1604-xenial-v20170610"
}

variable "subnet" {
  description = "subnet name."
  default     = "default"
}

variable "network" {
  description = "network name. E.g default"
  default     = "default"
}

variable "name" {
  description = "Instance name"
}
