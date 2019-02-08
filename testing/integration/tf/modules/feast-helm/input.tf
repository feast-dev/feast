variable "project_name" {
  description = "Project name"
}

variable "subnetwork" {
  description = "Desired subnetwork"
}

variable "region" {
  description = "Subnet region"
}

variable "docker_tag" {
  default     = "0.1.0"
  description = "Feast build version"
}

variable "revision" {
  default     = "master"
  description = "Github revision to pull charts from"
}

variable "core_address" {
  description = "Core internal address"
}

variable "serving_address" {
  description = "Serving internal address"
}

variable "statsd_host" {
  description = "Statsd host to write metrics to"
  default     = ""
}

variable "job_runner" {
  description = "Desired job runner"
  default     = "Dataflow"
}

variable "job_runner_options" {
  description = "Job runner options as a json string"
  default = "'{}'"
}

variable "errors_store_type" {
  description = "Job errors store type. One of stdout, stderr, file.json"
  default     = "stdout"
}

variable "errors_store_options" {
  description = "Errors store options as a json string"
  default = "'{}'"
}

variable "load_balancer_source_range" {
  description = "ingress filter for google internal load balancer"
}

variable depends_on {
  default = []

  type = "list"
}
