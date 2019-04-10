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

variable "core_address" {
  description = "Core internal address"
}

variable "serving_address" {
  description = "Serving internal address"
}

variable "redis_address" {
  description = "Redis internal address"
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

variable "load_balancer_source_range" {
  description = "ingress filter for google internal load balancer"
}

variable "bq_dataset" {
  description = "BigQuery dataset for warehouse"
}

variable depends_on {
  default = []

  type = "list"
}

variable "bucket_name" {
  description = "Working storage for feast"
}