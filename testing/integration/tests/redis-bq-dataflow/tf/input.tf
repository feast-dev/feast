variable "revision" {
  description = "Feast revision to pull helm charts from"
}

variable "docker_tag" {
  description = "Docker image to deploy"
  default = ""
}
