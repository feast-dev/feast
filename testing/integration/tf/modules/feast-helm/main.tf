
locals {
 warehouse_option = {
    "project" = "${var.project_name}"
    "dataset" = "${var.bq_dataset}"
  }
  serving_option = {
    "host" = "${var.redis_address}"
    "port" = "6379"
  }
  store_error_option = {
    "path" = "gs://${var.bucket_name}/error-log"
  }
}

resource "google_compute_address" "core_address" {
  project      = "${var.project_name}"
  name         = "feast-core-ip"
  subnetwork   = "${var.subnetwork}"
  address_type = "INTERNAL"
  address      = "${var.core_address}"
  region       = "${var.region}"
}

resource "google_compute_address" "serving_address" {
  project      = "${var.project_name}"
  name         = "feast-serving-ip"
  subnetwork   = "${var.subnetwork}"
  address_type = "INTERNAL"
  address      = "${var.serving_address}"
  region       = "${var.region}"
}

resource "template_file" "helm_values" {
  template = "${file("${path.module}/values.tmpl")}"

  vars = {
    docker_tag             = "${var.docker_tag}"
    job_runner             = "${var.job_runner}"
    job_runner_options     = "${var.job_runner_options}"
    store_warehouse_option = "'${jsonencode(local.warehouse_option)}'"
    store_error_option     = "'${jsonencode(local.store_error_option)}'"
    store_serving_option   = "'${jsonencode(local.serving_option)}'"
    project_id             = "${var.project_name}"
    region                 = "${var.region}"
    workspace              = "gs://${var.bucket_name}/workspace"
  }
}

resource "local_file" "helm_values_output" {
  content  = "${template_file.helm_values.rendered}"
  filename = "values.yaml"

  depends_on = ["template_file.helm_values"]
}

resource "helm_release" "feast" {
  name       = "feast-it"
  chart      = "/feast/charts/feast"

  set {
    name  = "core.service.extIPAdr"
    value = "${google_compute_address.core_address.address}"
  }

  set {
    name  = "redis.master.service.loadBalancerIP"
    value = "${var.redis_address}"
  }

  set {
    name  = "core.service.loadBalancerSourceRanges[0]"
    value = "${var.load_balancer_source_range}"
  }

  set {
    name  = "serving.service.extIPAdr"
    value = "${google_compute_address.serving_address.address}"
  }

  set {
    name  = "serving.service.loadBalancerSourceRanges[0]"
    value = "${var.load_balancer_source_range}"
  }

  set {
    name  = "statsd.host"
    value = "${var.statsd_host}"
  }

  values = [
    "${template_file.helm_values.rendered}",
  ]

  timeout = 600

  depends_on = [
    "local_file.helm_values_output",
  ]
}
