# resource "kubernetes_service_account" "tiller" {
#   metadata {
#     name      = "tiller"
#     namespace = "kube-system"
#   }
# }

# resource "kubernetes_cluster_role_binding" "tiller" {
#   metadata {
#     name = "tiller"
#   }

#   subject {
#     kind = "User"
#     name = "system:serviceaccount:kube-system:tiller"
#   }

#   role_ref {
#     kind = "ClusterRole"
#     name = "cluster-admin"
#   }

#   depends_on = ["kubernetes_service_account.tiller"]
# }

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
    docker_tag         = "${var.docker_tag}"
    job_runner         = "${var.job_runner}"
    job_runner_options = "${var.job_runner_options}"
    errors_store_type  = "${var.errors_store_type}"
    errors_store_options = "${var.errors_store_options}"
    project_id         = "${var.project_name}"
    region             = "${var.region}"
  }
}

resource "local_file" "helm_values_output" {
  content  = "${template_file.helm_values.rendered}"
  filename = "values.yaml"

  depends_on = ["template_file.helm_values"]
}

resource "helm_release" "feast" {
  name       = "feast-it"
  repository = "https://raw.githubusercontent.com/zhilingc/feast/master/charts/dist/"
  chart      = "feast"
  version    = "0.1.0"

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
