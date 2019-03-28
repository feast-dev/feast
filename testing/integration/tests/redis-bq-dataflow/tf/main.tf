locals {
  project_name = "kf-feast"
  region = "us-central1"
  subnetwork = "regions/${local.region}/subnetworks/default"
  network = "default"
  cluster_name = "it-feast"

  job_runner_options = {
    "project"              = "${local.project_name}"
    "region"               = "${local.region}"
    "tempLocation"         = "gs://${local.cluster_name}-storage/tempJob"
    "subnetwork"           = "${local.subnetwork}"
    "maxNumWorkers"        = "64"
    "autoscalingAlgorithm" = "THROUGHPUT_BASED"
  }

  errors_store_options = {
    "path" = "gs://${local.cluster_name}-storage/errors"
  }
}

module "cluster" {
  source       = "../../../tf/modules/cluster"
  cluster_name = "${local.cluster_name}"
  project_name = "${local.project_name}"
  region       = "${local.region}"
  bucket_name  = "${local.cluster_name}-storage"
  network      = "${local.network}"
  subnetwork   = "${local.subnetwork}"
}

resource "null_resource" "wait_for_regional_cluster" {
  provisioner "local-exec" {
    command = "${path.module}/scripts/wait-for-cluster.sh ${local.project_name} ${local.cluster_name}"
  }

  provisioner "local-exec" {
    when    = "destroy"
    command = "${path.module}/scripts/wait-for-cluster.sh ${local.project_name} ${local.cluster_name}"
  }

  depends_on = ["module.cluster"]
}

module "feast" {
  source                     = "../../../tf/modules/feast-helm"
  project_name               = "${local.project_name}"
  region                     = "${local.region}"
  subnetwork                 = "${local.subnetwork}"
  docker_tag                 = "${var.docker_tag}"
  core_address               = "10.128.0.99"
  serving_address            = "10.128.0.100"
  redis_address              = "10.128.0.101"
  load_balancer_source_range = "10.0.0.0/8"
  job_runner                 = "DataflowRunner"
  job_runner_options         = "'${jsonencode(local.job_runner_options)}'"
  bucket_name                = "${local.cluster_name}-storage"
  bq_dataset                 = "${google_bigquery_dataset.feast_bq_dataset.dataset_id}"

  depends_on = ["module.cluster.cluster_name", "null_resource.wait_for_regional_cluster"]
}

resource "google_bigquery_dataset" "feast_bq_dataset" {
  dataset_id                  = "feast_it"
  description                 = "Feast integration test dataset"
  default_table_expiration_ms = 36000000
  location                    = "US"
  delete_contents_on_destroy  = true
}
