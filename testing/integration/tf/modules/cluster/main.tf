data "google_client_config" "current" {}

resource "google_storage_bucket" "feast_storage_bucket" {
  name          = "${var.bucket_name}"
  location      = "${var.region}"
  force_destroy = true
}

resource "google_container_cluster" "feast_k8s" {
  name               = "${var.cluster_name}"
  region             = "${var.region}"
  initial_node_count = 1

  # node_version = "1.11.5-gke.5"
  # min_master_version = "1.11.5-gke.5"
  network = "${var.network}"

  subnetwork = "${var.subnetwork}"

  enable_legacy_abac = true

  node_config {
    machine_type = "n1-standard-4"
    disk_size_gb = "50"

    oauth_scopes = [
      "https://www.googleapis.com/auth/compute",
      "https://www.googleapis.com/auth/devstorage.read_write",
      "https://www.googleapis.com/auth/logging.write",
      "https://www.googleapis.com/auth/monitoring",
      "https://www.googleapis.com/auth/bigquery",
      "https://www.googleapis.com/auth/bigtable.admin",
      "https://www.googleapis.com/auth/bigtable.data",
    ]
  }

  provisioner "local-exec" {
    when    = "destroy"
    command = "sleep 90"
  }
}
