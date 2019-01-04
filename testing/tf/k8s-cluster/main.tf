resource "google_container_cluster" "primary-test-cluster" {
  name       = "primary-test-cluster"
  zone       = "${var.default_region}-a"
  network    = "${var.default_network}"
  subnetwork = "${var.default_subnet}"

  initial_node_count = 1
  min_master_version = "1.11.5-gke.5"
  node_version       = "1.11.5-gke.5"

  additional_zones = [
    "${var.default_region}-b",
    "${var.default_region}-c",
  ]

  node_config {
    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform",
    ]

    machine_type = "n1-standard-4"
  }
}
