# The following outputs allow authentication and connectivity to the GKE Cluster.
output "client_certificate" {
  value = "${google_container_cluster.primary-test-cluster.master_auth.0.client_certificate}"
}

output "client_key" {
  value = "${google_container_cluster.primary-test-cluster.master_auth.0.client_key}"
}

output "cluster_ca_certificate" {
  value = "${google_container_cluster.primary-test-cluster.master_auth.0.cluster_ca_certificate}"
}