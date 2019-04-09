

output "endpoint" {
  value = "${google_container_cluster.feast_k8s.endpoint}"
}

output "cluster_name" {
  value = "${var.cluster_name}"
}

output "client_certificate" {
  value = "${google_container_cluster.feast_k8s.master_auth.0.client_certificate}"
}

output "client_key" {
  value = "${google_container_cluster.feast_k8s.master_auth.0.client_key}"
}

output "cluster_ca_certificate" {
  value = "${google_container_cluster.feast_k8s.master_auth.0.cluster_ca_certificate}"
}

output "username" {
  value = "${google_container_cluster.feast_k8s.master_auth.0.username}"
}

output "password" {
  value = "${google_container_cluster.feast_k8s.master_auth.0.password}"
}

output "feast_storage_bucket" {
  value = "${google_storage_bucket.feast_storage_bucket.name}"
}