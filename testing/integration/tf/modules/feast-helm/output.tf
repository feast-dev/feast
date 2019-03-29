output "core_url" {
  value = "${google_compute_address.core_address.address}"
}

output "serving_url" {
  value = "${google_compute_address.serving_address.address}"
}

output "redis_url" {
  value = "${var.redis_address}"
}