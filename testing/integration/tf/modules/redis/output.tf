output "instance_url" {
  value = "${google_compute_instance.redis_vm.network_interface.0.network_ip}"
}
