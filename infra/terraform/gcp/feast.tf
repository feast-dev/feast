locals {
  feast_postgres_secret_name = "${var.name_prefix}-postgres-secret"
}

resource "random_password" "feast-postgres-password" {
  length  = 16
  special = false
}

resource "kubernetes_secret" "feast-postgres-secret" {
  metadata {
    name = local.feast_postgres_secret_name
  }
  data = {
    postgresql-password = random_password.feast-postgres-password.result
  }
}

resource "google_compute_address" "kafka_broker" {
  project      = var.gcp_project_name
  region       = var.region
  subnetwork   = var.subnetwork
  name         = "${var.name_prefix}-kafka"
  address_type = "INTERNAL"
}

resource "helm_release" "feast" {
  depends_on = [kubernetes_secret.feast-postgres-secret, kubernetes_secret.feast_sa_secret]

  name  = var.name_prefix
  chart = "../../charts/feast"

  values = [
    templatefile("templates/feast.yaml", {
      postgres_secret_name            = local.feast_postgres_secret_name
      redis_host                      = google_redis_instance.online_store.host
      dataproc_cluster_name           = google_dataproc_cluster.feast_dataproc_cluster.name
      dataproc_project                = var.gcp_project_name
      dataproc_region                 = var.region
      dataproc_staging_bucket         = var.dataproc_staging_bucket
      gcp_service_account_secret_name = var.feast_sa_secret_name
      gcp_service_account_secret_key  = "credentials.json"
      feast_core_host                 = "${var.name_prefix}-feast-core"
      feast_core_port                 = 6565
      kafka_broker_ip                 = google_compute_address.kafka_broker.address
    })
  ]
}
