locals {
  feast_postgres_secret_name = "${var.name_prefix}-postgres-secret"
  feast_helm_values = {
    redis = {
      enabled = false
    }

    grafana = {
      enabled = false
    }

    postgresql = {
      existingSecret = local.feast_postgres_secret_name
    }

    feast-core = {
      postgresql = {
        existingSecret = local.feast_postgres_secret_name
      }
    }

    feast-online-serving = {
      enabled = true
      "application-override.yaml" = {
        feast = {
          core-host      = "${var.name_prefix}-feast-core"
          core-grpc-port = 6565
          active_store   = "online_store"
          stores = [
            {
              name = "online_store"
              type = "REDIS"
              config = {
                host = azurerm_redis_cache.main.hostname
                port = azurerm_redis_cache.main.ssl_port
                ssl  = true
                subscriptions = [
                  {
                    name    = "*"
                    project = "*"
                    version = "*"
                  }
                ]
              }
            }
          ]
          job_store = {
            redis_host = azurerm_redis_cache.main.hostname
            redis_port = azurerm_redis_cache.main.ssl_port
          }
        }
      }
    }

    feast-jupyter = {
      enabled = true
      envOverrides = {
        feast_redis_host             = azurerm_redis_cache.main.hostname,
        feast_redis_port             = azurerm_redis_cache.main.ssl_port,
        feast_spark_launcher         = "standalone"
        feast_spark_staging_location = "https://${azurerm_storage_account.main.name}.blob.core.windows.net/${azurerm_storage_container.staging.name}/artifacts/"
        feast_historical_feature_output_location : "https://${azurerm_storage_account.main.name}.blob.core.windows.net/${azurerm_storage_container.staging.name}/out/"
        feast_historical_feature_output_format : "parquet"
        demo_kafka_brokers : "${azurerm_kubernetes_cluster.main.network_profile[0].dns_service_ip}:9094"
        demo_data_location : "https://${azurerm_storage_account.main.name}.blob.core.windows.net/${azurerm_storage_container.staging.name}/test-data/"
      }
    }
  }
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

resource "helm_release" "feast" {
  depends_on = [kubernetes_secret.feast-postgres-secret]

  name  = var.name_prefix
  namespace = var.aks_namespace
  chart = "../../charts/feast"

  values = [
    yamlencode(local.feast_helm_values)
  ]
}
