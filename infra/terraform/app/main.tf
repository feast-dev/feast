data "azurerm_storage_account" "datalake" {
  name                = var.datalake_name
  resource_group_name = var.datalake_resource_group_name
}

data "azurerm_postgresql_server" "postgres" {
  name                = var.postgresql_name
  resource_group_name = var.postgresql_resource_group_name
}

locals {
  databricks_secret_scope        = "feast"
  databricks_secret_datalake_key = "azure_account_key"
  pypi_password_secret_key       = "pypi_password"
  pypi_username_secret_key       = "pypi_username"
  databricks_dbfs_jar_folder     = "dbfs:/feast/run${var.run_number}"
  databricks_spark_version       = "6.6.x-scala2.11"
  databricks_vm_type             = "Standard_D3_v2"
  databricks_instance_pool_name  = "Feast"
  databricks_pypi_init_script    = <<EOT
      #!/usr/bin/env bash

      mkdir /.config && mkdir /.config/pip
      echo -e "[global]\nindex-url = https://$PYPI_USER:$PYPI_PWD@$PYPI_REPO\nextra-index-url =  https://pypi.org/simple/\n" > /.config/pip/pip.conf
      export PIP_CONFIG_FILE=/.config/pip/pip.conf
      EOT
}

resource "azurerm_postgresql_database" "feast" {
  name                = "feast"
  resource_group_name = var.postgresql_resource_group_name
  server_name         = var.postgresql_name
  charset             = "UTF8"
  collation           = "English_United States.1252"
}

resource "databricks_token" "feast" {
  lifetime_seconds = 315569520 # ten years
  comment          = "Token used by CI/CD pipeline"
}

resource "databricks_instance_pool" "feast" {
  instance_pool_name                    = local.databricks_instance_pool_name
  min_idle_instances                    = 2
  max_capacity                          = 6
  node_type_id                          = local.databricks_vm_type
  idle_instance_autotermination_minutes = 60
}

resource "databricks_secret_scope" "feast" {
  name                     = local.databricks_secret_scope
  initial_manage_principal = "users"
}

resource "databricks_secret" "azure_account_key" {
  key          = local.databricks_secret_datalake_key
  string_value = data.azurerm_storage_account.datalake.primary_access_key
  scope        = databricks_secret_scope.feast.name
}

resource "null_resource" "dbfs-ingestion" {
  triggers = {
    dbfs_jar_folder = local.databricks_dbfs_jar_folder
  }
  provisioner "local-exec" {
    command = <<EOT
        pip install databricks-cli==0.11.0
        dbfs cp -r --overwrite "${var.spark_job_jars}" "${local.databricks_dbfs_jar_folder}"
EOT

    environment = {
      DATABRICKS_HOST  = var.databricks_workspace_url
      DATABRICKS_TOKEN = databricks_token.feast.token_value
    }

  }
}

resource "helm_release" "feast_services" {
  name  = "feast-services"
  chart = "../../../infra/charts/feast"

  wait    = true
  timeout = 600

  values = [
    <<EOT

kafka:
  external:
    enabled: true
    type: "LoadBalancer"
    annotations:
      service.beta.kubernetes.io/azure-load-balancer-internal: "true"
      service.beta.kubernetes.io/azure-load-balancer-internal-subnet: "internal-load-balancers"
    loadBalancerIP:
    - "${var.kafka_vnet_ip}"
  configurationOverrides:
    advertised.listeners: "EXTERNAL://$${LOAD_BALANCER_IP}:31090"
    listener.security.protocol.map: "PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT"
    offsets.topic.replication.factor: 1
    transaction.state.log.replication.factor: 1
    transaction.state.log.min.isr: 1
  persistence:
    enabled: false
  replicas: 1
  zookeeper:
    replicaCount: 1

postgresql:
  enabled: false

prometheus:
  alertmanager:
    persistentVolume:
      enabled: false
  server:
    persistentVolume:
      enabled: false

prometheus-statsd-exporter:
  enabled: false

redis:
  enabled: false

feast-core:
  enabled: false
feast-online-serving:
  enabled: false
feast-batch-serving:
  enabled: false
EOT
  ]
}

# Install Feast Core in separate release, to avoid
# CrashLoopBackOff cycles while waiting for Kafka to start
resource "helm_release" "feast_core" {
  name  = "feast-core"
  chart = "../../../infra/charts/feast"

  wait    = true
  timeout = 600

  values = [
    <<EOT
feast-core:
  image:
    repository: ${var.feast_core_image_repository}
    tag: ${var.feast_version}
  service:
    type: LoadBalancer
    annotations:
      service.beta.kubernetes.io/azure-load-balancer-internal: "true"
      service.beta.kubernetes.io/azure-load-balancer-internal-subnet: "internal-load-balancers"
    loadBalancerIP: ${var.feast_core_vnet_ip}

  application-secret.yaml:

    spring:
      datasource:
        url: "jdbc:postgresql://${data.azurerm_postgresql_server.postgres.fqdn}/${azurerm_postgresql_database.feast.name}"
        username: "${data.azurerm_postgresql_server.postgres.administrator_login}@${data.azurerm_postgresql_server.postgres.name}"
        password: "${var.postgresql_administrator_login_password}"

    feast:
      jobs:
        polling_interval_milliseconds: 5000
        # databricks job can take several minutes to start (on new clusters)
        job_update_timeout_seconds: 1200
        active_runner: databricks
        runners:
          - name: direct
            type: DirectRunner
            options: {}
          - name: databricks
            type: DatabricksRunner
            options:
              host: "${var.databricks_workspace_url}"
              token: "${databricks_token.feast.token_value}"
              checkpointLocation: dbfs:/checkpoints/feast
              jarFile: "${local.databricks_dbfs_jar_folder}/sparkjars/spark-ingestion-job.jar"
              timeoutSeconds: 1200
              newCluster:
                sparkVersion: "${local.databricks_spark_version}"
                instancePoolId: "${databricks_instance_pool.feast.id}"
                numWorkers: 1
                sparkConf: |
                  fs.azure.account.key.${var.datalake_name}.dfs.core.windows.net {{secrets/${local.databricks_secret_scope}/${local.databricks_secret_datalake_key}}}

      stream:
        type: kafka
        options:
          topic: feast-features
          bootstrapServers: "${var.kafka_vnet_ip}:31090"

feast-online-serving:
  enabled: false
feast-batch-serving:
  enabled: false
postgresql:
  enabled: false
kafka:
  enabled: false
redis:
  enabled: false
prometheus-statsd-exporter:
  persistentVolume:
    enabled: false

prometheus:
  enabled: false
grafana:
  enabled: false
EOT
  ]

  depends_on = [
    helm_release.feast_services
  ]
}

# Install Feast Serving in separate release, to avoid
# long CrashLoopBackOff cycles while waiting for Feast core
# to start (resulting in back-off periods up to 5 minutes)
resource "helm_release" "feast_serving" {
  name  = "feast-serving"
  chart = "../../../infra/charts/feast"

  wait    = true
  timeout = 120

  values = [
    <<EOT
feast-online-serving:
  image:
    repository: ${var.feast_serving_image_repository}
    tag: ${var.feast_version}

  service:
    type: LoadBalancer
    annotations:
      service.beta.kubernetes.io/azure-load-balancer-internal: "true"
      service.beta.kubernetes.io/azure-load-balancer-internal-subnet: "internal-load-balancers"
    loadBalancerIP: ${var.feast_online_serving_vnet_ip}

  application-override.yaml:
    feast:
      core-host: "${var.feast_core_vnet_ip}"
      stores:
      - name: online
        type: REDIS
        config:
          host: ${var.redis_hostname}
          port: ${var.redis_port}
        subscriptions:
        - name: "*"
          project: "*"

feast-batch-serving:
  enabled: true
  image:
    repository: ${var.feast_serving_image_repository}
    tag: ${var.feast_version}

  service:
    type: LoadBalancer
    annotations:
      service.beta.kubernetes.io/azure-load-balancer-internal: "true"
      service.beta.kubernetes.io/azure-load-balancer-internal-subnet: "internal-load-balancers"
    loadBalancerIP: ${var.feast_batch_serving_vnet_ip}

  application-override.yaml:
    feast:
      core-host: "${var.feast_core_vnet_ip}"
      active_store: delta
      stores:
        - name: delta
          type: DELTA
          config:
            path: "abfss://${var.datalake_filesystem}@${var.datalake_name}.dfs.core.windows.net/feast${var.run_number}"
          subscriptions:
            - name: "*"
              project: "*"
      job_store:
        redis_host: ${var.redis_hostname}
        redis_port: ${var.redis_port}

feast-core:
  enabled: false
postgresql:
  enabled: false
kafka:
  enabled: false
redis:
  enabled: false
prometheus-statsd-exporter:
  enabled: false
prometheus:
  enabled: false
grafana:
  enabled: false
EOT
  ]

  depends_on = [
    helm_release.feast_core
  ]
}

resource "databricks_secret" "pypi_username" {
  key          = local.pypi_username_secret_key
  string_value = var.pypi_user
  scope        = databricks_secret_scope.feast.name
}

resource "databricks_secret" "pypi_password" {
  key          = local.pypi_password_secret_key
  string_value = var.pypi_password
  scope        = databricks_secret_scope.feast.name
}

resource "databricks_dbfs_file" "init_pypi_script" {
  content              = filebase64("../../scripts/init_pypi.sh")
  path                 = "/init_scripts/init_pypi.sh"
  overwrite            = true
  mkdirs               = true
  validate_remote_file = true
}

resource "databricks_cluster" "feast-cluster" {
  cluster_name            = "feast-dev-test"
  spark_version           = "6.5.x-scala2.11"
  node_type_id            = "Standard_DS3_v2"
  autotermination_minutes = 30

  autoscale {
      min_workers = 0
      max_workers = 2
    }

  init_scripts {
    dbfs {
      destination = "dbfs:${databricks_dbfs_file.init_pypi_script.path}"
    }
  }
  spark_env_vars = {
    "PYPI_PWD"        = "{{secrets/${databricks_secret_scope.feast.name}/${databricks_secret.pypi_password.key}}"
    "PYPI_USER"       = "{{secrets/${databricks_secret_scope.feast.name}/${databricks_secret.pypi_username.key}}}"
    "PYSPARK_PYTHON"  = "/databricks/python3/bin/python3"
    "PIP_CONFIG_FILE" = "/.config/pip/pip.conf"
  }
}