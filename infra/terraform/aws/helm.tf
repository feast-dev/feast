provider "helm" {
  kubernetes {
    host                   = data.aws_eks_cluster.cluster.endpoint
    cluster_ca_certificate = base64decode(data.aws_eks_cluster.cluster.certificate_authority.0.data)
    token                  = data.aws_eks_cluster_auth.cluster.token
    load_config_file       = false
  }
}

# Construct feast configs that need to point to RDS and Redis.
#
# RDS password is stored in a configmap which is not awesome but that RDS instance is not routable
# from the outside anyways so that'll do.
locals {
    feast_core_config = {
        redis = {
            enabled = false
        }
        postgresql = {
            enabled = false
        }
        kafka = {
            enabled = false
        }

        "feast-core" = {
            "application-generated.yaml" = {
                enabled = false
            }

            "application-override.yaml" = {
                spring = {
                    datasource = {
                        url = "jdbc:postgresql://${module.rds_cluster.endpoint}:5432/${module.rds_cluster.database_name}"
                        username = "${module.rds_cluster.master_username}"
                        password = "${random_password.db_password.result}"
                    }
                }
                feast = {
                    stream = {
                        type = "kafka"
                        options = {
                            bootstrapServers = ${aws_msk_cluster.msk.bootstrap_brokers}
                            topic = "feast"
                        }
                    }
                }
                server = {
                    port = "8080"
                }
            }
        }

        "feast-online-serving" = {
            "application-override.yaml" = {
                enabled = true
                feast = {
                    stores = [
                        {
                            name = "online"
                            type = "REDIS"
                            config = {
                                host = module.redis.endpoint
                                port = 6379
                            }
                            subscriptions = [
                                {
                                    name=  "*"
                                    project= "*"
                                    version= "*"
                                }
                            ]
                        }
                    ]
                    job_store = {
                        redis_host = module.redis.endpoint
                        redis_port = 6379
                    }
                }
            }
        }
    }
}

resource "helm_release" "feast" {
  name       = "feast"
  chart      = "../../charts/feast"

  wait       = false

  values = [
    yamlencode(local.feast_core_config)
  ]
}