feast
===== 

Feature store for machine learning. Current chart version is `0.9.5`

## Installation

https://docs.feast.dev/v/master/getting-started/deploying-feast/kubernetes

## Chart Requirements

| Repository | Name | Version |
|------------|------|---------|
|  | feast-core | 0.9.5 |
|  | feast-jupyter | 0.9.5 |
|  | feast-serving | 0.9.5 |
|  | prometheus-statsd-exporter | 0.1.2 |
| https://charts.bitnami.com/bitnami/ | kafka | 11.8.8 |
| https://kubernetes-charts.storage.googleapis.com/ | grafana | 5.0.5 |
| https://kubernetes-charts.storage.googleapis.com/ | postgresql | 8.6.1 |
| https://kubernetes-charts.storage.googleapis.com/ | prometheus | 11.0.2 |
| https://kubernetes-charts.storage.googleapis.com/ | redis | 10.5.6 |

## Chart Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| feast-core.enabled | bool | `true` | Flag to install Feast Core |
| feast-core.postgresql.existingSecret | string | `"feast-postgresql"` | Kubernetes secrets that contains the postgresql password |
| feast-jupyter.enabled | bool | `true` | Flag to install Feast Jupyter Notebook with SDK |
| feast-online-serving.enabled | bool | `true` | Flag to install Feast Online Serving |
| grafana.enabled | bool | `true` | Flag to install Grafana |
| kafka.enabled | bool | `true` | Flag to install Kafka |
| postgresql.enabled | bool | `true` | Flag to install Postgresql |
| postgresql.existingSecret | string | `"feast-postgresql"` | Kubernetes secrets that contains the postgresql password |
| prometheus-statsd-exporter.enabled | bool | `true` | Flag to install StatsD to Prometheus Exporter |
| prometheus.enabled | bool | `true` | Flag to install Prometheus |
| redis.enabled | bool | `true` | Flag to install Redis |
| redis.usePassword | bool | `false` | Disable redis password |


### Documentation development

This `README.md` is generated using [helm-docs](https://github.com/norwoodj/helm-docs/).
Please run `helm-docs` to regenerate the `README.md` every time `README.md.gotmpl`
or `values.yaml` are updated.
