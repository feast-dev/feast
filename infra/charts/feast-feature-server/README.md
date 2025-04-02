# Feast Python / Go Feature Server Helm Charts

Current chart version is `0.47.0`

## Installation

Run the following commands to add the repository

```
helm repo add feast-charts https://feast-helm-charts.storage.googleapis.com
helm repo update
```

Install Feast Feature Server on Kubernetes

- Feast Deployment Mode: The Feast Feature Server supports multiple deployment modes using the `feast_mode` property. Supported modes are `online` (default), `offline`, `ui`, and `registry`.
Users can set the `feast_mode` based on their deployment choice. The `online` mode is the default and maintains backward compatibility with previous Feast Feature Server implementations.

- Feature Store File: A base64 encoded version of the `feature_store.yaml` file is needed.

Helm install examples:
```
helm install feast-feature-server feast-charts/feast-feature-server --set feature_store_yaml_base64=$(base64 > feature_store.yaml)
helm install feast-offline-server feast-charts/feast-feature-server --set feast_mode=offline  --set feature_store_yaml_base64=$(base64 > feature_store.yaml)
helm install feast-ui-server feast-charts/feast-feature-server --set feast_mode=ui  --set feature_store_yaml_base64=$(base64 > feature_store.yaml)
helm install feast-registry-server feast-charts/feast-feature-server --set feast_mode=registry  --set feature_store_yaml_base64=$(base64 > feature_store.yaml)

```

## Tutorial
See [here](https://github.com/feast-dev/feast/tree/master/examples/python-helm-demo) for a sample tutorial on testing this helm chart with a demo feature repository and a local Redis instance.

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| affinity | object | `{}` |  |
| extraEnvs | list | `[]` | Additional environment variables to be set in the container |
| feast_mode | string | `"online"` | Feast supported deployment modes - online (default), offline, ui and registry |
| feature_store_yaml_base64 | string | `""` | [required] a base64 encoded version of feature_store.yaml |
| fullnameOverride | string | `""` |  |
| image.pullPolicy | string | `"IfNotPresent"` |  |
| image.repository | string | `"quay.io/feastdev/feature-server"` | Docker image for Feature Server repository |
| image.tag | string | `"0.47.0"` | The Docker image tag (can be overwritten if custom feature server deps are needed for on demand transforms) |
| imagePullSecrets | list | `[]` |  |
| livenessProbe.initialDelaySeconds | int | `30` |  |
| livenessProbe.periodSeconds | int | `30` |  |
| logLevel | string | `"WARNING"` |  |
| metrics.enabled | bool | `false` |  |
| metrics.otelCollector.endpoint | string | `""` |  |
| metrics.otelCollector.port | int | `4317` |  |
| nameOverride | string | `""` |  |
| nodeSelector | object | `{}` |  |
| podAnnotations | object | `{}` |  |
| podSecurityContext | object | `{}` |  |
| readinessProbe.initialDelaySeconds | int | `20` |  |
| readinessProbe.periodSeconds | int | `10` |  |
| replicaCount | int | `1` |  |
| resources | object | `{}` |  |
| route.enabled | bool | `false` |  |
| securityContext | object | `{}` |  |
| service.port | int | `80` |  |
| service.type | string | `"ClusterIP"` |  |
| serviceAccount.name | string | `""` |  |
| tolerations | list | `[]` |  |
| volumeMounts | list | `[]` |  |
| volumes | list | `[]` |  |