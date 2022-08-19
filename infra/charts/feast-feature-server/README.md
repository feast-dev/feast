# Feast Python / Go Feature Server Helm Charts

Current chart version is `0.23.0`

## Installation
A base64 encoded version of the `feature_store.yaml` file is needed. Helm install example:
```
helm install feast-feature-server . --set feature_store_yaml_base64=$(base64 feature_store.yaml)
```

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| affinity | object | `{}` |  |
| feature_store_yaml_base64 | string | `""` | [required] a base64 encoded version of feature_store.yaml |
| fullnameOverride | string | `""` |  |
| image.pullPolicy | string | `"IfNotPresent"` |  |
| image.repository | string | `"feastdev/feature-server"` | Docker image for Feature Server repository |
| image.tag | string | `"0.23.0"` | The Docker image tag (can be overwritten if custom feature server deps are needed for on demand transforms) |
| imagePullSecrets | list | `[]` |  |
| livenessProbe.initialDelaySeconds | int | `30` |  |
| livenessProbe.periodSeconds | int | `30` |  |
| nameOverride | string | `""` |  |
| nodeSelector | object | `{}` |  |
| podAnnotations | object | `{}` |  |
| podSecurityContext | object | `{}` |  |
| readinessProbe.initialDelaySeconds | int | `20` |  |
| readinessProbe.periodSeconds | int | `10` |  |
| replicaCount | int | `1` |  |
| resources | object | `{}` |  |
| securityContext | object | `{}` |  |
| service.port | int | `80` |  |
| service.type | string | `"ClusterIP"` |  |
| tolerations | list | `[]` |  |

## Development
To build a local
```
helm install feast-python-server . --set image.repository=REPO --set image.tag=TAG
```