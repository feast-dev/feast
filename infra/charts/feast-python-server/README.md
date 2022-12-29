# Feast Python Feature Server Helm Charts (deprecated)

> Note: this helm chart is deprecated in favor of [feast-feature-server](../feast-feature-server/README.md)

Current chart version is `0.25.1`

## Installation
Docker repository and tag are required. Helm install example:
```
helm install feast-python-server . --set image.repository=REPO --set image.tag=TAG
```

Deployment assumes that `feature_store.yaml` exists on docker image. Example docker image:
```
FROM python:3.8

RUN apt update && \
    apt install -y jq

RUN pip install pip --upgrade

RUN pip install feast

COPY feature_store.yaml /feature_store.yaml
```

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| affinity | object | `{}` |  |
| fullnameOverride | string | `""` |  |
| image.pullPolicy | string | `"IfNotPresent"` |  |
| image.repository | string | `""` | [required] The repository for the Docker image |
| image.tag | string | `""` | [required] The Docker image tag |
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