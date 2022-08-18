# Feast Python / Go Feature Server Helm Charts

Feast Feature Server in Go or Python
Current chart version is `0.23.0`

## Installation
Docker repository and tag are required. Helm install example:
```
helm install feast-feature-server . --set image.repository=REPO --set image.tag=TAG
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

Furthermore, if you wish to use the Go feature server, then you must install the Apache Arrow C++ libraries, and your `feature_store.yaml` should include `go_feature_server: True`.
 For more details, see the [docs](https://docs.feast.dev/reference/feature-servers/go-feature-server).
 The docker image might look like:
 ```
 FROM python:3.8
 RUN apt update && \
     apt install -y jq
 RUN pip install pip --upgrade
 RUN pip install feast
 RUN apt update
 RUN apt install -y -V ca-certificates lsb-release wget
 RUN wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
 RUN apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
 RUN apt update
 RUN apt -y install libarrow-dev
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