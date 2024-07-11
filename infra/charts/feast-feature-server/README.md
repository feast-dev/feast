# Feast Python / Go Feature Server Helm Charts

Current chart version is `0.39.0`

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
| feast_mode | string | `"online"` | Feast supported deployment modes - online (default), offline, ui and registry |
| feature_store_yaml_base64 | string | `""` | [required] a base64 encoded version of feature_store.yaml |
| fullnameOverride | string | `""` |  |
| image.pullPolicy | string | `"IfNotPresent"` |  |
| image.repository | string | `"feastdev/feature-server"` | Docker image for Feature Server repository |
| image.tag | string | `"0.39.0"` | The Docker image tag (can be overwritten if custom feature server deps are needed for on demand transforms) |
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

## Adding Monitoring
To add monitoring to the Feast Feature Server, follow these steps:

### Configuration
Update your Helm values file to enable monitoring and configure the OpenTelemetry Collector.

Feast instrumentation Using OpenTelemetry and Prometheus - https://lucid.app/lucidchart/b07f9c86-e31b-4e19-8123-0a64ba532096/edit?page=0_0&invitationId=inv_b57cd30b-5741-436f-baac-2e4b547ac2a5#

### Deploy Feast 
Deploy Feast and set `metrics` value to `true`.

Example - 
```
helm install feast-release infra/charts/feast-feature-server --set metric=true --set feature_store_yaml_base64=""
```

### Deploy Prometheus Operator
Navigate to OperatorHub and install the stable version of the Prometheus Operator

### Deploy OpenTelemetry Operator
Before installing OTEL Operator, install `cert-manager` and validate the `pods` should spin up --
```
kubectl apply -f https://github.com/open-telemetry/opentelemetry-operator/releases/latest/download/opentelemetry-operator.yaml
```

Then navigate to OperatorHub and install the stable version of the community OpenTelemetry Operator


### Configure OpenTelemetry Collector
Add the OpenTelemetry Collector configuration under the metrics section in your values.yaml file.

Example values.yaml:

```
metrics:
  enabled: true
  otelCollector:
    endpoint: "otel-collector.default.svc.cluster.local:4317"
    headers:
      api-key: "your-api-key"
```

### Add instrumentation annotation and environment variables in the deployment.yaml - 

```
template:
    metadata:
    {{- with .Values.podAnnotations }}
      annotations:
        {{- toYaml . | nindent 8 }}
        instrumentation.opentelemetry.io/inject-python: "true"
```

```
- name: OTEL_EXPORTER_OTLP_ENDPOINT
    value: http://{{ .Values.service.name }}-collector.{{ .Release.namespace }}.svc.cluster.local:{{ .Values.metrics.endpoint.port}}
- name: OTEL_EXPORTER_OTLP_INSECURE
              value: "true"     
```

### Deploy manifests -
Add Instrumentation, OpenTelemetryCollector, ServiceMonitors, Prometheus Instance and RBAC rules as provided in the samples/ directory.

For latest updates please refer the official repository - https://github.com/open-telemetry/opentelemetry-operator

## Introduction to the Custom Resources used 
Monitoring Feast involves several components that work together to collect, process, and visualize metrics. Here's a brief introduction to each component:

1. Instrumentation: This refers to the process of adding code to your application to collect metrics and traces. For Feast, this involves enabling the collection of relevant metrics from the feature store.

2. OpenTelemetryCollector: The OpenTelemetry Collector is a vendor-agnostic way to receive, process, and export telemetry data. It supports various data sources and destinations, making it a flexible solution for monitoring.

3. ServiceMonitor: A ServiceMonitor is used by Prometheus Operator to monitor services running on Kubernetes. It specifies how and which services should be scraped for metrics.

4. Prometheus: Prometheus is an open-source systems monitoring and alerting toolkit. It collects and stores metrics as time series data, providing powerful querying capabilities.

5. RBAC Policies: Role-Based Access Control (RBAC) policies define the permissions for accessing and managing resources in a Kubernetes cluster. Proper RBAC policies ensure that monitoring components can securely access the necessary resources.


## See logs 
Once the opentelemetry is deployed, you can search the logs to see the required metrics - 

```
oc logs otelcol-collector-0 | grep "Name: feast_feature_server_memory_usage\|Value: 0.*"
oc logs otelcol-collector-0 | grep "Name: feast_feature_server_cpu_usage\|Value: 0.*"
```
```
 -> Name: feast_feature_server_memory_usage
Value: 0.579426
```
```
-> Name: feast_feature_server_cpu_usage
Value: 0.000000
```
