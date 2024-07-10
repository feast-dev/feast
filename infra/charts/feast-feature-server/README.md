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

![Blank diagram (3)](https://gist.github.com/assets/67011812/a3975749-dea3-4b5a-bae6-638e2895464f)

## Deploy Feast 
Deploy Feast and set `metrics` value to `true`.

Example - 
```
helm install feast-release infra/charts/feast-feature-server --set metric=true --set feature_store_yaml_base64=""
```

## Deploy Prometheus Operator
Navigate to OperatorHub and install the stable version of the Prometheus Operator

## Deploy OpenTelemetry Operator
Before installing OTEL Operator, install `cert-manager` and validate the `pods` should spin up --
```
kubectl apply -f https://github.com/open-telemetry/opentelemetry-operator/releases/latest/download/opentelemetry-operator.yaml
```

Then navigate to OperatorHub and install the stable version of the community OpenTelemetry Operator

## Deploy k8s resources
Monitoring Feast involves several components that work together to collect, process, and visualize metrics. Here's a brief introduction to each component:

1. Instrumentation: This refers to the process of adding code to your application to collect metrics and traces. For Feast, this involves enabling the collection of relevant metrics from the feature store.

2. OpenTelemetryCollector: The OpenTelemetry Collector is a vendor-agnostic way to receive, process, and export telemetry data. It supports various data sources and destinations, making it a flexible solution for monitoring.

3. ServiceMonitor: A ServiceMonitor is used by Prometheus Operator to monitor services running on Kubernetes. It specifies how and which services should be scraped for metrics.

4. Prometheus: Prometheus is an open-source systems monitoring and alerting toolkit. It collects and stores metrics as time series data, providing powerful querying capabilities.

5. RBAC Policies: Role-Based Access Control (RBAC) policies define the permissions for accessing and managing resources in a Kubernetes cluster. Proper RBAC policies ensure that monitoring components can securely access the necessary resources.


## See logs 
Once the opentelemetry is deployed, you can search the logs to see the required metrics - 

```
 oc logs otelcol-collector-0 | grep "Name:\|Value:" | uniq
 ```
 <img width="416" alt="Screenshot 2024-06-20 at 7 25 03 PM" src="https://gist.github.com/assets/67011812/e2d9a469-fc07-4aa7-a100-b55a88f704b5">
<img width="416" alt="Screenshot 2024-06-20 at 7 25 03 PM" src="https://gist.github.com/assets/67011812/c3f9baaf-5438-44a5-a419-c4d6de459a5a">

```
oc logs otelcol-collector-0 | grep "Name: feast_feature_server_memory_usage\|Value: 0.*"
oc logs otelcol-collector-0 | grep "Name: feast_feature_server_cpu_usage\|Value: 0.*"
```
<img width="442" alt="Screenshot 2024-06-26 at 8 56 45 PM" src="https://gist.github.com/assets/67011812/f1a71306-b051-4e2a-896e-0b1c78230a1f">
<img width="442" alt="Screenshot 2024-06-26 at 8 56 45 PM" src="https://gist.github.com/assets/67011812/ae5e5083-cf00-47d4-aac4-1f3779ac1aff">
