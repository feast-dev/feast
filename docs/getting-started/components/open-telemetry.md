# OpenTelemetry Integration

The OpenTelemetry integration in Feast provides comprehensive monitoring and observability capabilities for your feature serving infrastructure. This component enables you to track key metrics, traces, and logs from your Feast deployment.

## Motivation

Monitoring and observability are critical for production machine learning systems. The OpenTelemetry integration addresses these needs by:

1. **Performance Monitoring:** Track CPU and memory usage of feature servers
2. **Operational Insights:** Collect metrics to understand system behavior and performance
3. **Troubleshooting:** Enable effective debugging through distributed tracing
4. **Resource Optimization:** Monitor resource utilization to optimize deployments
5. **Production Readiness:** Provide enterprise-grade observability capabilities

## Architecture

The OpenTelemetry integration in Feast consists of several components working together:

- **OpenTelemetry Collector:** Receives, processes, and exports telemetry data
- **Prometheus Integration:** Enables metrics collection and monitoring
- **Instrumentation:** Automatic Python instrumentation for tracking metrics
- **Exporters:** Components that send telemetry data to monitoring systems

## Key Features

1. **Automated Instrumentation:** Python auto-instrumentation for comprehensive metric collection
2. **Metric Collection:** Track key performance indicators including:
   - Memory usage
   - CPU utilization
   - Request latencies
   - Feature retrieval statistics
3. **Flexible Configuration:** Customizable metric collection and export settings
4. **Kubernetes Integration:** Native support for Kubernetes deployments
5. **Prometheus Compatibility:** Integration with Prometheus for metrics visualization

## Setup and Configuration

To add monitoring to the Feast Feature Server, follow these steps:

### 1. Deploy Prometheus Operator
Follow the [Prometheus Operator documentation](https://github.com/prometheus-operator/prometheus-operator/blob/main/Documentation/user-guides/getting-started.md) to install the operator.

### 2. Deploy OpenTelemetry Operator
Before installing the OpenTelemetry Operator:
1. Install `cert-manager`
2. Validate that the `pods` are running
3. Apply the OpenTelemetry operator:
```bash
kubectl apply -f https://github.com/open-telemetry/opentelemetry-operator/releases/latest/download/opentelemetry-operator.yaml
```

For additional installation steps, refer to the [OpenTelemetry Operator documentation](https://github.com/open-telemetry/opentelemetry-operator).

### 3. Configure OpenTelemetry Collector
Add the OpenTelemetry Collector configuration under the metrics section in your values.yaml file:

```yaml
metrics:
  enabled: true
  otelCollector:
    endpoint: "otel-collector.default.svc.cluster.local:4317"  # sample
    headers:
      api-key: "your-api-key"
```

### 4. Add Instrumentation Configuration
Add the following annotations and environment variables to your deployment.yaml:

```yaml
template:
  metadata:
    annotations:
      instrumentation.opentelemetry.io/inject-python: "true"
```

```yaml
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: http://{{ .Values.service.name }}-collector.{{ .Release.namespace }}.svc.cluster.local:{{ .Values.metrics.endpoint.port}}
- name: OTEL_EXPORTER_OTLP_INSECURE
  value: "true"
```

### 5. Add Metric Checks
Add metric checks to all manifests and deployment files:

```yaml
{{ if .Values.metrics.enabled }}
apiVersion: opentelemetry.io/v1alpha1
kind: Instrumentation
metadata:
  name: feast-instrumentation
spec:
  exporter:
    endpoint: http://{{ .Values.service.name }}-collector.{{ .Release.Namespace }}.svc.cluster.local:4318
  env:
  propagators:
    - tracecontext
    - baggage
  python:
    env:
      - name: OTEL_METRICS_EXPORTER
        value: console,otlp_proto_http
      - name: OTEL_LOGS_EXPORTER
        value: otlp_proto_http
      - name: OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED
        value: "true"
{{end}}
```

### 6. Add Required Manifests
Add the following components to your chart:
- Instrumentation
- OpenTelemetryCollector
- ServiceMonitors
- Prometheus Instance
- RBAC rules

### 7. Deploy Feast
Deploy Feast with metrics enabled:

```bash
helm install feast-release infra/charts/feast-feature-server --set metric=true --set feature_store_yaml_base64=""
```

## Usage

To enable OpenTelemetry monitoring in your Feast deployment:

1. Set `metrics.enabled=true` in your Helm values
2. Configure the OpenTelemetry Collector endpoint
3. Deploy with proper annotations and environment variables

Example configuration:
```yaml
metrics:
  enabled: true
  otelCollector:
    endpoint: "otel-collector.default.svc.cluster.local:4317"
```

## Monitoring

Once configured, you can monitor various metrics including:

- `feast_feature_server_memory_usage`: Memory utilization of the feature server
- `feast_feature_server_cpu_usage`: CPU usage statistics
- Additional custom metrics based on your configuration

These metrics can be visualized using Prometheus and other compatible monitoring tools.
