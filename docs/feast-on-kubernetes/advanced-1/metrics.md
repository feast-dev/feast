# Metrics

{% hint style="warning" %}
This page applies to Feast 0.7. The content may be out of date for Feast 0.8+
{% endhint %}

## Overview

Feast Components export metrics that can provide insight into Feast behavior:

* [Feast Ingestion Jobs  can be configured to push metrics into StatsD](metrics.md#pushing-ingestion-metrics-to-statsd)
* [Prometheus can be configured to scrape metrics from Feast Core and Serving.](metrics.md#exporting-feast-metrics-to-prometheus)

See the [Metrics Reference ](../reference-1/metrics-reference.md)for documentation on metrics are exported by Feast.

{% hint style="info" %}
Feast Job Controller currently does not export any metrics on its own. However its `application.yml` is used to configure metrics export for ingestion jobs.
{% endhint %}

## Pushing Ingestion Metrics to StatsD

### **Feast Ingestion Job**

Feast Ingestion Job can be configured to push Ingestion metrics to a StatsD instance. Metrics export to StatsD for Ingestion Job is configured in Job Controller's `application.yml` under `feast.jobs.metrics`

```yaml
 feast:
   jobs:
    metrics:
      # Enables Statd metrics export if true.
      enabled: true
      type: statsd
      # Host and port of the StatsD instance to export to.
      host: localhost
      port: 9125
```

{% hint style="info" %}
If you need Ingestion Metrics in Prometheus or some other metrics backend, use a metrics forwarder to forward Ingestion Metrics from StatsD to the metrics backend of choice. \(ie Use [`prometheus-statsd-exporter`](https://github.com/prometheus/statsd_exporter) to forward metrics to Prometheus\).
{% endhint %}

## Exporting Feast Metrics to Prometheus

### **Feast Core and Serving**

Feast Core and Serving exports metrics to a Prometheus instance via Prometheus scraping its `/metrics` endpoint. Metrics export to Prometheus for Core and Serving can be configured via their corresponding `application.yml`

```yaml
server:
  # Configures the port where metrics are exposed via /metrics for Prometheus to scrape.
  port: 8081
```

[Direct Prometheus](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#scrape_config) to scrape directly from Core and Serving's `/metrics` endpoint.

## Further Reading

See the [Metrics Reference ](../reference-1/metrics-reference.md)for documentation on metrics are exported by Feast.

