# Metrics

{% hint style="danger" %}
We strongly encourage all users to upgrade from Feast 0.9 to Feast 0.10+. Please see [this](https://docs.feast.dev/v/master/project/feast-0.9-vs-feast-0.10+) for an explanation of the differences between the two versions. A guide to upgrading can be found [here](https://docs.google.com/document/d/1AOsr_baczuARjCpmZgVd8mCqTF4AZ49OEyU4Cn-uTT0/edit#heading=h.9gb2523q4jlh). 
{% endhint %}

## Overview

Feast Components export metrics that can provide insight into Feast behavior:

* [Feast Ingestion Jobs  can be configured to push metrics into StatsD](metrics.md#2-exporting-feast-metrics-to-prometheus)
* [Prometheus can be configured to scrape metrics from Feast Core and Serving.](metrics.md#2-exporting-feast-metrics-to-prometheus)

See the [Metrics Reference ](../reference/metrics-reference.md)for documentation on metrics are exported by Feast.

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

See the [Metrics Reference ](../reference/metrics-reference.md)for documentation on metrics are exported by Feast.

