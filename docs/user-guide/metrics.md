# Metrics

## 0. Overview

Feast Components \(Core, Serving, Job Controller, Ingestion Jobs\) export metrics that can provide visibility into Feast usage and behavior such as:

* No. of Feature Sets registered with Feast Core.
* Online Feature Request latency retrieving from Feast Serving.
* Lag time between Feature Rows ingested into Feast Ingestion Jobs.

## 1. Exporting Metrics to StatsD

Feast directly supports exporting metrics to a StatsD instance.

* Metrics export to StatsD for Core, Serving and Job Controller components are configured in the component's coresponding`application.yml`

```yaml
management:
  metrics:
    export:
      statsd:
        # Enables Statd metrics export if true.
        enabled: true
        # Host and port of the StatsD instance to export to.
        host: localhost
        port: 8125
```

* Metrics export for Ingestion for the Ingestion Job component is configured in Job Controller's `application.yml` under `feast.jobs.metrics`

```yaml
 feast:
   jobs:
    metrics:
      # Enables Statd metrics export if true.
      enabled: false
      # Type of metrics sink. Only statsd is currently supported.
      type: statsd
      # Host of the metrics sink.
      host: localhost
      # Port of the metrics sink.
      port: 9125
```





