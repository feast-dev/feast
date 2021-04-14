# feature\_store.yaml

## Overview

`feature_store.yaml` is a file that is placed at the root of the [Feature Repository](../concepts/feature-repository.md). This file contains configuration about how the feature store runs. An example `feature_store.yaml` is shown below:

{% code title="feature\_store.yaml" %}
```yaml
project: loyal_spider
registry: data/registry.db
provider: local
online_store:
    type: sqlite
    path: data/online_store.db
```
{% endcode %}

## Fields in feature\_store.yaml

* **provider** \("local" or "gcp"\)  — Defines the environment in which Feast will execute data flows.
* **registry** \(a local or GCS filepath\) — Defines the location of the feature registry.
* **online\_store** — Configures the online store. This field will have various subfields depending on the type of online store:
  * **type** \("sqlite" or "datastore"\) — Defines the type of online store.
  * **path** \(a local filepath\) — Parameter for the sqlite online store. Defines the path to the SQLite database file.
  * **project\_id**  — Optional parameter for the datastore online store. Sets the GCP project id used by Feast, if not set Feast will use the default GCP project id in the local environment.
* **project** — Defines a namespace for the entire feature store. Can be used to isolate multiple deployments in a single installation of Feast.

## Providers

The `provider` field defines the environment in which Feast will execute data flows. As a result, it also determines the default values for other fields.

### Local

When using the local provider:

* Feast can read from **local Parquet data sources.**
* Feast performs historical feature retrieval \(point-in-time joins\) using **pandas.**
* Feast performs online feature serving from a **SQLite database.**

### **GCP**

When using the GCP provider:

* Feast can read data from **BigQuery data sources.**
* Feast performs historical feature retrieval \(point-in-time joins\) in **BigQuery.**
* Feast performs online feature serving from **Google Cloud Datastore.**

