# feature\_store.yaml

## Overview

`feature_store.yaml` is a file that is placed at the root of the [Feature Repository](feature-repository.md). This file contains configuration about how the feature store runs. An example `feature_store.yaml` is shown below:

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

**Permissions**

<table>
  <thead>
    <tr>
      <th style="text-align:left"><b>Command</b>
      </th>
      <th style="text-align:left">Component</th>
      <th style="text-align:left">Permissions</th>
      <th style="text-align:left">Recommended Role</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align:left"><b>Apply</b>
      </td>
      <td style="text-align:left">BigQuery (source)</td>
      <td style="text-align:left">
        <p>bigquery.jobs.create</p>
        <p>bigquery.readsessions.create</p>
        <p>bigquery.readsessions.getData</p>
      </td>
      <td style="text-align:left">roles/bigquery.user</td>
    </tr>
    <tr>
      <td style="text-align:left"><b>Apply</b>
      </td>
      <td style="text-align:left">Datastore (destination)</td>
      <td style="text-align:left">
        <p>datastore.entities.allocateIds</p>
        <p>datastore.entities.create</p>
        <p>datastore.entities.delete</p>
        <p>datastore.entities.get</p>
        <p>datastore.entities.list</p>
        <p>datastore.entities.update</p>
      </td>
      <td style="text-align:left">roles/datastore.owner</td>
    </tr>
    <tr>
      <td style="text-align:left"><b>Materialize</b>
      </td>
      <td style="text-align:left">BigQuery (source)</td>
      <td style="text-align:left">bigquery.jobs.create</td>
      <td style="text-align:left">roles/bigquery.user</td>
    </tr>
    <tr>
      <td style="text-align:left"><b>Materialize</b>
      </td>
      <td style="text-align:left">Datastore (destination)</td>
      <td style="text-align:left">
        <p>datastore.entities.allocateIds</p>
        <p>datastore.entities.create</p>
        <p>datastore.entities.delete</p>
        <p>datastore.entities.get</p>
        <p>datastore.entities.list</p>
        <p>datastore.entities.update</p>
        <p>datastore.databases.get</p>
      </td>
      <td style="text-align:left">roles/datastore.owner</td>
    </tr>
    <tr>
      <td style="text-align:left"><b>Get Online Features</b>
      </td>
      <td style="text-align:left">Datastore</td>
      <td style="text-align:left">datastore.entities.get</td>
      <td style="text-align:left">roles/datastore.user</td>
    </tr>
    <tr>
      <td style="text-align:left"><b>Get Historical Features</b>
      </td>
      <td style="text-align:left">BigQuery (source)</td>
      <td style="text-align:left">
        <p>bigquery.datasets.get</p>
        <p>bigquery.tables.get</p>
        <p>bigquery.tables.create</p>
        <p>bigquery.tables.updateData</p>
        <p>bigquery.tables.update</p>
        <p>bigquery.tables.delete</p>
        <p>bigquery.tables.getData</p>
      </td>
      <td style="text-align:left">roles/bigquery.dataEditor</td>
    </tr>
  </tbody>
</table>

