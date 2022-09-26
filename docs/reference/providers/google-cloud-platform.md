# Google Cloud Platform

## Description

* Offline Store: Uses the **BigQuery** offline store by default. Also supports File as the offline store.
* Online Store: Uses the **Datastore** online store by default. Also supports Sqlite as an online store.

## Getting started
In order to use this offline store, you'll need to run `pip install 'feast[gcp]'`. You can get started by then running `feast init -t gcp`.

## Example

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: gs://my-bucket/data/registry.db
provider: gcp
```
{% endcode %}

## **Permissions**

| **Command**                 | Component               | Permissions                                                                                                                                                                                                                    | Recommended Role          |
| --------------------------- | ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------- |
| **Apply**                   | BigQuery (source)       | <p>bigquery.jobs.create</p><p>bigquery.readsessions.create</p><p>bigquery.readsessions.getData</p>                                                                                                                             | roles/bigquery.user       |
| **Apply**                   | Datastore (destination) | <p>datastore.entities.allocateIds</p><p>datastore.entities.create</p><p>datastore.entities.delete</p><p>datastore.entities.get</p><p>datastore.entities.list</p><p>datastore.entities.update</p>                               | roles/datastore.owner     |
| **Materialize**             | BigQuery (source)       | bigquery.jobs.create                                                                                                                                                                                                           | roles/bigquery.user       |
| **Materialize**             | Datastore (destination) | <p>datastore.entities.allocateIds</p><p>datastore.entities.create</p><p>datastore.entities.delete</p><p>datastore.entities.get</p><p>datastore.entities.list</p><p>datastore.entities.update</p><p>datastore.databases.get</p> | roles/datastore.owner     |
| **Get Online Features**     | Datastore               | datastore.entities.get                                                                                                                                                                                                         | roles/datastore.user      |
| **Get Historical Features** | BigQuery (source)       | <p>bigquery.datasets.get</p><p>bigquery.tables.get</p><p>bigquery.tables.create</p><p>bigquery.tables.updateData</p><p>bigquery.tables.update</p><p>bigquery.tables.delete</p><p>bigquery.tables.getData</p>                   | roles/bigquery.dataEditor |
