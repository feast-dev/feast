# Datastore

### Description

The [Datastore](https://cloud.google.com/datastore) online store provides support for materializing feature values into Cloud Datastore. The data model used to store feature values in Datastore is described in more detail [here](https://github.com/feast-dev/feast/blob/master/docs/specs/online_store_format.md#google-datastore-online-store-format).

### Example

{% code title="feature\_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: gcp
online_store:
  type: datastore
  project_id: my_gcp_project
  namespace: my_datastore_namespace
```
{% endcode %}

Configuration options are available [here](https://rtd.feast.dev/en/latest/#feast.repo_config.DatastoreOnlineStoreConfig).

