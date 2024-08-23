# GCS Registry

## Description

GCS registry provides support for storing the protobuf representation of your feature store objects (data sources, feature views, feature services, etc.) uing Google Cloud Storage.

While it can be used in production, there are still inherent limitations with a file-based registries, since changing a single field in the registry requires re-writing the whole registry file. With multiple concurrent writers, this presents a risk of data loss, or bottlenecks writes to the registry since all changes have to be serialized (e.g. when running materialization for multiple feature views or time ranges concurrently).

An example of how to configure this would be:

## Example

{% code title="feature_store.yaml" %}
```yaml
project: feast_gcp
registry:
  path: gs://[YOUR BUCKET YOU CREATED]/registry.pb
  cache_ttl_seconds: 60
online_store: null
offline_store:
  type: dask
```
{% endcode %}