# Hybrid online store

## Description

The HybridOnlineStore allows routing online feature operations to different online store backends based on a configurable tag (such as `tribe`, `team`, or `project`) on the FeatureView. This enables a single Feast deployment to support multiple online store backends, each configured independently and selected dynamically at runtime.

## Getting started

To use the HybridOnlineStore, install Feast with all required online store dependencies (e.g., Bigtable, Cassandra, etc.) for the stores you plan to use. For example:

```
pip install 'feast[gcp,cassandra]'
```

## Example

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: hybrid
  routing_tag: team  # or any tag name you want to use in FeatureView's for routing
  online_stores:
    - type: bigtable
      conf:
        project_id: my_gcp_project
        instance: my_bigtable_instance
    - type: cassandra
      conf:
        hosts:
          - cassandra1.example.com
          - cassandra2.example.com
        keyspace: feast_keyspace
        username: feast_user
        password: feast_password
```
{% endcode %}

### Setting the Routing Tag in FeatureView

To enable routing, add a tag to your FeatureView that matches the `routing_tag` specified in your `feature_store.yaml`. For example, if your `routing_tag` is `team`, add a `team` tag to your FeatureView:

```yaml
tags:
  team: bigtable  # This tag determines which online store is used
```

The value of this tag (e.g., `bigtable`) should match the type or identifier of the online store you want to use for this FeatureView. The HybridOnlineStore will route all online operations for this FeatureView to the corresponding backend.

### Example FeatureView

{% code title="feature_view" %}
```yaml
name: user_features
entities:
  - name: user_id
    join_keys: ["user_id"]
ttl: null
schema:
  - name: age
    dtype: int64
  - name: country
    dtype: string
online: true
source:
  path: data/user_features.parquet
  event_timestamp_column: event_timestamp
  created_timestamp_column: created_timestamp
tags:
  team: bigtable  # This tag determines which online store is used
```
{% endcode %}

The `team` tag in the FeatureView's `tags` field determines which online store backend is used for this FeatureView. In this example, all online operations for `user_features` will be routed to the Bigtable online store, as specified by the tag value and the `routing_tag` in your `feature_store.yaml`.

The HybridOnlineStore will route requests to the correct online store based on the value of the tag specified by `routing_tag`.

The full set of configuration options for each online store is available in their respective documentation:
- [BigtableOnlineStoreConfig](https://rtd.feast.dev/en/latest/#feast.infra.online_stores.bigtable.BigtableOnlineStoreConfig)
- [CassandraOnlineStoreConfig](https://rtd.feast.dev/en/master/#feast.infra.online_stores.cassandra_online_store.cassandra_online_store.CassandraOnlineStoreConfig)

For a full explanation of configuration options, please refer to the documentation for each online store backend you configure in the `online_stores` list.

Storage specifications can be found at [docs/specs/online_store_format.md](../../specs/online_store_format.md).

## Functionality Matrix

The set of functionality supported by online stores is described in detail [here](overview.md#functionality). Below is a matrix indicating which functionality is supported by the HybridOnlineStore.

|                                                           | HybridOnlineStore |
|-----------------------------------------------------------|-------------------|
| write feature values to the online store                  | yes               |
| read feature values from the online store                 | yes               |
| update infrastructure (e.g. tables) in the online store   | yes               |
| teardown infrastructure (e.g. tables) in the online store | yes               |
| generate a plan of infrastructure changes                 | no                |
| support for on-demand transforms                          | yes               |
| readable by Python SDK                                    | yes               |
| readable by Java                                          | no                |
| readable by Go                                            | no                |
| support for entityless feature views                      | yes               |
| support for concurrent writing to the same key            | yes               |
| support for ttl (time to live) at retrieval               | no                |
| support for deleting expired data                         | no                |
| collocated by feature view                                | yes               |
| collocated by feature service                             | no                |
| collocated by entity key                                  | yes               |

To compare this set of functionality against other online stores, please see the full [functionality matrix](overview.md#functionality-matrix).
