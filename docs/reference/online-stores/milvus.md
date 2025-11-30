# Milvus online store

## Description

The [Milvus](https://milvus.io/) online store provides support for materializing feature values into Milvus.

* The data model used to store feature values in Milvus is described in more detail [here](../../specs/online\_store\_format.md).
* Multi-tenancy behavior is controlled by the `multi_tenancy_mode` setting (see below).

## Getting started
In order to use this online store, you'll need to install the Milvus extra (along with the dependency needed for the offline store of choice). E.g.

`pip install 'feast[milvus]'`

You can get started by using any of the other templates (e.g. `feast init -t gcp` or `feast init -t snowflake` or `feast init -t aws`), and then swapping in Milvus as the online store as seen below in the examples.

## Examples

### Local mode (file-based)

Connecting to a local MilvusDB instance:

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: milvus
  path: "data/online_store.db"
  embedding_dim: 128
  index_type: "FLAT"
  metric_type: "COSINE"
```
{% endcode %}

### Remote mode

Connecting to a remote Milvus instance:

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: milvus
  host: "http://localhost"
  port: 19530
  embedding_dim: 128
  index_type: "FLAT"
  metric_type: "COSINE"
  username: "username"
  password: "password"
```
{% endcode %}

### Multi-Tenancy Modes

Feast supports three mutually exclusive multi-tenancy strategies for Milvus. Set `multi_tenancy_mode` to make intent explicit; legacy configs are inferred for backwards compatibility.

- `none` (default): single-tenant; disallows `db_name` and `collection_name_prefix`
- `database`: per-tenant database isolation; requires `db_name`; forbids `collection_name_prefix`; not supported with local (Milvus Lite) mode
- `collection`: shared database with per-tenant collection prefix; requires `collection_name_prefix`; forbids `db_name`

#### Database isolation (`multi_tenancy_mode: database`)

For multi-tenant environments, you can use database-level isolation by specifying the `db_name` parameter. This creates a separate Milvus database for each tenant, providing logical isolation of collections and data:

{% code title="feature_store.yaml" %}
```yaml
# Marketing team configuration
project: marketing_features
registry: data/registry.db
provider: remote
online_store:
  type: milvus
  host: "http://localhost"
  port: 19530
  multi_tenancy_mode: database
  db_name: "marketing_db"
  embedding_dim: 768
  index_type: "IVF_FLAT"
  metric_type: "COSINE"
```
{% endcode %}

{% code title="feature_store.yaml" %}
```yaml
# Sales team configuration
project: sales_features
registry: data/registry.db
provider: remote
online_store:
  type: milvus
  host: "http://localhost"
  port: 19530
  multi_tenancy_mode: database
  db_name: "sales_db"
  embedding_dim: 128
  index_type: "FLAT"
  metric_type: "L2"
```
{% endcode %}

With database-level multi-tenancy:
- Each team's collections are isolated in their own database
- Different embedding dimensions can be used per database
- Different vector index types and metrics can be configured per team
- Access control can be managed at the database level in Milvus

#### Collection prefix isolation (`multi_tenancy_mode: collection`)

Use a shared Milvus database and isolate tenants via collection name prefixes:

{% code title="feature_store.yaml" %}
```yaml
project: shared_features
registry: data/registry.db
provider: remote
online_store:
  type: milvus
  host: "http://localhost"
  port: 19530
  multi_tenancy_mode: collection
  collection_name_prefix: "tenant_alpha"
  embedding_dim: 128
  index_type: "FLAT"
  metric_type: "COSINE"
```
{% endcode %}

The full set of configuration options is available in [MilvusOnlineStoreConfig](https://rtd.feast.dev/en/latest/#feast.infra.online_stores.milvus.MilvusOnlineStoreConfig).

## Functionality Matrix

The set of functionality supported by online stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the Milvus online store.

|                                                           | Milvus |
|:----------------------------------------------------------|:-------|
| write feature values to the online store                  | yes    |
| read feature values from the online store                 | yes    |
| update infrastructure (e.g. tables) in the online store   | yes    |
| teardown infrastructure (e.g. tables) in the online store | yes    |
| generate a plan of infrastructure changes                 | no     |
| support for on-demand transforms                          | yes    |
| readable by Python SDK                                    | yes    |
| readable by Java                                          | no     |
| readable by Go                                            | no     |
| support for entityless feature views                      | yes    |
| support for concurrent writing to the same key            | yes    |
| support for ttl (time to live) at retrieval               | yes    |
| support for deleting expired data                         | yes    |
| collocated by feature view                                | no     |
| collocated by feature service                             | no     |
| collocated by entity key                                  | no     |
| vector similarity search                                  | yes    |
| database-level multi-tenancy                              | yes    |

To compare this set of functionality against other online stores, please see the full [functionality matrix](overview.md#functionality-matrix).
