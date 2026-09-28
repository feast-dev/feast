# Milvus online store

## Description

The [Milvus](https://milvus.io/) online store provides support for materializing feature values into Milvus.

* The data model used to store feature values in Milvus is described in more detail [here](../../specs/online\_store\_format.md).

## Getting started
In order to use this online store, you'll need to install the Milvus extra (along with the dependency needed for the offline store of choice). E.g.

`pip install 'feast[milvus]'`

{% hint style="warning" %}
**Upgrading to milvus-lite 3.0.0+**

Feast supports both milvus-lite 2.x and 3.x. However, if you upgrade from milvus-lite 2.x.x to 3.0.0+, the `.db` files created by the original storage format are **not compatible** with the milvus-lite 3.0.0+ engine. You will need to re-import your data into a new database — automatic migration is not available.

See the [milvus-lite GitHub page](https://github.com/milvus-io/milvus-lite) for more details.
{% endhint %}

You can get started by using any of the other templates (e.g. `feast init -t gcp` or `feast init -t snowflake` or `feast init -t aws`), and then swapping in Milvus as the online store as seen below in the examples.

## Examples

Using Milvus Lite, which stores data in a local file:

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

Connecting to a self-hosted Milvus server:

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: milvus
  host: "http://localhost"
  port: 19530
  username: "username"
  password: "password"
  embedding_dim: 128
  index_type: "IVF_FLAT"
  metric_type: "COSINE"
```
{% endcode %}

## Configuration options

| Option | Default | Description |
|:-------|:--------|:------------|
| `path` | `""` | Path to a Milvus Lite database file. Used when `provider: local` and `path` is set. |
| `host` | `http://localhost` | Milvus server host, including the scheme. |
| `port` | `19530` | Milvus server port. |
| `username` / `password` | `""` | Credentials, sent as the token `username:password`. |
| `embedding_dim` | `128` | Dimension of vector fields. |
| `index_type` | `FLAT` | Index type for vector fields with `vector_index=True`. |
| `metric_type` | `COSINE` | Default metric when a field does not set `vector_search_metric`. |
| `nlist` | `128` | `nlist` index parameter. |
| `vector_enabled` | `true` | Enables vector search. |
| `varchar_max_length` | `65535` | Default `max_length` of VARCHAR fields. Override per field with the `max_length` tag. |
| `enable_openai_compatible_store` | `false` | Store numeric features as native Milvus numeric types. |

The full set of configuration options is available in [MilvusOnlineStoreConfig](https://rtd.feast.dev/en/latest/#feast.infra.online_stores.milvus.MilvusOnlineStoreConfig).

## Feature views without vectors

Milvus requires every collection to have a vector field. For feature views that have no vector
feature, Feast adds a 2-dimensional `_placeholder_vector` field with a FLAT index and fills it with zeros.
It is never returned or searched.

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

To compare this set of functionality against other online stores, please see the full [functionality matrix](overview.md#functionality-matrix).
