# Redis online store

## Description

The [Milvus](https://milvus.io/) online store provides support for materializing feature values into Milvus.

* The data model used to store feature values in Milvus is described in more detail [here](../../specs/online\_store\_format.md).

## Getting started
In order to use this online store, you'll need to install the Milvus extra (along with the dependency needed for the offline store of choice). E.g.

`pip install 'feast[milvus]'`

You can get started by using any of the other templates (e.g. `feast init -t gcp` or `feast init -t snowflake` or `feast init -t aws`), and then swapping in Redis as the online store as seen below in the examples.

## Vector Search Configuration

### Field-Level Configuration (Recommended)

Starting with Feast 0.x, you can configure vector search parameters at the field level, allowing multiple teams to share the same Milvus online store with different vector configurations.

```python
from feast import Field, FeatureView
from feast.types import Array, Float32

# Marketing team: 768-dimension vectors with COSINE similarity
marketing_fv = FeatureView(
    name="marketing_embeddings",
    entities=[item],
    schema=[
        Field(
            name="product_vector",
            dtype=Array(Float32),
            vector_index=True,
            vector_length=768,           # Dimension specific to this field
            vector_search_metric="COSINE",  # Metric specific to this field
            vector_index_type="HNSW",    # Index type specific to this field
        ),
        Field(name="product_name", dtype=String),
    ],
    source=source,
    ttl=timedelta(days=1),
)

# Sales team: 128-dimension vectors with L2 distance
sales_fv = FeatureView(
    name="sales_embeddings",
    entities=[customer],
    schema=[
        Field(
            name="customer_vector",
            dtype=Array(Float32),
            vector_index=True,
            vector_length=128,              # Different dimension
            vector_search_metric="L2",      # Different metric
            vector_index_type="IVF_FLAT",   # Different index type
        ),
        Field(name="customer_name", dtype=String),
    ],
    source=source,
    ttl=timedelta(days=7),
)
```

**Field-level parameters:**
- `vector_length`: The dimension of the vector embedding (e.g., 128, 384, 768)
- `vector_search_metric`: The distance metric to use (e.g., "COSINE", "L2", "IP")
- `vector_index_type`: The index type for the vector field (e.g., "FLAT", "IVF_FLAT", "HNSW")

### Global Configuration (Backward Compatible)

For backward compatibility, you can still configure vector search parameters globally in the `feature_store.yaml`. These global settings serve as defaults when field-level parameters are not specified.

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: milvus
  path: "data/online_store.db"
  connection_string: "localhost:6379"
  embedding_dim: 128        # Default dimension if not specified in field
  index_type: "FLAT"        # Default index type if not specified in field
  metric_type: "COSINE"     # Default metric if not specified in field
  username: "username"
  password: "password"
```
{% endcode %}

**When to use global vs. field-level configuration:**
- Use **field-level configuration** when different feature views need different vector configurations (recommended for multi-team environments)
- Use **global configuration** as a fallback default or when all vectors have the same configuration

## Multi-Tenancy Support

Milvus supports multiple patterns for multi-tenancy as described in the [Milvus documentation](https://milvus.io/docs/multi_tenancy.md). With field-level vector configuration, Feast supports:

1. **Collection-level multi-tenancy**: Each feature view creates its own Milvus collection with its own vector configuration
2. **Field-level multi-tenancy**: Multiple vector fields within the same feature view can have different configurations

## Examples

### Basic Example - Single Vector Configuration

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: milvus
  path: "data/online_store.db"
  connection_string: "localhost:6379"
  embedding_dim: 128
  index_type: "FLAT"
  metric_type: "COSINE"
  username: "username"
  password: "password"
```
{% endcode %}

### Basic Example - Single Vector Configuration

Connecting to a local MilvusDB instance:

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: milvus
  path: "data/online_store.db"
  connection_string: "localhost:6379"
  embedding_dim: 128        # Default dimension (can be overridden at field level)
  index_type: "FLAT"        # Default index type (can be overridden at field level)
  metric_type: "COSINE"     # Default metric (can be overridden at field level)
  username: "username"
  password: "password"
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

To compare this set of functionality against other online stores, please see the full [functionality matrix](overview.md#functionality-matrix).
