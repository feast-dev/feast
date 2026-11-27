# ScyllaDB Cloud online store

## Description

[ScyllaDB](https://www.scylladb.com/) is a distributed real-time NoSQL database with vector search support.
This integration uses the native **`scylla-driver`** Python driver for optimised performance and supports materializing feature values into a [ScyllaDB Cloud](https://www.scylladb.com/product/scylla-cloud/) cluster for real-time online feature serving.

## Getting started

Install Feast with the `scylladb` extra, which pulls in `scylla-driver` automatically:

```bash
pip install feast[scylladb]
```

### Example (ScyllaDB)

{% code title="feature_store.yaml" %}
```yaml
project: scylla_feature_repo
registry: data/registry.db
provider: local
online_store:
    type: scylladb
    hosts:
        - 172.17.0.2
    keyspace: feast
    username: scylla
    password: password
```
{% endcode %}

### Example (ScyllaDB Cloud)

{% code title="feature_store.yaml" %}
```yaml
project: scylla_feature_repo
registry: data/registry.db
provider: local
online_store:
    type: scylladb
    hosts:
        - node-0.aws_us_east_1.xxxxxxxx.clusters.scylla.cloud
        - node-1.aws_us_east_1.xxxxxxxx.clusters.scylla.cloud
        - node-2.aws_us_east_1.xxxxxxxx.clusters.scylla.cloud
    keyspace: feast
    username: scylla
    password: xxxxxx
    local_dc: AWS_US_EAST_1
```
{% endcode %}

## Configuration options

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `hosts` | list[str] | *(required)* | Contact-point host addresses. |
| `port` | int | `9042` | CQL port. |
| `keyspace` | str | `feast_keyspace` | Target ScyllaDB keyspace. |
| `username` | str | `None` | Auth username. |
| `password` | str | `None` | Auth password. |
| `local_dc` | str | `None` | Local datacenter name for DC-aware load balancing. |
| `request_timeout` | float | `None` | Driver request timeout in seconds. |
| `read_concurrency` | int | `100` | `concurrency` argument passed to the driver's `execute_concurrent_with_args` for reads. Controls how many CQL statements are in-flight at once. |
| `write_concurrency` | int | `100` | `concurrency` argument passed to the driver's `execute_concurrent_with_args` for writes. Controls how many CQL statements are in-flight at once. |
| `vector_similarity_function` | str | `COSINE` | Default similarity function for vector indexes. Supported: `COSINE`, `DOT_PRODUCT`, `EUCLIDEAN`. Can be overridden per-feature via the `similarity_function` Field tag. |

Storage specifications can be found at `docs/specs/online_store_format.md`.

## Vector Search

ScyllaDB Cloud supports approximate nearest-neighbour (ANN) vector search.
To enable it for a feature view, tag the embedding `Field` with `vector_index=true` and specify the number of dimensions:

{% code title="feature_definitions.py" %}
```python
from feast import FeatureView, Field
from feast.types import Array, Float32, String

documents_fv = FeatureView(
    name="documents",
    entities=[item],
    schema=[
        Field(name="text", dtype=String),
        Field(
            name="embedding",
            dtype=Array(Float32),
            tags={
                "vector_index": "true",
                "dimensions": "768",
                "similarity_function": "COSINE",  # COSINE | DOT_PRODUCT | EUCLIDEAN
            },
        ),
    ],
    online=True,
    source=push_source,
)
```
{% endcode %}

When `feast apply` runs, the store automatically creates the necessary tables and HNSW ANN index for any feature view with vector-tagged fields.

To query the top-k most similar documents:

```python
result = store.retrieve_online_documents_v2(
    features=["documents:text", "documents:embedding"],
    query=[0.1, 0.2, ...],   # your query embedding
    top_k=10,
    distance_metric="COSINE",
)
```

### Metadata filtering (OpenAI-compatible)

ScyllaDB supports vector similarity search, but OpenAI-style metadata filtering is **not supported yet**.
Passing `filters` to `retrieve_online_documents_v2` or the OpenAI-compatible search endpoint raises `NotImplementedError`.

For filtered vector search today, use one of the backends that implement metadata filters (for example Milvus, Elasticsearch, Postgres, SQLite, or MongoDB). See [Alpha Vector Database](../alpha-vector-database.md#supported-online-stores).

## Functionality Matrix

The set of functionality supported by online stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the ScyllaDB online store.

|                                                           | ScyllaDB  |
| :-------------------------------------------------------- | :-------- |
| write feature values to the online store                  | yes       |
| read feature values from the online store                 | yes       |
| update infrastructure (e.g. tables) in the online store   | yes       |
| teardown infrastructure (e.g. tables) in the online store | yes       |
| generate a plan of infrastructure changes                 | no        |
| support for on-demand transforms                          | yes       |
| readable by Python SDK                                    | yes       |
| readable by Java                                          | no        |
| readable by Go                                            | no        |
| support for entityless feature views                      | yes       |
| support for concurrent writing to the same key            | no        |
| support for ttl (time to live) at retrieval               | yes       |
| support for deleting expired data                         | yes       |
| collocated by feature view                                | yes       |
| collocated by feature service                             | no        |
| collocated by entity key                                  | no        |

To compare this set of functionality against other online stores, please see the full [functionality matrix](overview.md#functionality-matrix).

## Resources

* [ScyllaDB Vector Search documentation](https://cloud.docs.scylladb.com/stable/vector-search/)
* [ScyllaDB website](https://www.scylladb.com/)
* [ScyllaDB Cloud documentation](https://cloud.docs.scylladb.com/stable/)
