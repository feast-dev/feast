# MongoDB source (contrib)

## Description

MongoDB data sources are [MongoDB](https://www.mongodb.com/) collections that can be used as a source for feature data. The `MongoDBSource` points at a MongoDB collection and provides the metadata Feast needs to read historical features from the offline store's collection.

## Examples

Defining a MongoDB source:

```python
from feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb import (
    MongoDBSource,
)

driver_stats_source = MongoDBSource(
    name="driver_stats",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_at",
)
```

The `name` field becomes the `feature_view` discriminator stored in every document in the `feature_history` collection.

Configuration options such as `connection_string`, `database`, and `collection` are inherited from the offline store configuration in `feature_store.yaml`.

The full set of configuration options is available [here](https://rtd.feast.dev/en/master/#feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb.MongoDBSource).

## Vector Search

The MongoDB online store supports [Atlas Vector Search](https://www.mongodb.com/docs/atlas/atlas-vector-search/), enabling similarity search over feature embeddings stored in MongoDB Atlas. This is powered by the `$vectorSearch` aggregation stage and requires MongoDB Atlas (or the `mongodb/mongodb-atlas-local` Docker image for local development).

See [PR #6344](https://github.com/feast-dev/feast/pull/6344) for full implementation details.

### Configuration

Enable vector search in your `feature_store.yaml`:

```yaml
project: my_project
provider: local
online_store:
  type: mongodb
  connection_string: mongodb+srv://<user>:<pass>@cluster.mongodb.net
  vector_enabled: true
  similarity: cosine  # cosine | euclidean | dotProduct
  vector_index_wait_timeout: 60  # seconds to wait for index to become queryable
  vector_index_wait_poll_interval: 1.0  # seconds between polls
```

### Defining a Feature View with Vector Index

Mark embedding fields with `vector_index=True` and specify `vector_length`:

```python
from feast import Entity, FeatureView, Field, FileSource
from feast.types import Array, Float32, Int64, String
from datetime import timedelta

item_embeddings = FeatureView(
    name="item_embeddings",
    entities=[Entity(name="item_id", join_keys=["item_id"])],
    schema=[
        Field(
            name="embedding",
            dtype=Array(Float32),
            vector_index=True,
            vector_length=384,
            vector_search_metric="cosine",
        ),
        Field(name="title", dtype=String),
        Field(name="item_id", dtype=Int64),
    ],
    source=FileSource(path="items.parquet", timestamp_field="event_timestamp"),
    ttl=timedelta(hours=24),
)
```

When `feast apply` (or `store.update()`) runs with `vector_enabled=True`, Atlas vector search indexes are automatically created for any field with `vector_index=True`. Indexes are also automatically dropped when feature views are removed.

### Retrieving Documents via Vector Search

Use `retrieve_online_documents_v2()` to perform similarity search:

```python
results = FeatureStore.store.retrieve_online_documents_v2(
    config=repo_config,
    table=item_embeddings,
    requested_features=["embedding", "title"],
    embedding=[0.1, 0.2, ...],  # query vector
    top_k=5,
)

# Each result is a (event_timestamp, entity_key_proto, feature_dict) tuple.
# feature_dict includes a synthetic "distance" key with the vector search score.
for ts, entity_key, features in results:
    print(features["title"].string_val, features["distance"].float_val)
```
```

### How It Works

- **Index creation**: `update()` creates an Atlas vector search index named `<feature_view>__<field>__vs_index` for each vector-indexed field. It waits for the index to reach `READY` status before proceeding.
- **Query execution**: `retrieve_online_documents_v2()` builds a `$vectorSearch` aggregation pipeline with `numCandidates = max(top_k * 10, 100)` and the specified `limit`.
- **Score**: Results include a `distance` field populated from `$meta: "vectorSearchScore"`.
- **BSON compatibility**: Query vectors are coerced to native Python floats to avoid numpy serialization issues.
- **Idempotency**: Calling `update()` multiple times will not duplicate indexes.

## Supported Types

MongoDB data sources support all eight primitive types (`bytes`, `string`, `int32`, `int64`, `float32`, `float64`, `bool`, `timestamp`) and their corresponding array types. Complex types such as `Map` and `Struct` are preserved through the MongoDB document model.
For a comparison against other batch data sources, please see [here](overview.md#functionality-matrix).
