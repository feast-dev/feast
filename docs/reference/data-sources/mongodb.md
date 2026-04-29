# MongoDB source (contrib)

## Description

MongoDB data sources are [MongoDB](https://www.mongodb.com/) collections that can be used as a source for feature data. The `MongoDBSource` points at a MongoDB collection and provides the metadata Feast needs to read historical features from the offline store's `feature_history` collection.

## Examples

Defining a MongoDB source:

```python
from feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb_source import (
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

The full set of configuration options is available [here](https://rtd.feast.dev/en/master/#feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb_source.MongoDBSource).

## Supported Types

MongoDB data sources support all eight primitive types (`bytes`, `string`, `int32`, `int64`, `float32`, `float64`, `bool`, `timestamp`) and their corresponding array types. Complex types such as `Map` and `Struct` are preserved through the MongoDB document model.
For a comparison against other batch data sources, please see [here](overview.md#functionality-matrix).
