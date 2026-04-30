# MongoDB offline store (contrib)

## Description

The MongoDB offline store provides support for reading [MongoDBSource](../data-sources/mongodb.md).
* Uses a single shared collection with a compound index for all FeatureViews, distinguished by a `feature_view` discriminator field.
* Entity dataframes can be provided as a Pandas dataframe. The offline store converts entity identifiers into serialized entity keys for efficient lookup against the collection.

## Getting started

In order to use this offline store, you'll need to run `pip install 'feast[mongodb]'`.

## Example

{% code title="feature_store.yaml" %}
```yaml
project: my_project
registry: data/registry.db
provider: local
offline_store:
  type: feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb.MongoDBOfflineStore
  connection_string: "mongodb+srv://user:pass@cluster.mongodb.net"
  database: feast
  collection: feature_history
online_store:
  type: mongodb
  connection_string: "mongodb+srv://user:pass@cluster.mongodb.net"
  database_name: feast_online_store
  collection_suffix: latest
  client_kwargs: {}
```
{% endcode %}

The full set of configuration options is available in [MongoDBOfflineStoreConfig](https://rtd.feast.dev/en/master/#feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb.MongoDBOfflineStoreConfig).

## Data Model

The offline store uses a single shared collection (by default `feature_history`) that stores append-only historical feature rows for all feature views. Each document represents one observation of one entity for one FeatureView at a specific event timestamp:

```json
{
  "entity_id": "Binary(...)",
  "feature_view": "driver_stats",
  "event_timestamp": "ISODate(2024-01-15T12:00:00Z)",
  "created_at": "ISODate(2024-01-15T12:01:00Z)",
  "features": {
    "conv_rate": 0.72,
    "acc_rate": 0.91,
    "avg_daily_trips": 14
  }
}
```

Key properties:

* **Append-only**: Historical data is treated as immutable; corrections are written as new rows with newer `created_at` timestamps rather than in-place updates.
* **Time-series friendly**: `event_timestamp` represents when the feature value was observed; `created_at` is used as a tie-breaker when multiple observations share the same event timestamp.
* **Feature grouping by FeatureView**: `feature_view` identifies which FeatureView the row belongs to, so a single collection can host multiple FVs.

A single compound index supports all major query patterns:

```
(entity_id ASC, feature_view ASC, event_timestamp DESC, created_at DESC)
```

This index enables efficient range scans over entities and feature views, while ensuring that the most recent observation per `(entity_id, feature_view)` is seen first during aggregation. The index is created lazily on first use and cached per connection string.

## Key Optimizations

* **K-collapse**: Multiple FeatureViews that share the same join keys are queried in a single aggregation using `feature_view: {$in: [...]}`, reducing round trips.
* **Scoring vs. training paths**: When each entity appears only once in `entity_df` (scoring/inference — one feature lookup per entity), server-side `$group $first` efficiently returns the single latest value per entity. When the same entity appears at multiple timestamps (training — building a dataset with many historical snapshots per entity), the store retrieves all candidate rows and uses `pd.merge_asof` to select the correct point-in-time value for each request timestamp.
* **Two-level chunking**: `CHUNK_SIZE` (50,000 rows) controls the size of intermediate DataFrames in memory; `MONGO_BATCH_SIZE` (10,000 entity IDs) limits the query size sent to MongoDB.

## Functionality Matrix

The set of functionality supported by offline stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the MongoDB offline store.

|                                                                    | MongoDB |
| :----------------------------------------------------------------- | :------ |
| `get_historical_features` (point-in-time correct join)             | yes     |
| `pull_latest_from_table_or_query` (retrieve latest feature values) | yes     |
| `pull_all_from_table_or_query` (retrieve a saved dataset)          | yes     |
| `offline_write_batch` (persist dataframes to offline store)        | yes     |
| `write_logged_features` (persist logged features to offline store) | no      |

Below is a matrix indicating which functionality is supported by `MongoDBRetrievalJob`.

|                                                       | MongoDB |
| ----------------------------------------------------- | ------- |
| export to dataframe                                   | yes     |
| export to arrow table                                 | yes     |
| export to arrow batches                               | no      |
| export to SQL                                         | no      |
| export to data lake (S3, GCS, etc.)                   | no      |
| export to data warehouse                              | no      |
| export as Spark dataframe                             | no      |
| local execution of Python-based on-demand transforms  | yes     |
| remote execution of Python-based on-demand transforms | no      |
| persist results in the offline store                  | yes     |
| preview the query plan before execution               | no      |
| read partitioned data                                 | no      |

To compare this set of functionality against other offline stores, please see the full [functionality matrix](overview.md#functionality-matrix).
