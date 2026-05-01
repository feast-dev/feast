# MongoDB Offline Store

This offline store lets you train models and run batch scoring directly from it.
All feature views share a single collection (`feature_history`). Reads use
MongoDB aggregation pipelines with a compound index, so per-entity cost is
O(log n_observations) regardless of collection size, and K feature views with the same
entity key collapse into one round-trip instead of K (1 if your data shares a unique id.)

## Schema

All feature views share one collection (default: `feature_history`), discriminated by the `feature_view` field.

```javascript
// Collection: feature_history
{
  "entity_id":       Binary("..."),           // Serialized entity key (bytes)
  "feature_view":    "driver_stats",          // Discriminator
  "features": {                               // Nested subdocument
    "trips_today":   5,
    "rating":        4.8
  },
  "event_timestamp": ISODate("2024-01-15T10:00:00Z"),
  "created_at":      ISODate("2024-01-15T10:00:01Z")
}
```
## Index

The store creates one compound index lazily on first use. This index supports every query issued..

```javascript
db.feature_history.createIndex({
  "entity_id":       1,
  "feature_view":    1,
  "event_timestamp": -1,
  "created_at":      -1
})

```
## Configuration

```yaml
offline_store:
  type: feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb.MongoDBOfflineStore
  connection_string: mongodb://localhost:27017
  database: feast
  collection: feature_history  # optional, default: feature_history
```

## Key Features

**Query-collapse** — Feature views that share the same join key set are grouped into a single MongoDB aggregation round-trip instead of one per feature view. Reduces round-trips from K to the number of unique join key signatures, often one.

**Scoring path** — When `entity_df` contains unique entity IDs, a `$match + $sort + $group` pipeline performs server-side deduplication returning at most one document per `(entity_id, feature_view)`. The compound index makes per-entity cost O(log n_obs).

**Training path** — When `entity_df` contains repeated entity IDs at different timestamps, the `$group` stage is omitted and `pandas.merge_asof` performs per-row point-in-time joins optimized in C.

**`strict_pit`** — `get_historical_features` accepts a `strict_pit` keyword argument (default `True`). With `strict_pit=True` (default, safe for training), documents whose timestamp is strictly after the entity request timestamp are returned as `NULL`. Set `strict_pit=False` for real-time inference where you always want the most recent observation.


## Writing Data

Use `offline_write_batch` (called automatically by `feast materialize`) to write feature observations:

```python
store.write_to_offline_store(feature_view_name, df)
```

Documents are appended; `pull_latest` and the scoring path select the highest `created_at` at read time.

## Memory Behaviour

The store filters by entity key in `$match` rather than loading the entire collection. Memory usage is bounded by the number of unique entity IDs × documents per entity, not the total collection size.
