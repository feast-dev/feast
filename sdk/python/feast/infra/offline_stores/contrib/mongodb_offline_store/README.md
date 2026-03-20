# MongoDB Offline Store

Two MongoDB offline store implementations optimized for different use cases.

## Overview

| Aspect | `MongoDBOfflineStoreMany` | `MongoDBOfflineStoreOne` |
|--------|---------------------------|--------------------------|
| Collections | One per FeatureView | Single shared collection |
| Schema | Flat documents | Nested `features` subdoc |
| Entity ID | Separate columns | Serialized bytes |
| Best for | Small-medium feature stores | Large feature stores |

## MongoDBOfflineStoreMany (mongodb_many.py)

**One collection per FeatureView** — each FeatureView maps to its own MongoDB collection.

### Schema

```javascript
// Collection: driver_stats
{
  "driver_id": 1001,
  "event_timestamp": ISODate("2024-01-15T10:00:00Z"),
  "created_at": ISODate("2024-01-15T10:00:01Z"),  // Optional tie-breaker
  "trips_today": 5,
  "rating": 4.8
}
```

Ties (same `event_timestamp`) are broken by `created_timestamp_column` if configured.

### Configuration

```yaml
offline_store:
  type: feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb_many.MongoDBOfflineStoreMany
  connection_string: mongodb://localhost:27017
  database: feast
```

### When to Use

✅ **Small to medium feature stores** — loads entire collection into memory  
✅ **Fast PIT joins** — Ibis memtables are highly optimized  
✅ **Simple schema** — flat documents, easy to query directly  
✅ **Per-collection indexes** — each FV can have tailored indexes  

⚠️ **Caution**: Loads ALL documents from each collection. May OOM on very large collections.

## MongoDBOfflineStoreOne (mongodb_one.py)

**Single shared collection** — all FeatureViews store data in one collection with a discriminator field.

### Schema

```javascript
// Collection: feature_history (shared by all FVs)
{
  "entity_id": Binary("..."),           // Serialized entity key
  "feature_view": "driver_stats",       // Discriminator
  "features": {                         // Nested subdocument
    "trips_today": 5,
    "rating": 4.8
  },
  "event_timestamp": ISODate("2024-01-15T10:00:00Z"),
  "created_at": ISODate("2024-01-15T10:00:01Z")
}
```

### Configuration

```yaml
offline_store:
  type: feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb_one.MongoDBOfflineStoreOne
  connection_string: mongodb://localhost:27017
  database: feast
  collection: feature_history
```

### When to Use

✅ **Large feature stores** — filters by entity_id, doesn't load entire collection  
✅ **Memory-safe** — processes in chunks, bounded memory usage  
✅ **Schema consistency** — matches online store pattern  
✅ **Efficient materialization** — MQL aggregation pipeline  

⚠️ **Trade-off**: Slightly slower than Many for small workloads due to serialization overhead.

## Performance Comparison

Benchmarks with 10 features, 3 historical rows per entity:

| Entity Rows | Many (time) | One (time) | Winner |
|-------------|-------------|------------|--------|
| 1,000 | 0.30s | 0.06s | One |
| 10,000 | 0.20s | 0.31s | Many |
| 100,000 | 1.51s | 5.22s | Many |
| 1,000,000 | 16.08s | 212s | Many |

### Memory Behavior

| Scenario | Many | One |
|----------|------|-----|
| Large feature collection, small entity_df | ❌ Loads all | ✅ Filters |
| Small feature collection, large entity_df | ✅ Fast | ⚠️ Slower |

## Choosing an Implementation

```
                    ┌─────────────────────────────┐
                    │ Is your feature collection  │
                    │ larger than available RAM?  │
                    └─────────────────────────────┘
                               │
                    ┌──────────┴──────────┐
                    ▼                     ▼
                   YES                    NO
                    │                     │
                    ▼                     ▼
            ┌───────────────┐     ┌───────────────┐
            │ Use ONE       │     │ Use MANY      │
            │ (memory-safe) │     │ (faster)      │
            └───────────────┘     └───────────────┘
```

## Index Recommendations

### Many (per-collection)

```javascript
db.driver_stats.createIndex({ "driver_id": 1, "event_timestamp": -1 })
```

### One (shared collection)

```javascript
db.feature_history.createIndex({
  "entity_id": 1,
  "feature_view": 1, 
  "event_timestamp": -1
})
```

The One implementation creates this index automatically on first use.

