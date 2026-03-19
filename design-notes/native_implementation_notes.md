# Native MongoDB Offline Store Implementation Review

## Overview

This document reviews the native MongoDB offline store implementation (`mongodb_native.py`) in the context of Feast idioms, the MongoDB online store implementation, and best practices.

---

## Schema Alignment: Online ↔ Offline

### Online Store Schema (mongodb_online_store/mongodb.py)
```javascript
{
  "_id": bytes,  // serialized entity key
  "features": {
    "<feature_view_name>": {
      "<feature_name>": value
    }
  },
  "event_timestamps": { "<feature_view>": datetime },
  "created_timestamp": datetime
}
```

### Offline Store Schema (Native)
```javascript
{
  "_id": ObjectId(),
  "entity_id": bytes,  // serialized entity key (same format as online _id)
  "feature_view": "driver_stats",  // discriminator
  "features": { "<feature_name>": value },
  "event_timestamp": datetime,
  "created_at": datetime
}
```

### ✅ Alignment Strengths
1. **Entity key serialization**: Both use `serialize_entity_key()` from `key_encoding_utils.py`
2. **Nested features**: Both use `features: { ... }` subdocument pattern
3. **Timestamps**: Both track event and created timestamps

### ⚠️ Alignment Concerns
1. **`_id` usage**: Online uses `_id` = entity_id; Offline uses `_id` = ObjectId() with separate `entity_id` field
   - **Recommendation**: Consider using `_id` = `{entity_id, feature_view, event_timestamp}` compound key for offline, eliminating ObjectId overhead
   
2. **Feature nesting depth**: Online nests by feature_view then feature; Offline nests only by feature (feature_view is top-level)
   - This is intentional (offline is one doc per event; online is one doc per entity with all FVs)

---

## Feast Idioms Compliance

### ✅ Correctly Followed
1. **RetrievalJob pattern**: Returns `MongoDBNativeRetrievalJob` wrapping a `query_fn` closure
2. **Arrow output**: `_to_arrow_internal()` returns `pyarrow.Table` (hard requirement)
3. **Warnings for preview**: Uses `warnings.warn()` with `RuntimeWarning`
4. **Config inheritance**: `MongoDBOfflineStoreNativeConfig` extends `FeastConfigBaseModel`
5. **DataSource pattern**: `MongoDBSourceNative` extends `DataSource` with `from_proto`/`_to_proto_impl`

### ⚠️ Missing or Incomplete
1. **`offline_write_batch`**: Not implemented (raises `NotImplementedError` in persist)
   - Required for push sources and `feast materialize` reverse path
   - Should accept `pyarrow.Table` and insert into `feature_history` collection

2. **`write_logged_features`**: Not implemented
   - Lower priority but needed for feature logging

3. **`persist()` on RetrievalJob**: Not implemented
   - Should write results to a new collection for saved datasets

---

## MQL Pipeline Quality

### ✅ Well Implemented
1. **`pull_all_from_table_or_query`**: Clean range scan with `$project` flattening features server-side
2. **`pull_latest_from_table_or_query`**: Proper `$sort` → `$group` → `$project` pattern
3. **`get_historical_features`**: Uses `$lookup` with correlated subpipeline for server-side PIT join
4. **Per-FV TTL via `$switch`**: Elegant solution for different TTLs per feature view

### ⚠️ Potential Improvements
1. **Index usage in `$lookup`**: The `$expr` in `$match` may not use indexes efficiently
   - MongoDB 5.0+ has better support for `$expr` index usage
   - Consider adding `hint` option if performance is critical

2. **Temp collection cleanup**: Currently uses `try/finally` but could benefit from context manager pattern

3. **Connection pooling**: Each method creates a new `MongoClient`. The online store caches `_client` and `_collection`
   - **Recommendation**: Add `_client` caching to the offline store class or use connection pooling

---

## Comparison with Online Store Patterns

| Aspect | Online Store | Offline Store (Native) |
|--------|--------------|------------------------|
| Client caching | `_client`, `_collection` instance vars | New client per operation |
| Async support | Yes (`AsyncMongoClient`) | No |
| Batch operations | `bulk_write` with `UpdateOne` | `insert_many` |
| Error handling | Raises `RuntimeError` for config mismatch | Raises `ValueError` |
| DriverInfo | ✅ Yes | ✅ Yes |

### Recommendations
1. **Add client caching** to avoid connection overhead per query
2. **Consider async support** for large entity_df scenarios
3. **Standardize error types** (use `RuntimeError` or `FeastError` subclasses)

---

## Missing Features for Production Readiness

### High Priority
1. **`offline_write_batch`**: Insert Arrow table into feature_history
   ```python
   @staticmethod
   def offline_write_batch(
       config: RepoConfig,
       feature_view: FeatureView,
       table: pyarrow.Table,
       progress: Optional[Callable[[int], Any]],
   ):
       # Convert Arrow → docs with schema:
       # { entity_id, feature_view, features: {...}, event_timestamp, created_at }
       # Then insert_many()
   ```

2. **Index creation helper**: Document or auto-create the compound index
   ```javascript
   db.feature_history.createIndex({
     entity_id: 1,
     feature_view: 1,
     event_timestamp: -1
   })
   ```

3. **Connection pooling / client reuse**

### Medium Priority
4. **`persist()` for saved datasets**: Write retrieval results to a collection
5. **`write_logged_features`**: For feature logging support
6. **Async operations**: Mirror online store's async pattern

### Lower Priority
7. **Streaming cursor support**: For very large result sets
8. **Explain plan logging**: Debug mode to show MQL execution plan

---

## Code Quality Observations

### ✅ Good
- Clear docstrings explaining schema and index requirements
- Type hints throughout
- Helper functions extracted (`_ttl_to_ms`, `_build_ttl_gte_expr`, `_serialize_entity_key_from_row`)
- Proper cleanup of temp collections in `finally` block

### ⚠️ Could Improve
- Some duplication in timestamp timezone handling (could extract helper)
- Magic strings like `"event_timestamp"`, `"created_at"` could be constants
- The `_run()` closures are large — consider extracting to separate methods

---

## Test Coverage Assessment

Current tests cover:
- ✅ `pull_latest_from_table_or_query`
- ✅ `pull_all_from_table_or_query`
- ✅ `get_historical_features` (PIT join)
- ✅ TTL filtering
- ✅ Multiple feature views
- ✅ Compound join keys

Missing tests:
- ❌ `offline_write_batch` (not implemented)
- ❌ Empty result handling edge cases
- ❌ Very large entity_df (performance/memory)
- ❌ Concurrent access to temp collections
- ❌ Index usage verification (explain plans)

---

## Summary

The native implementation is a solid foundation with proper use of MQL aggregation pipelines. Key next steps:

1. **Implement `offline_write_batch`** — Required for push sources
2. **Add client caching** — Match online store pattern
3. **Document/automate index creation** — Critical for performance
4. **Consider `_id` schema optimization** — Use compound `_id` instead of ObjectId + entity_id

