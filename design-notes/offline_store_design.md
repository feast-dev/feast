# Corrected MongoDB OfflineStore Design

## What the interface actually requires

`RetrievalJob._to_arrow_internal` must return a `pyarrow.Table`. This is non-negotiable
because the compute engines call `retrieval_job.to_arrow()` directly:

```python
# sdk/python/feast/infra/compute_engines/local/nodes.py
retrieval_job = create_offline_store_retrieval_job(...)
arrow_table = retrieval_job.to_arrow()   # ← hard requirement
```

The compute engine then converts Arrow → proto tuples itself before calling
`OnlineStore.online_write_batch(data: List[Tuple[EntityKeyProto, ...]])`.
The offline store never sees the proto tuple format.

`OfflineStore.offline_write_batch` (the push-source write path) takes a `pyarrow.Table`
— so Arrow is also the *input* format for writes.

## The right approach — native aggregation, then Arrow

The Couchbase offline store is the correct reference. It:
1. Expresses computation natively in the database (SQL++ window functions).
2. Iterates the cursor in Python.
3. Converts directly: `pa.Table.from_pylist(processed_rows)` — **no pandas intermediate**.

MongoDB should follow the same pattern using its aggregation pipeline.

## pull_latest_from_table_or_query

The `$group` + `$sort` aggregation is the natural MongoDB equivalent of
`ROW_NUMBER() OVER(PARTITION BY entity ORDER BY timestamp DESC) = 1`:

```python
pipeline = [
    {"$match": {
        timestamp_field: {"$gte": start_date, "$lte": end_date}
    }},
    {"$sort": {timestamp_field: -1, created_timestamp_column: -1}},
    {"$group": {
        "_id": {k: f"${k}" for k in join_key_columns},
        **{f: {"$first": f"${f}"} for f in feature_name_columns},
        timestamp_field: {"$first": f"${timestamp_field}"},
    }},
]
# cursor → pa.Table.from_pylist([doc for doc in collection.aggregate(pipeline)])
```

No pandas. No Feast join utilities. The database does the work.

## get_historical_features

This is harder. The point-in-time join requires: for each (entity, entity_timestamp) row,
find the feature row with the latest `event_timestamp <= entity_timestamp`.

MongoDB has no SQL window functions, but the aggregation pipeline can express this:

```
For each feature view:
  $match: entity_ids in entity_df AND event_timestamp <= max(entity_timestamps)
  $sort: entity_id, event_timestamp DESC
  $lookup or unwind against entity_df rows
  $match: event_timestamp <= entity_row.entity_timestamp (and TTL if set)
  $group by (entity_id, entity_row_id): $first of features
```

This is complex but keeps computation in MongoDB and avoids loading the full history
into Python memory. The result cursor is then converted via `pa.Table.from_pylist()`.

For an initial implementation it is acceptable to pull the filtered documents into
memory and do the join in Python (like the Dask store) — but this should be noted
as a known limitation, not the target design.

## offline_write_batch

Receives a `pyarrow.Table` from Feast (push-source path). Convert with
`table.to_pylist()` and `insert_many()` into the collection.

## What changes from the previous design

| Previous (incorrect)                        | Corrected                                   |
|---------------------------------------------|---------------------------------------------|
| Pull docs into pandas, use offline_utils    | Use MongoDB aggregation pipeline            |
| pandas is the intermediate format           | MongoDB cursor → `pa.Table.from_pylist()`   |
| Arrow is an afterthought                    | Arrow is the required output of the job     |
| Claimed online_write_batch takes Arrow      | It takes proto tuples; compute engine converts |

## Implementation order (unchanged)

1. `MongoDBSource` — DataSource subclass (connection_string, database, collection, timestamp_field).
2. `MongoDBOfflineStoreConfig` — pydantic config.
3. `MongoDBRetrievalJob` — wraps aggregation pipeline, implements `_to_arrow_internal`.
4. `offline_write_batch` — `pyarrow.Table` → `insert_many`.
5. `pull_latest_from_table_or_query` — `$sort` + `$group` aggregation.
6. `pull_all_from_table_or_query` — `$match` time-range scan.
7. `get_historical_features` — aggregation pipeline PIT join (or in-memory fallback).

