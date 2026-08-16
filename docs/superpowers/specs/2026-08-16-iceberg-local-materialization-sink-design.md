# Local Iceberg Materialization Sink Design

## Summary

Extend Feast's existing `sink_source` model so the local compute engine can
persist derived FeatureView materialization results to Apache Iceberg. The
first phase uses PyIceberg to create a missing table and idempotently upsert a
PyArrow result into an existing table.

This design deliberately reuses `sink_source=IcebergSource(...)`. It does not
add another FeatureView field, make Iceberg the repository's global offline
store, or add Spark writes. A follow-up can implement the same behavior in the
Spark compute engine with Iceberg `MERGE INTO`.

## Goals

- Support `IcebergSource` as the `sink_source` of a derived FeatureView during
  local materialization.
- Preserve Feast materialization idempotency by upserting on entity join keys
  plus the event timestamp.
- Support the catalog configurations already represented by `IcebergSource`,
  including REST, Glue, Hive, SQL, and DynamoDB catalogs.
- Create the destination Iceberg table when it does not exist.
- Keep catalog, authentication, table, schema, and write behavior isolated
  behind the Iceberg source implementation.
- Produce actionable errors for invalid configuration, incompatible schemas,
  missing keys, duplicate input keys, authentication failures, and commit
  failures.

## Non-goals

- Spark, Ray, or other compute-engine writes.
- A generic writable-data-source interface.
- Automatic Iceberg namespace creation.
- Automatic schema, partition-spec, or sort-order evolution.
- Cross-store transactional atomicity among Iceberg, online stores, and the
  configured offline store.
- Writing a base FeatureView to an additional arbitrary sink. In this phase,
  the existing `sink_source` semantics remain limited to derived views.
- Snapshot expiration, compaction, branching, tagging, or other Iceberg table
  maintenance.

## User API

The public API is the existing derived-view API:

```python
from feast import BatchFeatureView
from feast.infra.data_sources.contrib.iceberg_catalog import IcebergSource

driver_stats_iceberg = IcebergSource(
    catalog_type="rest",
    endpoint="https://catalog.example.com/iceberg",
    warehouse="production",
    namespace="features",
    table="daily_driver_stats",
    token_env_var="ICEBERG_TOKEN",
    timestamp_field="event_timestamp",
)

daily_driver_stats = BatchFeatureView(
    name="daily_driver_stats",
    source=hourly_driver_stats,
    sink_source=driver_stats_iceberg,
    entities=[driver],
    schema=[...],
    udf=build_daily_features,
    online=True,
)
```

The derived view continues to serialize its sink as its effective batch source,
so no FeatureView or DataSource protobuf change is required.

## Architecture

### Iceberg writer boundary

`IcebergSource` will own a focused materialization method that accepts:

- the PyArrow table produced by the local DAG;
- resolved upsert key column names;
- optional snapshot metadata such as Feast project and FeatureView name.

The method is responsible for constructing a PyIceberg catalog, resolving the
table identifier, creating or loading the table, validating the write, and
performing the upsert. Keeping this behavior with `IcebergSource` prevents
catalog-specific details from leaking into compute-engine nodes.

The current lightweight Feast REST client remains unchanged for reads and
governance operations. Sink writes construct a PyIceberg catalog for every
supported catalog type, including REST. Catalog construction reuses
`catalog_name`, `endpoint`, `warehouse`, `catalog_properties`, and the token
resolved from `token_env_var`.

### Local output integration

`LocalFeatureBuilder` will pass its resolved `ColumnInfo` to `LocalOutputNode`.
This avoids reconstructing field mappings in the output node and guarantees
that upsert keys refer to the post-mapping Arrow column names.

`LocalOutputNode.execute()` retains its existing empty-table short circuit and
online/offline writes. For a derived view whose effective batch source is an
`IcebergSource`, it additionally invokes the Iceberg materialization method.

The Iceberg write is part of the materialization job. An exception propagates
through the existing local compute engine and results in an errored
materialization job.

### Dependency

The `iceberg` optional dependency changes from `pyiceberg>=0.7.0` to
`pyiceberg>=0.10.0`. PyIceberg 0.10 introduced the native Arrow-based
`Table.upsert()` API required for the idempotency guarantee.

Users without the `iceberg` extra remain unaffected. Attempting to use an
Iceberg sink without PyIceberg installed raises an installation-oriented error
that names `feast[iceberg]`.

## Data Flow

1. The local compute engine resolves and executes the derived FeatureView DAG.
2. The final node produces a PyArrow table with source field mappings already
   applied.
3. `LocalOutputNode` performs the configured online and offline store writes.
4. If the derived view has an Iceberg sink, the node resolves upsert keys from
   `ColumnInfo`: all mapped entity join keys followed by the mapped event
   timestamp column.
5. `IcebergSource` constructs a PyIceberg catalog and resolves
   `<namespace>.<table>`.
6. If the table does not exist, `IcebergSource` creates it from the Arrow
   schema. The namespace must already exist.
7. The source validates schema compatibility and key presence.
8. `Table.upsert(arrow_table, join_cols=keys)` atomically commits the Iceberg
   snapshot.
9. The node returns its existing `ArrowTableValue` output.

## Upsert Semantics

The identity of a materialized record is:

```text
mapped entity join keys + mapped event timestamp
```

Including the event timestamp preserves multiple historical values for an
entity while making a repeated materialization of the same interval
idempotent. The created-timestamp column is not part of the key; a later write
for the same entity/event timestamp replaces the prior row.

Entityless views use the event timestamp as their key. If no usable event
timestamp column is present, materialization fails before any Iceberg commit.

The input batch must contain at most one row for each composite key. Duplicate
keys are rejected rather than relying on unspecified merge ordering.

## Table Creation and Schema Rules

When the destination table is absent:

- the containing namespace must already exist;
- the table is created from the materialized Arrow schema;
- Feast passes the explicit join columns to each upsert, so the initial phase
  does not require changing Iceberg identifier-field metadata;
- table properties from `IcebergSource.catalog_properties` remain catalog
  configuration and are not silently copied into table properties.

When the table exists, the materialized columns and types must be compatible
with its schema. The first phase does not add, drop, rename, or widen columns.
The error identifies missing, unexpected, and incompatible columns.

## Failure Behavior

- Missing `pyiceberg`: raise an error instructing the user to install
  `feast[iceberg]`.
- Missing namespace: fail without creating governance boundaries implicitly.
- Authentication or credential-vending failure: preserve the underlying cause
  and identify the catalog endpoint and target table without exposing secrets.
- Missing upsert columns or duplicate input keys: fail before catalog mutation.
- Incompatible existing schema: fail before the upsert.
- Concurrent Iceberg commit conflict: surface the PyIceberg commit error; the
  first phase does not implement implicit retries.
- Empty Arrow table: perform no catalog call or write.

Feast cannot atomically commit Iceberg together with its online and offline
stores. A failure after another store has accepted data is reported as a failed
materialization job, and rerunning is safe for the Iceberg sink because the
write is an upsert.

## Testing

### Unit tests

- Catalog configuration for REST, Glue, Hive, SQL, and DynamoDB sources.
- Helpful missing-dependency error.
- Table creation with the materialized Arrow schema.
- Existing-table load and `upsert()` invocation.
- Composite join keys and entityless event-timestamp keys.
- Field-mapped entity and timestamp columns.
- Empty input as a no-op.
- Missing key, duplicate key, namespace, authentication, schema, and commit
  failures.
- Verification that secrets resolved from environment variables do not appear
  in serialized source configuration or error messages.
- Existing IcebergSource protobuf round-trip when used as a derived view sink.

### Component test

Use a temporary filesystem warehouse and PyIceberg SQL catalog with the local
compute engine:

1. Materialize a derived FeatureView into a new Iceberg table.
2. Verify the expected rows and schema.
3. Materialize the same interval again.
4. Verify the row count is unchanged.
5. Change a non-key feature value, materialize again, and verify the row is
   updated rather than duplicated.

### Regression tests

- Local online-only and offline-only materialization without an Iceberg sink is
  unchanged.
- A derived view with a non-Iceberg sink retains its current behavior.

## Documentation

Extend the Iceberg data-source documentation with:

- a derived FeatureView `sink_source` example;
- the `feast[iceberg]` installation requirement;
- supported catalog types;
- composite upsert-key behavior;
- strict schema behavior;
- the phase-one local-engine limitation;
- a note that Spark support is planned separately.

## Follow-up: Spark

A separate PR will reuse the same `sink_source=IcebergSource(...)` contract in
`SparkWriteNode`. It will configure the Spark Iceberg catalog, create a missing
table, register the materialized DataFrame as a temporary view, and execute a
distributed Iceberg `MERGE INTO` on the same entity-plus-event-timestamp key.
It will not collect Spark data into the driver or route writes through
PyIceberg.
