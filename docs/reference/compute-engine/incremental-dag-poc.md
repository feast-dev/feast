# Incremental DAG Join Proof of Concept

This proof of concept adds change-aware inner joins to Feast's Local Compute
Engine. It is an experiment for evaluating DAG contracts, not a supported
FeatureView API or a production CDC pipeline.

## What is implemented

The engine-neutral DAG layer defines:

- `ChangeOperation`: INSERT, UPDATE, or DELETE.
- `ChangeRecord`: immutable before/after row data validated for its operation.
- `ChangeSet`: an ordered micro-batch addressed to a named node.
- `KeyedRows`: unique-primary-key state with atomic change application.
- `ExecutionPlan.initialize_incremental()`: seeds persistent state from one
  normal batch execution.
- `ExecutionPlan.execute_incremental()`: validates source changes, routes
  emitted deltas in topological order, and rolls back all node state on error.
- `ExecutionPlan.incremental_result()`: returns the terminal keyed result.

`LocalJoinNode` implements the incremental node contract for two-input inner
equi-joins. Existing `ExecutionPlan.execute()` and Local batch joins are
unchanged.

## Lifecycle

A caller constructs a physical plan, declares one primary-key tuple for every
node, and seeds it:

```python
plan.initialize_incremental(
    context,
    primary_keys={
        "customers": ("customer_id",),
        "orders": ("order_id",),
        "customer_orders": ("order_id",),
    },
)
```

The caller then submits changes to source nodes:

```python
changes = ChangeSet(
    source="customers",
    records=(
        ChangeRecord.update(
            before={"customer_id": 1, "name": "Ada"},
            after={"customer_id": 1, "name": "Grace"},
        ),
    ),
)
output_changes = plan.execute_incremental(context, {"customers": changes})
current_rows = plan.incremental_result(context)
```

Initialization is explicit. Incremental execution before initialization,
changes addressed to a non-source or unknown node, and mismatched
`ChangeSet.source` values fail without modifying persistent state.

## Join algorithm

For each micro-batch, the join:

1. validates changes against cloned input state;
2. collects join keys from every before and after row;
3. snapshots old output rows for those keys;
4. applies the changes to the relevant side or sides;
5. joins only current rows carrying affected keys;
6. replaces only that output partition;
7. diffs old and new rows by the declared output primary key.

A surviving output identity with changed values emits UPDATE. A removed
identity emits DELETE, and a new identity emits INSERT. A key-changing update
can therefore emit a DELETE and INSERT together. The emitted `ChangeSet` is
addressed to the join node, so a downstream incremental join consumes it in the
same way as a source change.

## Chained joins

A plan can route deltas through more than one join:

```text
customers ----+
              customer_orders ----+
orders -------+                    enriched_orders
regions ---------------------------+
```

An update to `customers` changes `customer_orders`; its output delta becomes
the left input delta for `enriched_orders`. An update to `regions` reaches
only the final join. Nodes and keys on unaffected branches are not recomputed.

## Supported boundary

The PoC supports:

- Local Compute Engine physical plans;
- unique declared primary keys;
- two-input inner equi-joins;
- INSERT, UPDATE, and DELETE on either side;
- updates that change primary or join keys;
- multiple changes in one atomic micro-batch;
- downstream propagation through chained joins.

It intentionally excludes outer or temporal joins, duplicate-key bag
semantics, incremental aggregations and arbitrary transformations, durable
state, checkpointing, source offsets, schema evolution, distributed engines,
and sink writes.

## Architecture gaps

Production support requires decisions and implementation for:

1. durable row identity distinct from entity join keys;
2. ordered CDC envelopes, source partitions, and offsets;
3. checkpoint, replay, transaction, and exactly-once boundaries;
4. durable state outside job-local `ExecutionContext`;
5. planner capability metadata and batch/incremental boundaries;
6. duplicate-key multiplicity and retractions;
7. unmatched-row and time-index state for outer and temporal joins;
8. aggregation retractions or targeted recomputation;
9. UDF determinism and declared change semantics;
10. atomic sink upsert/delete APIs coordinated with checkpoints;
11. stable schema and collision rules across engines;
12. state sizing, compaction, metrics, backpressure, and recovery.

## Relationship to tiling

Feast tiling accelerates time-window aggregations by storing cumulative or
per-hop intermediate values and subtracting compatible cumulative tiles.
That technique is useful for invertible aggregate state, but it is not a
general row-change protocol: it does not identify inserted, updated, or deleted
source rows; route retractions through arbitrary DAG nodes; maintain join-side
indexes; or coordinate source offsets and sink commits. A production
incremental aggregation operator could reuse tiling intermediate
representations after Feast defines the broader state and checkpoint contract.

