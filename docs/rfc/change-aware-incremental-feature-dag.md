# RFC: Change-aware Incremental Feature DAGs

## Summary

This RFC proposes an opt-in incremental execution model for Feast compute DAGs.
Sources deliver keyed changes, stateful operators emit downstream changes, and
checkpoint-aware sinks commit results with source progress. The Local
change-aware inner-join proof of concept validates row-change and propagation
semantics but does not define a stable public API.

## Motivation

Current compute plans process snapshots. Every node receives complete upstream
data and produces a complete output, even when only a few source keys changed.
Tiling optimizes a subset of windowed aggregation work, but Feast has no common
contract for row retractions, operator state, downstream deltas, or recovery.

A shared model would let engines avoid full recomputation for materialization
and transformation DAGs while keeping results equivalent to a batch run.

## Current Feast Constraints

`FeatureResolver` creates a logical FeatureView graph, `FeatureBuilder`
creates engine-specific nodes, and `ExecutionPlan.execute()` walks a
topologically sorted physical plan. `ExecutionContext.node_outputs` is a
per-run cache. It is neither durable state nor a checkpoint.

Sources expose range/snapshot reads rather than ordered CDC records. Operators
do not declare incremental capabilities. Online and offline stores do not share
an atomic upsert/delete-plus-checkpoint contract.

## Proposed Concepts

Introduce four engine-neutral concepts:

1. **Row identity**: an explicit key for each operator output.
2. **Change envelope**: INSERT, UPDATE(before, after), and DELETE records plus
   source partition, offset, schema version, and event metadata.
3. **Stateful operator contract**: initialize from a snapshot, apply a
   micro-batch, emit changes, snapshot/restore state, and report capabilities.
4. **Checkpoint coordinator**: atomically binds source progress, operator state,
   and sink commits.

The PoC implements only in-memory row identity, `ChangeOperation`,
`ChangeSet`, `KeyedRows`, incremental plan routing, and Local inner joins.
Offset metadata, durable checkpoints, and sink coordination remain proposals.

## Row Identity and Change Envelope

FeatureView entity keys are join keys, not always unique row identities.
Production APIs should require or derive an operator primary key independently
from join keys. A planner must reject a graph when an operator cannot prove the
identity of its output.

The portable change envelope should include:

- operation and before/after payloads;
- source/node identifier;
- primary-key values;
- partition and monotonically comparable offset;
- event and ingestion time;
- schema fingerprint or version;
- transaction/batch identifier.

Duplicate keys require bag or multiplicity semantics rather than the PoC's
unique-key map. The chosen model must specify whether two identical rows are
distinct and how a single deletion identifies its target.

## Stateful Operator Contract

An incremental node should declare supported operations, required indexes,
join/aggregation restrictions, state schema, and whether it can consume updates
directly or needs normalized delete-plus-insert records.

A conceptual contract is:

```python
initialize(snapshot_inputs, checkpoint) -> OperatorState
apply(input_changes, state) -> (output_changes, new_state)
snapshot(state, checkpoint_id) -> StateHandle
restore(state_handle) -> OperatorState
```

A micro-batch is atomic at the plan level. Operators stage state and output;
the coordinator commits only after every downstream operator and sink succeeds.
Retries use batch identifiers and offsets to remain idempotent.

## Planning and Capability Negotiation

`FeatureBuilder` should annotate the logical and physical DAG with row
identity, changelog mode, and operator capabilities. The planner can then:

- select a fully incremental path;
- insert normalization or materialization boundaries;
- fall back to bounded key recomputation;
- reject a graph whose semantics cannot be preserved.

Fallback must be explicit and observable. Silent conversion to full snapshot
execution makes cost and latency unpredictable.

## Checkpoint and Sink Semantics

Production state cannot live only in `ExecutionContext`. Each compute engine
needs a durable state backend and checkpoint integration. A completed
checkpoint must cover source offsets, every stateful operator, and all output
sinks.

Online and offline sinks need idempotent upsert and delete operations. Where a
sink cannot join the checkpoint transaction, Feast needs a documented
at-least-once protocol plus deduplication keys. Exactly-once should be claimed
only for engine/source/sink combinations that provide the required atomicity.

## Relationship to Tiling

Tiling already persists intermediate representations for time-window
aggregations. Sum and count tiles can be subtracted; average, variance, and
standard deviation can be reconstructed from invertible intermediate state.
Max, min, distinct count, and holistic functions need retained hops,
specialized indexes, or targeted recomputation.

Tiling does not currently consume a general `ChangeSet`, retract arbitrary
source rows, propagate deltas through joins, or checkpoint with source offsets
and sinks. It should become one implementation technique behind an incremental
aggregation operator, not the general change contract itself.

## Cross-engine Conformance

The change envelope and observable results should be portable even when state
layouts are engine-specific. A conformance suite should replay the same
snapshot and change stream through Local, Spark, Flink, Ray, and SQL-capable
implementations and compare each checkpoint with batch recomputation.

Required cases include changes on every join side, key changes, duplicates,
null keys, late and reordered events, schema transitions, replay after
failure, and sink retry behavior.

## Rollout Plan

1. Stabilize row identity, change envelopes, capability declarations, and a
   batch-equivalence conformance suite.
2. Add checkpointed Local execution and idempotent Local sink upsert/delete
   contracts.
3. Add projections, filters, deterministic transformations, and invertible
   aggregations, reusing tiling representations where appropriate.
4. Integrate one durable streaming engine and validate recovery semantics.
5. Extend duplicate-key, outer-join, temporal, and late-data behavior.
6. Graduate APIs only after cross-engine compatibility evidence exists.

## Alternatives Considered

**Signed row multiplicities.** Differential-dataflow-style integer weights give
elegant compositional join and aggregation rules. They also impose bag
semantics and consolidation machinery unfamiliar to current Feast APIs. They
remain a strong future internal representation.

**Dirty-key snapshot recomputation.** Tracking changed keys and rerunning
existing operators is easier to adopt, but it does not define downstream
retractions or durable state. It can serve as an explicit planner fallback.

**Engine-native APIs only.** Letting every engine invent its own changelog
contract reduces initial work but prevents portable planning and conformance.

## Security and Operational Considerations

Before/after payloads may contain sensitive feature values. Checkpoints, logs,
dead-letter queues, and metrics must follow Feast's access controls and avoid
recording raw rows by default. State backends need encryption, retention, and
tenant isolation.

Operators must expose state size, processed offsets, checkpoint age, replay
count, rejected records, and batch latency. Backpressure and state growth need
configured limits; a malformed or adversarial key distribution must not cause
unbounded memory use.

## Open Questions

- Where is row identity declared: DataSource, FeatureView, transformation, or
  physical plan?
- Are null join keys rejected consistently or represented as non-matching?
- Which duplicate-key/multiplicity model should Feast standardize?
- How are late and out-of-order changes reconciled with point-in-time joins?
- What schema changes are compatible with existing operator state?
- Which source/sink combinations can truthfully provide exactly-once results?
- Should UPDATE remain first-class or always normalize to DELETE plus INSERT?
- How should Python UDFs declare determinism and retraction behavior?
- When should the planner prefer targeted recomputation over maintained state?
