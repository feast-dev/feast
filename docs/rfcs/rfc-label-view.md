RFC: LabelView — First-Class Mutable Labels for Feast

Status: In Review
Authors: Nikhil Kathole
Pull Request: https://github.com/feast-dev/feast/pull/6292
Date: 2026-04-16

---

# Summary

This RFC proposes adding LabelView as a new first-class primitive to Feast, sitting alongside FeatureView, StreamFeatureView, and OnDemandFeatureView. A LabelView manages mutable labels and annotations — reward signals, safety scores, human judgments — that are kept separate from the immutable feature data in regular feature views. Labels are ingested in real time via `FeatureStore.push()` through a `PushSource`, support multi-labeler workflows with configurable conflict resolution policies, and integrate seamlessly with existing Feast APIs including `FeatureService`, `get_historical_features()`, `get_online_features()`, versioning, and the permission system.

---

# Motivation

Today, Feast treats all data as immutable, append-only feature data. This works well for observational signals (driver trip counts, page views, embedding vectors), but creates problems for a growing class of use cases where data is a mutable judgment about an entity rather than an observation of it:

1. **Labels are not features.** Reward labels, safety scores, and human annotations are mutable judgments, not immutable observations. Mixing them into regular FeatureViews conflates two fundamentally different data lifecycle patterns — append-only vs. overwrite — leading to confusing semantics and fragile pipelines.

2. **Multiple labelers disagree.** In RLHF, safety monitoring, and multi-annotator workflows, different sources (human reviewers, automated scanners, reward models) independently write labels for the same entity keys. Feast has no mechanism to track which labeler wrote what, or to resolve conflicts when labelers disagree.

3. **Safety systems need a feedback loop.** When an AI safety layer (e.g., NeMo Guardrails) blocks a harmful interaction, it needs to write negative reward data back into the feature store as a feedback signal for retraining. This requires a push-based, real-time write path for mutable data — something regular FeatureViews were not designed for.

4. **Training datasets need features + labels joined.** ML training pipelines need to retrieve features and their associated labels together with point-in-time correctness. Without a first-class label primitive, teams resort to ad-hoc joins outside Feast, losing reproducibility and governance.

---

# Design

## Core Concepts

| Concept | Description |
|---|---|
| **LabelView** | A new first-class Feast primitive (subclass of `BaseFeatureView`) that manages mutable labels keyed by entities. Stored in its own registry table/proto section. |
| **ConflictPolicy** | An enum (`LAST_WRITE_WINS`, `LABELER_PRIORITY`, `MAJORITY_VOTE`) controlling how conflicting labels from different labelers are resolved. Stored as metadata; enforcement is future work. |
| **labeler_field** | A designated schema field (default: `"labeler"`) that identifies which source wrote each label. Enables multi-labeler provenance tracking. |
| **retain_history** | A boolean flag indicating whether full write history should be kept per entity key. Stored as metadata; enforcement is future work. |
| **reference_feature_view** | Optional link to the `FeatureView` whose entities this label view annotates, for documentation and lineage. |
| **PushSource integration** | Labels are ingested via `FeatureStore.push()` through a `PushSource`, writing to both online and offline stores in real time. |

## Separation of Concerns: Features vs. Labels

| Dimension | FeatureView | LabelView |
|---|---|---|
| Data nature | Observational, immutable | Judgments, mutable |
| Write pattern | Batch or stream append | Real-time push (overwrite per key) |
| Writers | Single source of truth | Multiple labelers |
| Materialization | `feast materialize` / incremental | `FeatureStore.push()` only |
| Conflict handling | N/A (single writer) | `ConflictPolicy` (LAST_WRITE_WINS, etc.) |
| Labeler tracking | N/A | `labeler_field` identifies source |

## What Triggers a New Version

LabelViews inherit full versioning support from `BaseFeatureView` via the feature view versioning system (RFC-44). Schema changes to a LabelView trigger automatic version snapshots. Only schema-significant changes create new versions — metadata-only changes (description, tags, owner, TTL) update the active definition in place.

## Class Hierarchy

```
BaseFeatureView (abstract)
  ├── FeatureView
  │     ├── BatchFeatureView
  │     └── StreamFeatureView
  ├── OnDemandFeatureView
  └── LabelView          ← new
```

LabelView inherits from `BaseFeatureView`, gaining the standard name, features, projection, `proto_class`, versioning (`version`, `current_version_number`), and schema infrastructure. It adds label-specific fields: `labeler_field`, `conflict_policy`, `retain_history`, and `reference_feature_view`.

## Protobuf Schema

```protobuf
// feast/core/LabelView.proto

message LabelView {
    LabelViewSpec spec = 1;
    LabelViewMeta meta = 2;
}

enum ConflictResolutionPolicy {
    LAST_WRITE_WINS  = 0;
    LABELER_PRIORITY = 1;
    MAJORITY_VOTE    = 2;
}

message LabelViewSpec {
    string name                              = 1;
    string project                           = 2;
    repeated string entities                 = 3;
    repeated FeatureSpecV2 features          = 4;
    map<string, string> tags                 = 5;
    google.protobuf.Duration ttl             = 6;
    DataSource source                        = 7;
    bool online                              = 8;
    string description                       = 9;
    string owner                             = 10;
    repeated FeatureSpecV2 entity_columns    = 11;
    string labeler_field                     = 12;
    ConflictResolutionPolicy conflict_policy = 13;
    bool retain_history                      = 14;
    string reference_feature_view            = 15;
}

message LabelViewMeta {
    google.protobuf.Timestamp created_timestamp      = 1;
    google.protobuf.Timestamp last_updated_timestamp  = 2;
}
```

## Ingestion Path: FeatureStore.push()

Labels are written via the existing `FeatureStore.push()` API, which routes data to any FeatureView or LabelView whose `PushSource` matches the given name. The push path writes to both the online and offline stores by default (`PushMode.ONLINE`), making labels immediately available for serving and later available for training dataset generation.

```python
import pandas as pd
from feast import FeatureStore

store = FeatureStore(repo_path="feature_repo/")

labels_df = pd.DataFrame({
    "interaction_id": ["int-001", "int-002"],
    "reward_label": ["positive", "negative"],
    "safety_score": [0.95, 0.12],
    "labeler": ["nemo_guardrails", "nemo_guardrails"],
    "event_timestamp": pd.to_datetime(["2025-01-15", "2025-01-15"]),
})

# Writes to both online and offline stores
store.push("label_push_source", labels_df)
```

The `_fvs_for_push_source_or_raise()` method in FeatureStore was extended to iterate `list_label_views()` when resolving PushSource names, so existing push infrastructure works unchanged.

## Retrieval Path: get_historical_features()

LabelViews participate in historical retrieval through the same code path as regular feature views. The `get_any_feature_view()` registry method searches LabelViews alongside other view types, and LabelView exposes a `batch_source` property that unwraps the PushSource to its underlying batch source for offline store compatibility.

```python
# Direct feature references
training_df = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "driver_hourly_stats:conv_rate",        # from FeatureView
        "interaction_labels:reward_label",       # from LabelView
        "interaction_labels:safety_score",       # from LabelView
    ],
).to_df()
```

## FeatureService Composability

LabelViews can be bundled with regular FeatureViews in a `FeatureService`, allowing training pipelines to retrieve features and labels in a single call with point-in-time join semantics:

```python
from feast import FeatureService

training_service = FeatureService(
    name="interaction_training_service",
    features=[
        interaction_history,     # regular FeatureView
        interaction_labels,      # LabelView
    ],
)

# Single retrieval call for features + labels
training_df = store.get_historical_features(
    entity_df=entity_df,
    features=training_service,
).to_df()
```

## Batch Materialization: Explicitly Excluded

LabelViews are excluded from `feast materialize` and `feast materialize-incremental`. Labels are ingested via `push()` and do not go through the batch materialization pipeline. If a user explicitly requests materialization of a LabelView by name, a `ValueError` is raised with a clear message directing them to `FeatureStore.push()`.

```python
# _get_feature_views_to_materialize() in feature_store.py:
if isinstance(feature_view, LabelView):
    raise ValueError(
        f"LabelView {feature_view.name} cannot be materialized via "
        f"materialize(). Use FeatureStore.push() to write labels."
    )
```

## Versioning

LabelViews inherit full versioning support from `BaseFeatureView` via the feature view versioning system (RFC-44). Schema changes to a LabelView trigger automatic version snapshots. Version-qualified feature references (e.g., `interaction_labels@v2:reward_label`) work for both online and historical retrieval. Version pinning via `version="v1"` is also supported.

---

# Integration Points

LabelView integrates across the full Feast stack. The following table summarizes every component that was added or modified:

| Component | Change |
|---|---|
| `LabelView.proto` | New protobuf definition with `LabelViewSpec`, `LabelViewMeta`, `ConflictResolutionPolicy` enum |
| `RegistryServer.proto` | Added `label_view` arm to `ApplyFeatureViewRequest` oneof |
| `Permission.proto` | Added `LABEL_VIEW = 11` to `PermissionSpec.Type` enum |
| `Registry.proto` | Added `repeated LabelView label_views` field |
| `base_registry.py` | Added abstract methods: `_get_label_view`, `_list_label_views`, `delete_label_view`; `apply_materialization` type hint |
| `registry.py` (file) | Implemented label view CRUD, proto builder, delete, `apply_materialization` type hint |
| `sql.py` | Added `_infer_fv_table`/`_infer_fv_classes` for LabelView, `proto()` builder, type hints |
| `remote.py` | Added `apply_feature_view` branch for LabelView, type hints, get/list/delete methods |
| `snowflake.py` | Added `LABEL_VIEWS` DDL, `_infer_fv_classes`, `delete_feature_view` mapping, `proto()` builder |
| `registry_server.py` | Added `ApplyFeatureView` and proto builder branches for LabelView |
| `feature_store.py` | Extended `apply()`, `push()`, `teardown()`, `get_historical_features()`, `_make_inferences()`; excluded from `materialize` |
| `repo_operations.py` | Auto-collection of LabelView objects from repo modules |
| `repo_contents.py` | Added `label_views` field to `RepoContents` NamedTuple |
| `feature_service.py` | Accepts LabelView in `features` list |
| `feast_object.py` | Added LabelView to `FeastObject` union type |
| `permission.py` | Added `LABEL_VIEW` to `_PERMISSION_TYPES` map |
| CLI | Added `feast label-views list` and `feast label-views describe` commands |
| `provider.py` | Widened `update_infra` to accept `BaseFeatureView` for LabelView online table management |

---

# API Surface

## Python SDK

```python
from feast import Entity, FeatureStore, Field, PushSource
from feast.labeling import ConflictPolicy, LabelView
from feast.types import Float32, String

# Define
interaction_labels = LabelView(
    name="interaction_labels",
    entities=[interaction],
    ttl=timedelta(days=90),
    schema=[
        Field(name="interaction_id", dtype=String),
        Field(name="reward_label", dtype=String),
        Field(name="safety_score", dtype=Float32),
        Field(name="labeler", dtype=String),
    ],
    source=label_source,
    labeler_field="labeler",
    conflict_policy=ConflictPolicy.LAST_WRITE_WINS,
    retain_history=True,
    reference_feature_view="interaction_history",
)

# Register
store.apply([interaction, label_source, interaction_labels])

# Write labels
store.push("label_push_source", labels_df)

# Read online
store.get_online_features(
    features=["interaction_labels:reward_label"],
    entity_rows=[{"interaction_id": "int-001"}],
)

# Read historical (for training)
store.get_historical_features(
    entity_df=entity_df,
    features=["interaction_labels:reward_label"],
)

# List / get
store.list_label_views()
store.get_label_view("interaction_labels")

# Teardown
store.teardown()  # includes label view online tables
```

## CLI

```bash
# List all label views
feast label-views list

# Describe a specific label view
feast label-views describe interaction_labels
```

## ConflictPolicy Enum

| Policy | Behavior | Status |
|---|---|---|
| `LAST_WRITE_WINS` | Most recently written label wins (default) | Stored (default runtime behavior) |
| `LABELER_PRIORITY` | Higher-priority labelers override lower-priority ones | Stored only (not enforced) |
| `MAJORITY_VOTE` | Most frequent label value across labelers wins | Stored only (not enforced) |

---

# Registry Support

All four registry backends fully support LabelView CRUD operations: apply, get, list, delete, and proto serialization.

| Registry | Status |
|---|---|
| File-based registry | Supported |
| SQL registry | Supported |
| Remote gRPC registry | Supported |
| Snowflake registry | Supported |

The remote registry uses a dedicated `label_view` arm in the `ApplyFeatureViewRequest` oneof for gRPC transport.

---

# Permissions

LabelView is a permissioned resource. The `LABEL_VIEW` type was added to `Permission.proto` (value 11) and to the Python `_PERMISSION_TYPES` map, enabling standard Feast RBAC policies:

```python
from feast import Permission
from feast.permissions.action import AuthzedAction
from feast.labeling.label_view import LabelView

label_write_permission = Permission(
    name="label_writers",
    types=[LabelView],
    policy=my_policy,
    actions=[AuthzedAction.UPDATE],
)
```

---

# Migration & Backward Compatibility

* **Zero breaking changes.** LabelView is entirely opt-in. No existing Feast workflows, feature views, or configurations are affected. The primitive only appears when a user explicitly defines a LabelView in their repository.
* **No data migration.** LabelView uses the existing online and offline store infrastructure. No new store backends or table schemas are required beyond registry metadata.
* **Proto backward compatibility.** New proto fields use proto3 defaults. Old registry protos that lack LabelView sections deserialize correctly with empty label view lists.
* **Materialization unchanged.** LabelViews are excluded from the default materialization path. Running `feast materialize` without specifying a LabelView by name behaves identically to before.

---

# Why a Separate Primitive Instead of Extending FeatureView?

A natural question is: **why introduce a new type rather than adding optional label fields to `FeatureView`?**

Structurally, a LabelView today is a schema + entities + PushSource — similar to a `FeatureView` backed by a `PushSource`. The runtime code paths (push, online read, historical join) are identical. One could argue that `labeler_field`, `conflict_policy`, and `retain_history` could be optional fields on `FeatureView` instead of a new type.

We chose a separate primitive for the following reasons:

**1. Semantic separation matters more than implementation similarity.** Features and labels have fundamentally different lifecycle semantics. Features are append-only observations from a single source. Labels are mutable judgments from multiple sources. The type distinction lets users and tooling reason about data intent from the type system alone, rather than inspecting optional fields to determine if a "feature view" is really a label store.

**2. Features and labels differ in data nature, not compute timing.** The existing feature view hierarchy separates views by *when* or *how* compute runs (batch, streaming, on-demand). LabelView differs in *what the data represents* (mutable judgments vs. immutable observations). This is an orthogonal axis — one about compute, one about data semantics — and deserves its own type rather than being overloaded onto a compute-oriented hierarchy.

**3. Future enforcement requires a type boundary.** When `conflict_policy` and `retain_history` are enforced, the online store read and write paths will branch on "is this a label view?" — multi-row storage per entity, conflict resolution queries, history compaction. A distinct type makes these branches clean `isinstance` checks rather than scattered `if feature_view.conflict_policy is not None` guards across every store implementation.

**4. Materialization exclusion is semantic, not accidental.** LabelViews are excluded from `feast materialize` because labels are push-only by design. If labels were `FeatureView` objects, this exclusion would require a flag like `skip_materialization=True` and every materialization code path would need to check it. The type distinction makes the exclusion natural.

**5. Registry, permissions, and CLI benefit from type-level separation.** `feast label-views list` is clearer than labels being mixed into `feast feature-views list`. The `Permission` system distinguishing `LABEL_VIEW` from `FEATURE_VIEW` enables fine-grained RBAC (e.g., safety team can write labels but not modify features).

## Forward compatibility

LabelView inherits from `BaseFeatureView` and uses identical runtime code paths as `FeatureView`. If the community later decides labels should be a `FeatureView` variant with optional fields, the migration is straightforward — the two share the same base class, protobuf serialization model, and registry operations.

The design follows the principle that **it is easier to merge two types later than to split one type in two.** Starting with a distinct primitive is the lower-risk direction.

---

# Alpha Limitations & Future Work

| Limitation | Current Behavior | Future Direction |
|---|---|---|
| Conflict policy enforcement | `conflict_policy` is stored but not enforced at read time. Online store returns last-written row. | Online store query-path changes to consider multiple rows per entity key and apply resolution strategy. |
| History retention | `retain_history` is stored but not enforced at write time. Online store always overwrites. | Online store write-path changes to append rather than upsert, with compaction/eviction. |
| Labeler priority configuration | `LABELER_PRIORITY` policy has no mechanism to define the priority ordering. | Add a `labeler_priorities` field to `LabelViewSpec` or a separate config. |
| Batch materialization | LabelViews are excluded from `materialize`. Push-only ingestion. | Consider batch backfill support for historical label imports. |
| Cross-version label joins | No special handling for joining labels across versions in historical retrieval. | Version-aware label joins for reproducible training. |
| Label-aware training API | No dedicated `get_training_dataset(features=..., labels=...)` API. | First-class training dataset API that understands the feature/label distinction. |

---

# Open Questions

1. **Should conflict policy enforcement be configurable per online store?** Different online stores have different query capabilities. Some (e.g., SQL-based) could implement MAJORITY_VOTE natively; others (e.g., Redis) would need application-level resolution.

2. **Should retain_history have a configurable retention window?** Unbounded history growth is a concern. A `max_history_entries` or `history_ttl` config could bound storage while preserving auditability.

3. **Should LabelView support batch backfill via materialize?** Some teams have large historical label datasets they want to load into Feast. Currently they must use `push()` in a loop. A batch import path could be more efficient.

4. **Should FeatureService distinguish features from labels?** Today, FeatureService treats LabelViews and FeatureViews uniformly. A future enhancement could tag which projections are "labels" for downstream frameworks that need this distinction (e.g., auto-splitting X/y in training).

---

# References

* Branch: `labelView`
* Documentation: `docs/getting-started/concepts/label-view.md`
* Proto definition: `protos/feast/core/LabelView.proto`
* Python module: `sdk/python/feast/labeling/`
* Unit tests: `sdk/python/tests/unit/test_label_view.py`
