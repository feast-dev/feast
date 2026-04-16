# Label View

{% hint style="info" %}
**\[Alpha]** Label views are an alpha feature. The API may change in future releases.
{% endhint %}

A **label view** is a Feast primitive that manages *mutable* labels and annotations, kept separate from the *immutable* feature data stored in regular [feature views](feature-view.md). This separation follows a clean design principle: observational data (features) is append-only, while judgments about that data (labels, scores, reward signals) are updated over time by multiple independent sources.

Label views are especially useful in **RLHF/reward-modeling pipelines**, **multi-annotator workflows**, and **safety monitoring systems** where different labelers — human reviewers, automated scanners, reward models — independently write labels for the same entity keys.

## Key Capabilities

- **Multi-labeler support**: Multiple independent labelers can write labels for the same entity key. A configurable `labeler_field` tracks which source wrote each label.
- **Conflict resolution policies**: When labelers disagree, Feast resolves conflicts according to a `ConflictPolicy` — last-write-wins, labeler priority, or majority vote. See [Alpha limitations](#alpha-limitations) below.
- **History retention**: Optionally retain the full history of label writes per entity key, not just the latest value. See [Alpha limitations](#alpha-limitations) below.
- **Reference feature view**: Optionally link a label view to the `FeatureView` whose entities it annotates, for documentation and lineage.
- **Flexible ingestion**: Labels can be written in real time via `FeatureStore.push()` using a `PushSource`, or loaded in bulk from a historical table (Snowflake, Spark, Parquet, etc.) by setting a `batch_source` and running `feast materialize`.
- **FeatureService composability**: Label views can be included alongside regular feature views in a `FeatureService`, so training pipelines can retrieve features and their labels together.

## When to use Label Views

| Use a **FeatureView** when… | Use a **LabelView** when… |
|---|---|
| Data is observational and append-only (e.g. driver trip counts, page views) | Data is a judgment or annotation about an entity (e.g. reward labels, safety scores) |
| A single source of truth writes the data | Multiple labelers may write conflicting values for the same key |
| History is naturally time-series | You need explicit control over whether history is retained or overwritten |

## Defining a Label View

```python
from datetime import timedelta

from feast import Entity, FeatureService, Field, PushSource
from feast.labeling import ConflictPolicy, LabelView
from feast.types import Float32, String

interaction = Entity(
    name="interaction",
    join_keys=["interaction_id"],
)

label_source = PushSource(
    name="label_push_source",
    schema=[
        Field(name="interaction_id", dtype=String),
        Field(name="reward_label", dtype=String),
        Field(name="safety_score", dtype=Float32),
        Field(name="labeler", dtype=String),
    ],
)

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
    description="Reward and safety labels on agent interactions.",
    owner="ml-safety-team@example.com",
)
```

## Conflict Policies

The `ConflictPolicy` enum controls how conflicting labels from different labelers are **intended** to be resolved at read time:

| Policy | Behavior |
|---|---|
| `LAST_WRITE_WINS` | The most recently written label for a given entity key takes precedence, regardless of which labeler wrote it. This is the default. |
| `LABELER_PRIORITY` | Labels are ranked by a pre-configured labeler priority order. Higher-priority labelers override lower-priority ones. |
| `MAJORITY_VOTE` | The label value that appears most frequently across all labelers is selected. Useful for consensus-based annotation workflows. |

## Alpha Limitations

{% hint style="warning" %}
The following capabilities are **defined and stored** in the label-view metadata but are **not yet enforced** by the Feast runtime. They are persisted in the registry so that future releases can activate them without a schema migration.
{% endhint %}

### Conflict-policy enforcement at read time

`conflict_policy` is stored as part of the `LabelView` definition, but it is **not enforced** during `get_online_features`. The online store currently returns the last-written row for a given entity key regardless of which policy is configured.

Real enforcement will require changes to the online-store query path so that the store can consider multiple rows per entity key and apply the conflict-resolution strategy.

### History retention at write time

`retain_history` is stored but **not acted on**. The online store always overwrites the previous value when a new label is written for the same entity key.

Implementing retention will require changes to the online-store write path so that it appends rather than upserts, along with a compaction or eviction strategy for old entries.

### Batch materialization

Batch materialization behaviour depends on whether the label view has a `batch_source`:

- **With `batch_source`** (direct `DataSource` or a `PushSource` that wraps a `batch_source`): `feast materialize` and `feast materialize-incremental` include the label view and write historical label rows to the offline store. This is the recommended path for teams with large pre-existing label tables (e.g. a Snowflake or Spark table of loan-default outcomes).
- **Without `batch_source`** (push-only label views): the label view is excluded from `feast materialize`. Labels must arrive via `FeatureStore.push()`. Attempting to materialize such a label view by name will raise a clear error.

## Using with Feature Services

Label views can be composed with regular feature views in a `FeatureService`, so downstream consumers (training pipelines, batch scoring jobs) get features and labels in a single retrieval call:

```python
training_service = FeatureService(
    name="interaction_training_service",
    features=[
        interaction_history,    # regular FeatureView with immutable features
        interaction_labels,     # LabelView with mutable reward labels
    ],
)
```

## Pushing Labels

Labels are typically written via `FeatureStore.push()` using the label view's `PushSource`:

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

store.push("label_push_source", labels_df)
```

This writes the labels into both the online and offline stores, making them available for real-time serving and historical training dataset generation.
