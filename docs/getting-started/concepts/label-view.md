# Label View

## What is a Label View?

A **label view** is a Feast primitive for storing **judgments about entities** — reward signals, safety scores, human reviews, ground-truth answers — separately from the **immutable observations** stored in [feature views](feature-view.md).

| | Feature View | Label View |
|---|---|---|
| **Stores** | What was observed | What was judged |
| **Example** | Agent prompt and response | `response_quality: "poor"`, `is_safe: 0` |
| **Writers** | Usually one source | Multiple labelers (human, LLM judge, code) |
| **Changes over time** | Append-only | Updated by new label writes |

Use a label view when you need **governed, training-ready labels** that multiple sources can write, disagree on, and resolve — not when you need append-only feature data.

## Prerequisites

Before using label views, you need:

* Feast with the LabelView primitive
* A feature repo with at least one `Entity`
* A `PushSource` (or batch `DataSource`) for label ingestion
* `feast apply` run after defining label views

## Why use Label Views?

**Separate features from judgments.** Mixing reward labels into feature views blurs two different lifecycles — observations are append-only; labels are overwritten, corrected, and debated.

**Support multiple labelers.** Human reviewers, safety scanners, and LLM judges can all label the same entity. Feast tracks who wrote what via `labeler_field` and resolves conflicts via `ConflictPolicy`.

**Generate training datasets.** Compose label views with feature views in a `FeatureService` and retrieve features + labels together with point-in-time correctness.

**Annotate in the UI.** Configure annotation profiles so data scientists can label data directly in the Feast UI — entity forms, document spans, bulk review, or active learning.

## Feedback vs Expectations

Not all labels serve the same purpose. Feast distinguishes two common types — both stored in a label view, modeled with field names and tags:

| | **Feedback** | **Expectation** |
|---|---|---|
| **Question** | How good was the actual output? | What is the correct answer? |
| **Example** | `response_quality: "poor"`, `relevance: "irrelevant"` | `ground_truth: "relevant"`, `is_default: 1` |
| **Typical writers** | Human, LLM judge, automated code | Human experts (gold standard) |
| **Training use** | Reward signal, quality filter, active-learning queue | Supervised target column |
| **Conflict handling** | Common — use `ConflictPolicy` | Rare — usually one authoritative source |

Feast does not require separate primitives for feedback and expectations. Model them with **field names** and **tags**:

```python
tags={
    "feast.io/field-role:response_quality": "feedback",    # judgment about output
    "feast.io/field-role:ground_truth": "expectation",     # correct answer
}
```

**Practical pattern:**

* **Feedback label view** — multi-labeler, `ConflictPolicy`, fields like `response_quality`, `safety_score`, `relevance`
* **Expectation fields or view** — stable ground truth, fields like `ground_truth`, `expected_answer`, `is_default`
* **Mixed view** — one label view with both (e.g. RAG: `relevance` = feedback, `ground_truth` = expectation)

## Sources of Labels

Label views accept labels from any source. Track the writer in `labeler_field`:

| Source | `labeler` example | Typical role |
|---|---|---|
| **Human reviewer** | `human-reviewer@company.com` | Feedback or expectation |
| **LLM judge** | `gpt-4-evaluator` | Feedback (quality scores) |
| **Automated scanner** | `nemo-guardrails` | Feedback (safety signals) |
| **Batch import** | `risk-ops-team` | Expectation (historical outcomes) |

```python
labels_df = pd.DataFrame({
    "user_id": ["user-001"],
    "response_quality": ["poor"],
    "is_safe": [0],
    "labeler": ["human-reviewer@company.com"],
    "event_timestamp": [pd.Timestamp.now()],
})
store.push("agent_feedback_labels_push_source", labels_df)
```

## When to use Label Views

| Use a **FeatureView** when… | Use a **LabelView** when… |
|---|---|
| Data is observational and append-only | Data is a judgment or annotation |
| One source writes the data | Multiple labelers may disagree |
| No conflict resolution needed | You need governed conflict resolution |
| No labeling UI needed | You want structured annotation workflows |

## How Label Views Work

### Step 1: Define a label view

```python
from datetime import timedelta

from feast import Entity, FeatureService, Field, FileSource, PushSource
from feast.labeling import ConflictPolicy, LabelView
from feast.types import Float32, Int64, String

user = Entity(name="user_id", join_keys=["user_id"])

agent_feedback_labels = LabelView(
    name="agent_feedback_labels",
    entities=[user],
    schema=[
        Field(name="response_quality", dtype=String),
        Field(name="is_safe", dtype=Int64),
        Field(name="reviewer_notes", dtype=String),
        Field(name="labeler", dtype=String),
    ],
    source=PushSource(
        name="agent_feedback_labels_push_source",
        batch_source=FileSource(
            name="agent_feedback_labels_batch",
            path="data/agent_feedback.parquet",
            timestamp_field="event_timestamp",
        ),
    ),
    labeler_field="labeler",
    conflict_policy=ConflictPolicy.LAST_WRITE_WINS,
    reference_feature_view="user_profile",
    description="Human and automated feedback on agent responses.",
    tags={
        "feast.io/labeling-method": "entity-form",
        "feast.io/field-role:response_quality": "feedback",
        "feast.io/field-role:is_safe": "feedback",
        "feast.io/field-role:reviewer_notes": "metadata",
        "feast.io/label-values:response_quality": "excellent,good,acceptable,poor,harmful",
        "feast.io/label-values:is_safe": "1,0",
        "feast.io/label-widget:response_quality": "enum",
        "feast.io/label-widget:is_safe": "binary",
        "feast.io/label-widget:reviewer_notes": "text",
    },
)
```

Run `feast apply` to register the label view.

### Step 2: Push labels

Labels are written with `FeatureStore.push()`:

```python
import pandas as pd
from feast import FeatureStore

store = FeatureStore(repo_path="feature_repo/")

labels_df = pd.DataFrame({
    "user_id": ["user-001", "user-002"],
    "response_quality": ["good", "harmful"],
    "is_safe": [1, 0],
    "reviewer_notes": ["Accurate summary", "Unsafe medical advice"],
    "labeler": ["human-reviewer", "nemo-guardrails"],
    "event_timestamp": pd.to_datetime(["2025-01-15", "2025-01-15"]),
})

store.push("agent_feedback_labels_push_source", labels_df)
```

Each push appends to the offline store (full history retained) and updates the online store (latest value per key).

### Step 3: Annotate in the Feast UI

Open the label view in the Feast UI **Annotate** tab. The UI reads annotation tags and shows the right workflow:

1. Open **Label Views** in the sidebar
2. Select a label view (check the **Annotation** badge on the list page)
3. Go to the **Annotate** tab
4. Choose a method (Entity Form, Document Span, Review & Edit, or Active Learning)
5. Submit labels — they are pushed to the label view's `PushSource`

### Step 4: Join labels with features for training

```python
training_service = FeatureService(
    name="agent_training_service",
    features=[
        user_profile_fv,          # immutable features
        agent_feedback_labels,    # mutable labels
    ],
)

training_df = store.get_historical_features(
    entity_df=entities_df,
    features=training_service,
).to_df()
```

Training pipelines get features and resolved labels in one retrieval call.

## Annotation Profiles

Annotation profiles configure **how** labels are created in the UI. Set them via `tags` — no schema changes required.

### Supported profiles

| Profile | Best for | UI experience |
|---|---|---|
| `entity-form` | RLHF, safety review, per-entity feedback | Form — one entity at a time |
| `document-span` | RAG chunk labeling, span annotation | Load document, label chunks |
| `table` | Bulk review, correcting existing labels | Editable table with dropdowns |
| `active-learning` | Label high-value unlabeled entities | Queue from a reference feature view |

### Choosing a profile

Answer one question:

1. **"I need to review agent responses one at a time"** → `entity-form`
2. **"I need to label document chunks for RAG"** → `document-span`
3. **"I need to correct labels in bulk"** → `table`
4. **"I want to label only the most valuable unlabeled items"** → `active-learning` (requires `reference_feature_view`)

The **Annotate** tab shows only relevant methods per profile:

| Profile | Methods shown |
|---|---|
| `document-span` | Document Span, Review & Edit |
| `entity-form` | Entity Form, Review & Edit, Active Learning |
| `table` | Review & Edit, Active Learning, Entity Form |
| `active-learning` | Active Learning, Entity Form, Review & Edit |

### Tag reference

| Tag | Purpose | Example values |
|---|---|---|
| `feast.io/labeling-method` | Primary UI workflow | `entity-form`, `document-span`, `table` |
| `feast.io/field-role:<field>` | Semantic role of a field | `feedback`, `expectation`, `label`, `metadata`, `content`, `span_start`, `span_end` |
| `feast.io/label-values:<field>` | Allowed label values | `relevant,irrelevant` |
| `feast.io/label-widget:<field>` | Input widget type | `enum`, `binary`, `text`, `number` |

## Examples by use case

### Agent feedback (RLHF / safety)

Feedback from human reviewers and automated safety layers on agent responses.

```python
agent_feedback_labels = LabelView(
    name="agent_feedback_labels",
    entities=[user],
    schema=[
        Field(name="response_quality", dtype=String),
        Field(name="is_safe", dtype=Int64),
        Field(name="reviewer_notes", dtype=String),
        Field(name="labeler", dtype=String),
    ],
    source=feedback_push_source,
    labeler_field="labeler",
    conflict_policy=ConflictPolicy.LAST_WRITE_WINS,
    reference_feature_view="user_profile",
    tags={"feast.io/labeling-method": "entity-form", ...},
)
```

### RAG chunk labeling (feedback + expectation)

One view can hold both a retrieval judgment and ground truth:

```python
rag_chunk_labels = LabelView(
    name="rag_chunk_labels",
    entities=[chunk],
    schema=[
        Field(name="source_document", dtype=String),
        Field(name="chunk_text", dtype=String),
        Field(name="relevance", dtype=String),       # feedback
        Field(name="ground_truth", dtype=String),    # expectation
        Field(name="chunk_start", dtype=Int64),
        Field(name="chunk_end", dtype=Int64),
        Field(name="labeler", dtype=String),
    ],
    source=rag_push_source,
    labeler_field="labeler",
    conflict_policy=ConflictPolicy.MAJORITY_VOTE,
    tags={
        "feast.io/labeling-method": "document-span",
        "feast.io/field-role:relevance": "feedback",
        "feast.io/field-role:ground_truth": "expectation",
        "feast.io/field-role:chunk_text": "content",
        "feast.io/label-values:relevance": "relevant,irrelevant",
    },
)
```

### Historical ground truth (batch labels)

Pre-existing label tables loaded via `feast materialize`:

```python
loan_default_labels = LabelView(
    name="loan_default_labels",
    entities=[loan],
    schema=[
        Field(name="is_default", dtype=Int64),
        Field(name="delinquency_days", dtype=Int64),
        Field(name="loss_severity", dtype=Float32),
        Field(name="labeler", dtype=String),
    ],
    source=PushSource(
        name="loan_default_labels_push_source",
        batch_source=FileSource(
            path="data/loan_defaults.parquet",
            timestamp_field="event_timestamp",
        ),
    ),
    labeler_field="labeler",
    conflict_policy=ConflictPolicy.LABELER_PRIORITY,
    tags={
        "feast.io/labeling-method": "table",
        "feast.io/field-role:is_default": "expectation",
    },
)
```

## Conflict policies

When multiple labelers write different values for the same entity, `ConflictPolicy` picks one value for **offline store reads** (training, UI browse):

| Policy | When to use |
|---|---|
| `LAST_WRITE_WINS` | Default. Most recent write wins. |
| `LABELER_PRIORITY` | Trusted labelers override others (e.g. human over LLM judge). |
| `MAJORITY_VOTE` | Consensus labeling (e.g. multiple annotators on RAG chunks). |

{% hint style="info" %}
Conflict policies apply to the **offline store** (training). The **online store** always uses last-write-wins. Full label history is always retained in the offline store.
{% endhint %}

## Best practices

**Name fields by intent.** Use `response_quality` (feedback) and `ground_truth` (expectation) — not generic `score` or `label`.

**Tag field roles.** Set `feast.io/field-role:<field>` to `feedback` or `expectation` so your team and UI know what each field means.

**Match conflict policy to label type.** Use `LABELER_PRIORITY` when humans correct automated judges. Use `MAJORITY_VOTE` for multi-annotator consensus. Use `LAST_WRITE_WINS` for simple feedback streams.

**Link to features.** Set `reference_feature_view` so the UI and documentation show which feature view the labels annotate.

**Separate noisy feedback from stable ground truth.** When possible, put expectations in dedicated fields or views with stricter writer conventions (human-only).

## Limitations

* Conflict policies are enforced on offline reads only; online store is always last-write-wins.
* `LABELER_PRIORITY` requires explicit labeler ordering configuration.
* Annotation profiles are UI configuration via tags — not enforced at the SDK write path.

## Next steps

* [Feature view](feature-view.md) — immutable features that label views annotate
* [Feature retrieval](feature-retrieval.md) — point-in-time joins for training
* [ADR-0012: LabelView](../../adr/ADR-0012-label-view.md) — full design rationale
