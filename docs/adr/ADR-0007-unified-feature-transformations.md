# ADR-0007: Unified Feature Transformations and Feature Views

## Status

Accepted

## Context

In Feast, the `OnDemandFeatureView` name did not clearly convey that transformations execute at read time. The term "On Demand" was ambiguous about when the transformation occurs. Additionally, there were multiple feature view types (`FeatureView`, `BatchFeatureView`, `StreamFeatureView`, `OnDemandFeatureView`) with:

- **Excessive logic** handling each type throughout the codebase (e.g., `FeatureStore.apply()`, `get_online_features`, `get_historical_features`).
- **Redundant fields** across the different feature view classes.
- **Unclear transformation timing**: when transformations occur, where they execute, and how materialization works varied by type.

| Type | When Transformation Occurs | Where | Materialization |
|------|---------------------------|-------|-----------------|
| FeatureView | Undefined | Outside Feast | Feature Server or Batch |
| BatchFeatureView | Batch process | Offline Store (external) | Feature Server or Batch |
| StreamFeatureView | Streaming process | Stream Processor (external) | Stream Processor |
| OnDemandFeatureView | On Read | Feature Server | Feature Server |
| OnDemandFeatureView (writes) | On Write | Feature Server | Feature Server |

## Decision

Unify Batch, Streaming, and On-Demand feature views into a single `FeatureView` class with a `@transform` decorator that makes execution timing explicit.

### Transformation Types

```python
from enum import Enum

class FeastTransformation(Enum):
    NONE = 0       # No transformations (default)
    ON_READ = 1    # Current On Demand Feature View behavior
    ON_WRITE = 2   # Transformations at write time
    BATCH = 3      # Batch processing transformations
    STREAMING = 4  # Stream transformations
```

### Proposed API

```python
@transform(
    type=FeastTransformation.ON_WRITE,
    schema=[...],
    entity=[...],
    sources=[...],
    mode="python",           # pandas, substrate, etc.
    engine="feature_server", # Spark, Snowflake, etc.
    orchestrator=None,       # Airflow, KFP, etc.
)
def my_feature_view(inputs):
    outputs = {
        "my_feature": [v * 1.0 for v in inputs["input_feature_1"]],
    }
    return outputs
```

### Key Decisions

- **Single class** rather than defining a V2 class. A breaking change in stages is preferred to avoid rework for Java and Go servers.
- **Explicit transformation timing** via enum rather than implicit behavior based on class type.
- **Staggered release**: Ship a version supporting both old and new APIs with deprecation logging, then ship a breaking version.
- **Five clear primitives**: Entities, DataSources, Fields, FeatureViews, and FeatureServices.

### Current Implementation Status

The codebase currently uses a `transformation()` decorator and `TransformationMode` enum (with modes like PANDAS, PYTHON, SPARK, RAY, SQL, SUBSTRAIT) in `sdk/python/feast/transformation/base.py`. The legacy `OnDemandFeatureView`, `StreamFeatureView`, and `BatchFeatureView` classes still exist during the migration period.

## Consequences

### Positive

- Clearer, more explicit API that makes transformation timing obvious.
- Removes excessive handling of each feature view type throughout the codebase.
- Eliminates redundant field definitions across multiple classes.
- Establishes five clear primitives for the Feast data model.
- FeatureViews can declare other FeatureViews as data sources, enabling computational graphs.

### Negative

- Requires a migration period with both old and new APIs supported.
- Breaking change that needs careful coordination across Python, Java, and Go components.
- Users must update existing feature view definitions during migration.

## References

- Original RFC: [Feast RFC-043: Unify Feature Transformations and Feature Views](https://docs.google.com/document/d/1KXCXcsXq1bUvbSpfhnUjDSsu4HpuUZ5XiZoQyltCkvo/edit)
- GitHub Issue: [#4584](https://github.com/feast-dev/feast/issues/4584)
- Related RFCs: RFC-021 (On-Demand Transformations), RFC-036 (Stream Transformations)
- Implementation: `sdk/python/feast/transformation/base.py`
