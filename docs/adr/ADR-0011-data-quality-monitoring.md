# ADR-0011: Data Quality Monitoring

## Status

Accepted

## Context

Data quality issues can significantly impact ML model performance. Several complex data problems needed to be addressed:

- **Data consistency**: New training datasets can differ significantly from previous datasets, potentially requiring changes in model architecture.
- **Upstream pipeline bugs**: Bugs in upstream pipelines can cause invalid values to overwrite existing valid values in an online store.
- **Training/serving skew**: Distribution shift between training and serving data can decrease model performance.

Feast needed a mechanism to validate data at retrieval time to catch these issues before they affect model training or serving.

## Decision

Introduce a Data Quality Monitoring (DQM) module that validates datasets against user-curated rules, initially targeting historical retrieval (training dataset generation).

### Design

The validation process uses a **reference dataset** and a **profiler** pattern:

1. User prepares a reference dataset (saved from a known-good historical retrieval).
2. User defines a profiler function that produces a profile (set of expectations) from a dataset.
3. Validation is performed by comparing the tested dataset against the reference profile.

### Integration with Great Expectations

The initial implementation uses [Great Expectations](https://greatexpectations.io/) as the validation engine:

```python
from feast.dqm.profilers.ge_profiler import ge_profiler
from great_expectations.dataset import Dataset
from great_expectations.core.expectation_suite import ExpectationSuite

@ge_profiler
def my_profiler(dataset: Dataset) -> ExpectationSuite:
    dataset.expect_column_max_to_be_between("column", 1, 2)
    dataset.expect_column_values_to_not_be_null("important_feature")
    return dataset.get_expectation_suite()
```

### Usage

Validation is triggered during historical feature retrieval via a `validation_reference` parameter:

```python
from feast import FeatureStore

store = FeatureStore(".")

job = store.get_historical_features(...)
df = job.to_df(
    validation_reference=store
        .get_saved_dataset("my_reference_dataset")
        .as_reference(profiler=my_profiler)
)
```

If validation fails, a `ValidationFailed` exception is raised with details for all expectations that didn't pass. If validation succeeds, the materialized dataset is returned normally.

### Key Decisions

- **Profiler-based approach**: Users define their own validation rules via profiler functions rather than Feast prescribing fixed validation rules.
- **Great Expectations integration**: Leverages an established data validation framework rather than building custom validation logic.
- **Validation at retrieval time**: Validation is performed when datasets are materialized (`.to_df()` or `.to_arrow()`), not during ingestion.
- **ValidationReference as a registry object**: Saved datasets and their validation references are stored in the Feast registry for reuse.

## Consequences

### Positive

- Users can detect data quality issues before they affect model training.
- Flexible profiler pattern allows custom validation rules per use case.
- Integration with Great Expectations provides a rich set of built-in expectations.
- Reference datasets provide a baseline for detecting data drift.

### Negative

- Currently limited to historical retrieval; online store write/read validation is planned but not yet implemented.
- Dependency on Great Expectations adds to the install footprint (optional via `feast[ge]`).
- Automatic profiling capabilities are limited; manual expectation crafting is recommended.

## References

- Original RFC: Feast RFC-027: Data Quality Monitoring (Google Drive shortcut no longer accessible)
- Implementation: `sdk/python/feast/dqm/`, `sdk/python/feast/saved_dataset.py`
- Documentation: [Data Quality Monitoring](../reference/dqm.md)
