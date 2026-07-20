# ADR-0011: Data Quality Monitoring

## Status

Superseded — The original external-library-based validation has been replaced by Feast's native [Feature Quality Monitoring](../how-to-guides/feature-monitoring.md) system (`feast monitor run`).

## Context

Data quality issues can significantly impact ML model performance. Several complex data problems needed to be addressed:

- **Data consistency**: New training datasets can differ significantly from previous datasets, potentially requiring changes in model architecture.
- **Upstream pipeline bugs**: Bugs in upstream pipelines can cause invalid values to overwrite existing valid values in an online store.
- **Training/serving skew**: Distribution shift between training and serving data can decrease model performance.

Feast needed a mechanism to validate data to catch these issues before they affect model training or serving.

## Decision

Introduce a Data Quality Monitoring (DQM) module that validates datasets against user-curated rules.

### Original Design (now replaced)

The original validation process used a **reference dataset** and a **profiler** pattern:

1. User prepares a reference dataset (saved from a known-good historical retrieval).
2. User defines a profiler function that produces a profile (set of expectations) from a dataset.
3. Validation is performed by comparing the tested dataset against the reference profile.

This approach was limited to historical retrieval only, required additional dependencies, and offered no built-in UI or automation.

### Current Design

The current system (`feast monitor run`) provides:

- Automatic metric computation (null rates, percentiles, histograms) with no external dependencies
- Monitoring across batch data and serving logs
- CLI and REST API for automation
- Built-in UI monitoring dashboard
- Support for all offline store backends via SQL push-down

See [Feature Quality Monitoring](../how-to-guides/feature-monitoring.md) for full documentation.

## Consequences

### Positive

- Users can detect data quality issues before they affect model training.
- Native integration requires no extra dependencies.
- Covers both batch data and serving logs.
- Built-in UI provides immediate visibility into feature health.
- Baselines computed automatically on `feast apply`.

### Negative

- Migration required from the original profiler-based approach.

## References

- Original RFC: Feast RFC-027: Data Quality Monitoring
- Implementation: `sdk/python/feast/monitoring/`
- Documentation: [Data Quality Monitoring](../reference/dqm.md)
- [Feature Quality Monitoring guide](../how-to-guides/feature-monitoring.md)
