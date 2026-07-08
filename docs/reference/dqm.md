# Data Quality Monitoring

{% hint style="warning" %}
**Deprecated:** The Great Expectations-based validation described on this page is deprecated and will be removed in a future release. It has been superseded by Feast's built-in [Feature Quality Monitoring](../how-to-guides/feature-monitoring.md) system, which provides richer metrics (histograms, percentiles, drift detection), works across batch data and serving logs, requires no external dependencies, and includes a built-in UI dashboard.

Please migrate to the new monitoring system. See the [Feature Quality Monitoring guide](../how-to-guides/feature-monitoring.md) for setup instructions.
{% endhint %}

## Legacy: Great Expectations Integration

The following documents the deprecated Great Expectations-based validation that was previously the only DQM option in Feast. This integration relied on `pip install 'feast[ge]'` and only supported validation during historical retrieval.

---

### Overview

The legacy validation process consists of the following steps:
1. User prepares reference dataset (only [saved datasets](../getting-started/concepts/dataset.md) from historical retrieval are supported).
2. User defines a profiler function that produces a profile using [Great Expectations](https://docs.greatexpectations.io).
3. Validation of the tested dataset is performed with the reference dataset and profiler provided as parameters.

### Installation
```shell
pip install 'feast[ge]'
```

### Dataset profile

This integration uses [Great Expectation's](https://greatexpectations.io/) [ExpectationSuite](https://legacy.docs.greatexpectations.io/en/latest/autoapi/great_expectations/core/expectation_suite/index.html#great_expectations.core.expectation_suite.ExpectationSuite)
as the dataset profile format. The user defines a profiler function that receives a dataset and returns an ExpectationSuite.

```python
from great_expectations.dataset import Dataset
from great_expectations.core.expectation_suite import ExpectationSuite

from feast.dqm.profilers.ge_profiler import ge_profiler

@ge_profiler
def manual_profiler(dataset: Dataset) -> ExpectationSuite:
    dataset.expect_column_max_to_be_between("column", 1, 2)
    return dataset.get_expectation_suite()
```

### Validating Training Dataset

During retrieval of historical features, `validation_reference` can be passed as a parameter to methods `.to_df(validation_reference=...)` or `.to_arrow(validation_reference=...)` of RetrievalJob.
If validation is successful, the materialized dataset is returned. Otherwise, `feast.dqm.errors.ValidationFailed` exception is raised with details for expectations that didn't pass.

```python
from feast import FeatureStore

fs = FeatureStore(".")

job = fs.get_historical_features(...)
job.to_df(
    validation_reference=fs
        .get_saved_dataset("my_reference_dataset")
        .as_reference(profiler=manual_profiler)
)
```

---

## Migration Guide

The new [Feature Quality Monitoring](../how-to-guides/feature-monitoring.md) system replaces this integration with:

| Capability | GE-based (deprecated) | New DQM |
|---|---|---|
| Scope | Historical retrieval only | Batch data + serving logs |
| Dependencies | `feast[ge]` extra required | No extra dependencies |
| Metrics | User-defined expectations | Automatic: null rates, percentiles, histograms, drift |
| UI | None | Built-in monitoring dashboard |
| Automation | Manual profiler code | `feast monitor run` CLI + REST API |
| Backends | Limited | All offline store backends |

To migrate:

1. Enable DQM in `feature_store.yaml`:
   ```yaml
   data_quality_monitoring:
     auto_baseline: true
   ```

2. Run `feast apply` to compute baseline metrics automatically.

3. Schedule `feast monitor run` for ongoing monitoring.

4. Remove the `feast[ge]` dependency from your requirements.
