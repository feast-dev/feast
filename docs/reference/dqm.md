# Data Quality Monitoring

Data Quality Monitoring (DQM) is a Feast module aimed to help users to validate their data with the user-curated set of rules.
Validation could be applied during:
* Historical retrieval (training dataset generation)
* [planned] Writing features into an online store
* [planned] Reading features from an online store

Its goal is to address several complex data problems, namely:
* Data consistency - new training datasets can be significantly different from previous datasets. This might require a change in model architecture.
* Issues/bugs in the upstream pipeline - bugs in upstream pipelines can cause invalid values to overwrite existing valid values in an online store.
* Training/serving skew - distribution shift could significantly decrease the performance of the model.

> To monitor data quality, we check that the characteristics of the tested dataset (aka the tested dataset's profile) are "equivalent" to the characteristics of the reference dataset.
> How exactly profile equivalency should be measured is up to the user. 

### Overview

The validation process consists of the following steps:
1. User prepares reference dataset (currently only [saved datasets](../getting-started/concepts/dataset.md) from historical retrieval are supported).
2. User defines profiler function, which should produce profile by given dataset (currently only profilers based on [Great Expectations](https://docs.greatexpectations.io) are allowed).
3. Validation of tested dataset is performed with reference dataset and profiler provided as parameters.

### Preparations
Feast with Great Expectations support can be installed via
```shell
pip install 'feast[ge]'
```

### Dataset profile
Currently, Feast supports only [Great Expectation's](https://greatexpectations.io/) [ExpectationSuite](https://legacy.docs.greatexpectations.io/en/latest/autoapi/great_expectations/core/expectation_suite/index.html#great_expectations.core.expectation_suite.ExpectationSuite)
as dataset's profile. Hence, the user needs to define a function (profiler) that would receive a dataset and return an [ExpectationSuite](https://legacy.docs.greatexpectations.io/en/latest/autoapi/great_expectations/core/expectation_suite/index.html#great_expectations.core.expectation_suite.ExpectationSuite).

Great Expectations supports automatic profiling as well as manually specifying expectations:
```python
from great_expectations.dataset import Dataset
from great_expectations.core.expectation_suite import ExpectationSuite

from feast.dqm.profilers.ge_profiler import ge_profiler

@ge_profiler
def automatic_profiler(dataset: Dataset) -> ExpectationSuite:
    from great_expectations.profile.user_configurable_profiler import UserConfigurableProfiler

    return UserConfigurableProfiler(
        profile_dataset=dataset,
        ignored_columns=['conv_rate'],
        value_set_threshold='few'
    ).build_suite()
```
However, from our experience capabilities of automatic profiler are quite limited. So we would recommend crafting your own expectations:
```python
@ge_profiler
def manual_profiler(dataset: Dataset) -> ExpectationSuite:
    dataset.expect_column_max_to_be_between("column", 1, 2)
    return dataset.get_expectation_suite()
```



### Validating Training Dataset
During retrieval of historical features, `validation_reference` can be passed as a parameter to methods `.to_df(validation_reference=...)` or `.to_arrow(validation_reference=...)` of RetrievalJob.
If parameter is provided Feast will run validation once dataset is materialized. In case if validation successful materialized dataset is returned.
Otherwise, `feast.dqm.errors.ValidationFailed` exception would be raised. It will consist of all details for expectations that didn't pass.

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
