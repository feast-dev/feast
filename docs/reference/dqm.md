# Data Quality Monitoring

Data Quality Monitoring (DQM) is a Feast module aimed to help users to validate their data with the user-curated set of rules.
Validation could be applied during:
* Historical retrieval (training dataset generation)
* [planned] Writing features into an online store
* [planned] Reading features from an online store

Its goal is to address several complex data problems, namely:
* Data Consistency - new training dataset could be significantly different from previous, which will require change in model architecture.
* Issues/bugs in upstream pipeline - bug in upstream could case invalid values to overwrite existing valid values in an online store.
* Training/serving skew - distribution shift could significantly decrease performance of the model.

> By “monitoring data quality” we understand verifying that the characteristics of tested dataset (we call it dataset's profile) are "equivalent" to the characteristics of reference dataset.
> Eg, data currently passed to the model hasn’t changed significantly since the model was trained and expectations implicitly made by ML algorithm during training are still met.
> How exactly profiles equivalency should be measured is up to the user. 

### Overview

Validation process consists of the next steps:
1. User prepares reference dataset (currently only [saved dataset](../getting-started/concepts/dataset.md) from historical retrieval is supported).
2. User defines profiler function, which should produce profile by given dataset.
3. Validation of tested dataset is performed with reference dataset and profiler provided as parameters.

### Preparations
Feast with DQM support can be installed via
```shell
pip install 'feast[dqm]'
```

### Dataset profile
Currently, Feast supports only [great expectation's](https://greatexpectations.io/) [ExpectationSuite](https://legacy.docs.greatexpectations.io/en/latest/autoapi/great_expectations/core/expectation_suite/index.html#great_expectations.core.expectation_suite.ExpectationSuite)
as dataset's profile. Hence, user needs to define a function (profiler) that would receive a dataset and return [ExpectationSuite](https://legacy.docs.greatexpectations.io/en/latest/autoapi/great_expectations/core/expectation_suite/index.html#great_expectations.core.expectation_suite.ExpectationSuite).

Either automatic profiler or user selected expectations could be used in profiler function:
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

```python
@ge_profiler
def manual_profiler(dataset: Dataset) -> ExpectationSuite:
    dataset.expect_column_max_to_be_between("column", 1, 2)
    return dataset.get_expectation_suite()
```



### Validating Training Dataset
During retrieval of historical features additional parameter `validation_reference` could be passed.
If this parameter is supplied `get_historical_features` will return `RetrievalJobWithValidation` instead of simple `RetrievalJob`.
Such job will run validation once dataset is materialized (when `.to_df()` or `.to_arrow()` called). In case if validation successful materialized dataset is returned (no change to previous/regular behavior).
Otherwise `feast.dqm.errors.ValidationFailed` exception would be raised. It will consist of all details for expectations that didn't pass.

```python
from feast import FeatureStore

fs = FeatureStore(".")

fs.get_historical_features(
    ...,
    validation_reference=fs
        .get_saved_dataset("my_reference_dataset")
        .as_reference(profiler=manual_profiler)
)
```
