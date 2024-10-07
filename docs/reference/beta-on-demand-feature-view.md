# \[Beta] On demand feature view

**Warning**: This is an experimental feature. To our knowledge, this is stable, but there are still rough edges in the experience. Contributions are welcome!

## Overview

On demand feature views allows data scientists to use existing features and request time data (features only available 
at request time) to transform and create new features at the time the data is read from the online store. Users define 
python transformation logic which is executed in both historical retrieval and online retrieval paths.

### Why use on demand feature views?

This enables data scientists to easily impact the online feature retrieval path. For example, a data scientist could

1. Call `get_historical_features` to generate a training dataframe
2. Iterate in notebook on feature engineering in Pandas/Python
3. Copy transformation logic into on demand feature views and commit to a dev branch of the feature repository
4. Verify with `get_historical_features` (on a small dataset) that the transformation gives expected output over historical data
5. Verify with `get_online_features` on dev branch that the transformation correctly outputs online features
6. Submit a pull request to the staging / prod branches which impact production traffic

## CLI

There are new CLI commands:

* `feast on-demand-feature-views list` lists all registered on demand feature view after `feast apply` is run
* `feast on-demand-feature-views describe [NAME]` describes the definition of an on demand feature view

## Example

See [https://github.com/feast-dev/on-demand-feature-views-demo](https://github.com/feast-dev/on-demand-feature-views-demo) for an example on how to use on demand feature views.

### **Registering transformations**

On Demand Transformations support transformations using Pandas and native Python. Note, Native Python is much faster but not yet tested for offline retrieval.

We register `RequestSource` inputs and the transform in `on_demand_feature_view`:

```python
from feast import Field, RequestSource
from feast.types import Float64, Int64
from typing import Any, Dict
import pandas as pd

# Define a request data source which encodes features / information only
# available at request time (e.g. part of the user initiated HTTP request)
input_request = RequestSource(
    name="vals_to_add",
    schema=[
        Field(name='val_to_add', dtype=Int64),
        Field(name='val_to_add_2', dtype=Int64)
    ]
)

# Use the input data and feature view features to create new features Pandas mode
@on_demand_feature_view(
   sources=[
       driver_hourly_stats_view,
       input_request
   ],
   schema=[
     Field(name='conv_rate_plus_val1', dtype=Float64),
     Field(name='conv_rate_plus_val2', dtype=Float64)
   ],
   mode="pandas",
)
def transformed_conv_rate(features_df: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df['conv_rate_plus_val1'] = (features_df['conv_rate'] + features_df['val_to_add'])
    df['conv_rate_plus_val2'] = (features_df['conv_rate'] + features_df['val_to_add_2'])
    return df

# Use the input data and feature view features to create new features Python mode
@on_demand_feature_view(
    sources=[
        driver_hourly_stats_view,
        input_request
    ],
    schema=[
        Field(name='conv_rate_plus_val1_python', dtype=Float64),
        Field(name='conv_rate_plus_val2_python', dtype=Float64),
    ],
    mode="python",
)
def transformed_conv_rate_python(inputs: Dict[str, Any]) -> Dict[str, Any]:
    output: Dict[str, Any] = {
        "conv_rate_plus_val1_python": [
            conv_rate + val_to_add
            for conv_rate, val_to_add in zip(
                inputs["conv_rate"], inputs["val_to_add"]
            )
        ],
        "conv_rate_plus_val2_python": [
            conv_rate + val_to_add
            for conv_rate, val_to_add in zip(
                inputs["conv_rate"], inputs["val_to_add_2"]
            )
        ]
    }
    return output
```

### **Feature retrieval**

{% hint style="info" %}
The on demand feature view's name is the function name (i.e. `transformed_conv_rate`).
{% endhint %}


#### Offline Features
And then to retrieve historical, we can call this in a feature service or reference individual features:

```python
training_df = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_hourly_stats:avg_daily_trips",
        "transformed_conv_rate:conv_rate_plus_val1",
        "transformed_conv_rate:conv_rate_plus_val2",
    ],
).to_df()

```

#### Online Features

And then to retrieve online, we can call this in a feature service or reference individual features:

```python
entity_rows = [
    {
        "driver_id": 1001,
        "val_to_add": 1,
        "val_to_add_2": 2,
    }
]

online_response = store.get_online_features(
    entity_rows=entity_rows,
    features=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "transformed_conv_rate_python:conv_rate_plus_val1_python",
        "transformed_conv_rate_python:conv_rate_plus_val2_python",
    ],
).to_dict()
```
