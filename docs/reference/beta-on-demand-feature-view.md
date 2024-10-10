# \[Beta] On demand feature view

**Warning**: This is an experimental feature. To our knowledge, this is stable, but there are still rough edges in the experience. Contributions are welcome!

## Overview

On Demand Feature Views (ODFVs) allow data scientists to use existing features and request-time data (features only 
available at request time) to transform and create new features. Users define Python transformation logic which is 
executed during both historical retrieval and online retrieval. Additionally, ODFVs provide flexibility in 
applying transformations either during data ingestion (at write time) or during feature retrieval (at read time), 
controlled via the `write_to_online_store` parameter.

By setting `write_to_online_store=True`, transformations are applied during data ingestion, and the transformed 
features are stored in the online store. This can improve online feature retrieval performance by reducing computation 
during reads. Conversely, if `write_to_online_store=False` (the default if omitted), transformations are applied during 
feature retrieval.

### Why use on demand feature views?

This enables data scientists to easily impact the online feature retrieval path. For example, a data scientist could

1. Call `get_historical_features` to generate a training dataframe
2. Iterate in notebook on feature engineering in Pandas/Python
3. Copy transformation logic into ODFVs and commit to a development branch of the feature repository
4. Verify with `get_historical_features` (on a small dataset) that the transformation gives expected output over historical data
5. Decide whether to apply the transformation on writes or on reads by setting the `write_to_online_store` parameter accordingly.
6. Verify with `get_online_features` on dev branch that the transformation correctly outputs online features
7. Submit a pull request to the staging / prod branches which impact production traffic

## CLI

There are new CLI commands:

* `feast on-demand-feature-views list` lists all registered on demand feature view after `feast apply` is run
* `feast on-demand-feature-views describe [NAME]` describes the definition of an on demand feature view

## Example

See [https://github.com/feast-dev/on-demand-feature-views-demo](https://github.com/feast-dev/on-demand-feature-views-demo) for an example on how to use on demand feature views.

### **Registering transformations**

On Demand Transformations support transformations using Pandas and native Python. Note, Native Python is much faster 
but not yet tested for offline retrieval.

When defining an ODFV, you can control when the transformation is applied using the write_to_online_store parameter:

- `write_to_online_store=True`: The transformation is applied during data ingestion (on write), and the transformed features are stored in the online store.
- `write_to_online_store=False` (default when omitted): The transformation is applied during feature retrieval (on read).

We register `RequestSource` inputs and the transform in `on_demand_feature_view`:

## Example of an On Demand Transformation on Read

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

## Example of an On Demand Transformation on Write 

```python
from feast import Field, on_demand_feature_view
from feast.types import Float64
import pandas as pd

# Existing Feature View
driver_hourly_stats_view = ...

# Define an ODFV without RequestSource
@on_demand_feature_view(
    sources=[driver_hourly_stats_view],
    schema=[
        Field(name='conv_rate_adjusted', dtype=Float64),
    ],
    mode="pandas",
    write_to_online_store=True,  # Apply transformation during write time
)
def transformed_conv_rate(features_df: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df['conv_rate_adjusted'] = features_df['conv_rate'] * 1.1  # Adjust conv_rate by 10%
    return df
```
Then to ingest the data with the new feature view make sure to include all of the input features required for the 
transformations:

```python
from feast import FeatureStore
import pandas as pd

store = FeatureStore(repo_path=".")

# Data to ingest
data = pd.DataFrame({
    "driver_id": [1001],
    "event_timestamp": [pd.Timestamp.now()],
    "conv_rate": [0.5],
    "acc_rate": [0.8],
    "avg_daily_trips": [10],
})

# Ingest data to the online store
store.push("driver_hourly_stats_view", data)
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
