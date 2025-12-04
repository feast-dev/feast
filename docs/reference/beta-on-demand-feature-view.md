# [Beta] On Demand Feature Views

**Warning**: This is an experimental feature. While it is stable to our knowledge, there may still be rough edges in the experience. Contributions are welcome!

## Overview

On Demand Feature Views (ODFVs) allow data scientists to use existing features and request-time data to transform and
create new features. Users define transformation logic that is executed during both historical and online retrieval.
Additionally, ODFVs provide flexibility in applying transformations either during data ingestion (at write time) or
during feature retrieval (at read time), controlled via the `write_to_online_store` parameter.

By setting `write_to_online_store=True`, transformations are applied during data ingestion, and the transformed
features are stored in the online store. This can improve online feature retrieval performance by reducing computation
during reads. Conversely, if `write_to_online_store=False` (the default if omitted), transformations are applied during
feature retrieval.

## Transformation Execution Architecture

Feast provides four distinct execution timings for transformations:

### Execution Contexts

**Python Feature Server** (for real-time, low-latency operations):
- **`ON_READ`**: Execute during feature retrieval
- **`ON_WRITE`**: Execute during data ingestion to feature server

**Compute Engines** (for large-scale processing):
- **`BATCH`**: Execute on Spark/Ray/etc. for scheduled batch processing
- **`STREAMING`**: Execute on Flink/Spark Streaming/etc. for real-time streams

### Feature Storage Patterns

**Stateful Features**: Values stored in online database
- Batch-computed features (`when="batch"`)
- Stream-computed features (`when="streaming"`)
- Feature server materialized features (`when="on_write"`)
- Base features modified on-read

**Stateless Features**: Computed at request time
- Pure request-time computation (`when="on_read"` with only request sources)

### Recommended Patterns

#### 1. Large-Scale Batch Features (Compute Engine)

```python
@transformation(
    mode="spark",
    when="batch",        # Execute on Spark for large datasets
    sources=[driver_hourly_stats_view],
    schema=[Field(name="base_efficiency_score", dtype=Float64)],
    entities=[driver_entity],
    name="driver_base_features"
)
def compute_base_features(df: DataFrame) -> DataFrame:
    """Pre-compute base features on Spark."""
    from pyspark.sql import functions as F
    return df.withColumn("base_efficiency_score",
                        F.col("conv_rate") * F.col("acc_rate"))
# Executes on Spark, results stored in online DB
```

#### 2. Feature Server Materialization (Python)

```python
@transformation(
    mode="pandas",
    when="on_write",     # Execute in Python feature server during ingestion
    sources=[driver_hourly_stats_view],
    schema=[Field(name="quick_score", dtype=Float64)],
    entities=[driver_entity],
    name="driver_quick_features"
)
def compute_quick_features(df: pd.DataFrame) -> pd.DataFrame:
    """Fast computation in feature server."""
    result = pd.DataFrame()
    result["quick_score"] = df["conv_rate"] * 1.1
    return result
# Executes in Python feature server, stores to online DB
```

#### 3. Real-Time Stream Processing (Compute Engine)

```python
@transformation(
    mode="spark",
    when="streaming",    # Execute on Spark Streaming for real-time streams
    sources=[kafka_driver_events],
    schema=[Field(name="real_time_efficiency", dtype=Float64)],
    entities=[driver_entity],
    name="driver_streaming_features"
)
def compute_streaming_features(df: DataFrame) -> DataFrame:
    """Process real-time streams on Spark."""
    from pyspark.sql import functions as F
    return df.withColumn("real_time_efficiency",
                        F.col("events_per_minute") / F.col("trips_per_hour"))
# Executes on Spark Streaming, continuously updates online DB
```

#### 4. On-Read Adjustments (Feature Server)

```python
@transformation(
    mode="python",
    when="on_read",      # Execute in feature server during retrieval
    sources=[driver_base_features_view, request_source],
    schema=[Field(name="context_adjusted_score", dtype=Float64)]
)
def adjust_for_context(inputs: dict) -> dict:
    """Adjust stored features based on request context."""
    base_score = inputs["base_efficiency_score"][0]  # From online DB (stateful)
    context_factor = inputs["time_of_day"] / 24.0     # From request (dynamic)

    return {
        "context_adjusted_score": base_score * context_factor
    }
# Executes in Python feature server, uses stored + request data
```

#### 5. Pure Request-Time Features (Feature Server)

```python
@transformation(
    mode="python",
    when="on_read",      # Execute in feature server during retrieval
    sources=[request_source],  # Only request data (stateless)
    schema=[Field(name="request_derived_feature", dtype=Float64)]
)
def compute_from_request(inputs: dict) -> dict:
    """Compute features purely from request data."""
    return {
        "request_derived_feature": inputs["user_age"] * inputs["session_length"]
    }
# Executes in Python feature server at request time
```

### Benefits of Layered Architecture

1. **Performance**: Core features pre-computed and stored
2. **Flexibility**: Can adjust stored features based on context
3. **Cost Efficiency**: Expensive computations done once during materialization
4. **Real-time Capability**: Request-specific adjustments applied during serving

### Why Use On Demand Feature Views?

ODFVs enable data scientists to easily impact the online feature retrieval path. For example, a data scientist could:

1. Call `get_historical_features` to generate a training dataset.
2. Iterate in a notebook and do your feature engineering using Pandas or native Python.
3. Copy transformation logic into ODFVs and commit to a development branch of the feature repository.
4. Verify with `get_historical_features` (on a small dataset) that the transformation gives the expected output over historical data.
5. Decide whether to apply the transformation on writes or on reads by setting the `write_to_online_store` parameter accordingly.
6. Verify with `get_online_features` on the development branch that the transformation correctly outputs online features.
7. Submit a pull request to the staging or production branches, impacting production traffic.

## Transformation Modes

When defining an ODFV, you can specify the transformation mode using the `mode` parameter. Feast supports the following modes:

- **Pandas Mode (`mode="pandas"`)**: The transformation function takes a Pandas DataFrame as input and returns a Pandas DataFrame as output. This mode is useful for batch transformations over multiple rows.
- **Native Python Mode (`mode="python"`)**: The transformation function uses native Python and can operate on inputs as lists of values or as single dictionaries representing a singleton (single row).

### Singleton Transformations in Native Python Mode

Native Python mode supports transformations on singleton dictionaries by setting `singleton=True`. This allows you to
write transformation functions that operate on a single row at a time, making the code more intuitive and aligning with
how data scientists typically think about data transformations.

## Aggregations

On Demand Feature Views support aggregations that compute aggregate statistics over groups of rows. When using aggregations, data is grouped by entity columns (e.g., `driver_id`) and aggregated before being passed to the transformation function.

**Important**: Aggregations and transformations are mutually exclusive. When aggregations are specified, they replace the transformation function.

### Usage

```python
from feast import Aggregation
from datetime import timedelta

@on_demand_feature_view(
    sources=[driver_hourly_stats_view],
    schema=[
        Field(name="total_trips", dtype=Int64),
        Field(name="avg_rating", dtype=Float64),
    ],
    aggregations=[
        Aggregation(column="trips", function="sum"),
        Aggregation(column="rating", function="mean"),
    ],
)
def driver_aggregated_stats(inputs):
    # No transformation function needed when using aggregations
    pass
```

Aggregated columns are automatically named using the pattern `{function}_{column}` (e.g., `sum_trips`, `mean_rating`).

## Example
See [https://github.com/feast-dev/on-demand-feature-views-demo](https://github.com/feast-dev/on-demand-feature-views-demo) for an example on how to use on demand feature views.


## Registering Transformations

When defining an ODFV, you can control when the transformation is applied using the `write_to_online_store` parameter:

- `write_to_online_store=True`: The transformation is applied during data ingestion (on write), and the transformed features are stored in the online store.
- `write_to_online_store=False` (default): The transformation is applied during feature retrieval (on read).

### Examples

#### Example 1: On Demand Transformation on Read Using Pandas Mode

```python
from feast import Field, RequestSource
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float64, Int64
import pandas as pd

# Define a request data source for request-time features
input_request = RequestSource(
    name="vals_to_add",
    schema=[
        Field(name="val_to_add", dtype=Int64),
        Field(name="val_to_add_2", dtype=Int64),
    ],
)

# Use input data and feature view features to create new features in Pandas mode
@on_demand_feature_view(
    sources=[driver_hourly_stats_view, input_request],
    schema=[
        Field(name="conv_rate_plus_val1", dtype=Float64),
        Field(name="conv_rate_plus_val2", dtype=Float64),
    ],
    mode="pandas",
)
def transformed_conv_rate(features_df: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["conv_rate_plus_val1"] = features_df["conv_rate"] + features_df["val_to_add"]
    df["conv_rate_plus_val2"] = features_df["conv_rate"] + features_df["val_to_add_2"]
    return df
```

#### Example 2: On Demand Transformation on Read Using Native Python Mode (List Inputs)

```python
from feast import Field, on_demand_feature_view
from feast.types import Float64
from typing import Any, Dict

# Use input data and feature view features to create new features in Native Python mode
@on_demand_feature_view(
    sources=[driver_hourly_stats_view, input_request],
    schema=[
        Field(name="conv_rate_plus_val1_python", dtype=Float64),
        Field(name="conv_rate_plus_val2_python", dtype=Float64),
    ],
    mode="python",
)
def transformed_conv_rate_python(inputs: Dict[str, Any]) -> Dict[str, Any]:
    output = {
        "conv_rate_plus_val1_python": [
            conv_rate + val_to_add
            for conv_rate, val_to_add in zip(inputs["conv_rate"], inputs["val_to_add"])
        ],
        "conv_rate_plus_val2_python": [
            conv_rate + val_to_add
            for conv_rate, val_to_add in zip(
                inputs["conv_rate"], inputs["val_to_add_2"]
            )
        ],
    }
    return output
```

#### **New** Example 3: On Demand Transformation on Read Using Native Python Mode (Singleton Input)

```python
from feast import Field, on_demand_feature_view
from feast.types import Float64
from typing import Any, Dict

# Use input data and feature view features to create new features in Native Python mode with singleton input
@on_demand_feature_view(
    sources=[driver_hourly_stats_view, input_request],
    schema=[
        Field(name="conv_rate_plus_acc_singleton", dtype=Float64),
    ],
    mode="python",
    singleton=True,
)
def transformed_conv_rate_singleton(inputs: Dict[str, Any]) -> Dict[str, Any]:
    output = {
        "conv_rate_plus_acc_singleton": inputs["conv_rate"] + inputs["acc_rate"]
    }
    return output
```

In this example, `inputs` is a dictionary representing a single row, and the transformation function returns a dictionary of transformed features for that single row. This approach is more intuitive and aligns with how data scientists typically process single data records.

#### Example 4: On Demand Transformation on Write Using Pandas Mode

```python
from feast import Field, on_demand_feature_view
from feast.types import Float64
import pandas as pd

# Existing Feature View
driver_hourly_stats_view = ...

# Define an ODFV applying transformation during write time
@on_demand_feature_view(
    sources=[driver_hourly_stats_view],
    schema=[
        Field(name="conv_rate_adjusted", dtype=Float64),
    ],
    mode="pandas",
    write_to_online_store=True,  # Apply transformation during write time
)
def transformed_conv_rate(features_df: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["conv_rate_adjusted"] = features_df["conv_rate"] * 1.1  # Adjust conv_rate by 10%
    return df
```

To ingest data with the new feature view, include all input features required for the transformations:

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

### Feature Retrieval

{% hint style="info" %}
**Note**: The name of the on demand feature view is the function name (e.g., `transformed_conv_rate`).
{% endhint %}

#### Offline Features

Retrieve historical features by referencing individual features or using a feature service:

```python
training_df = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_hourly_stats:avg_daily_trips",
        "transformed_conv_rate:conv_rate_plus_val1",
        "transformed_conv_rate:conv_rate_plus_val2",
        "transformed_conv_rate_singleton:conv_rate_plus_acc_singleton",
    ],
).to_df()
```

#### Online Features

Retrieve online features by referencing individual features or using a feature service:

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
        "transformed_conv_rate_singleton:conv_rate_plus_acc_singleton",
    ],
).to_dict()
```

### Materializing Pre-transformed Data

In some scenarios, you may have already transformed your data in batch (e.g., using Spark or another batch processing framework) and want to directly materialize the pre-transformed features without applying transformations during ingestion. Feast supports this through the `transform_on_write` parameter.

When using `write_to_online_store=True` with On Demand Feature Views, you can set `transform_on_write=False` to skip transformations during the write operation. This is particularly useful for optimizing performance when working with large pre-transformed datasets.

```python
from feast import FeatureStore
import pandas as pd

store = FeatureStore(repo_path=".")

# Pre-transformed data (transformations already applied)
pre_transformed_data = pd.DataFrame({
    "driver_id": [1001],
    "event_timestamp": [pd.Timestamp.now()],
    "conv_rate": [0.5],
    # Pre-calculated values for the transformed features
    "conv_rate_adjusted": [0.55],  # Already contains the adjusted value
})

# Write to online store, skipping transformations
store.write_to_online_store(
    feature_view_name="transformed_conv_rate",
    df=pre_transformed_data,
    transform_on_write=False  # Skip transformation during write
)
```

This approach allows for a hybrid workflow where you can:
1. Transform data in batch using powerful distributed processing tools
2. Materialize the pre-transformed data without reapplying transformations
3. Still use the Feature Server to execute transformations during API calls when needed

Even when features are materialized with transformations skipped (`transform_on_write=False`), the feature server can still apply transformations during API calls for any missing values or for features that require real-time computation.

## Comparison: @on_demand_feature_view vs @transformation

### Original Approach with @on_demand_feature_view

```python
from feast.on_demand_feature_view import on_demand_feature_view
from feast import Field, RequestSource
from feast.types import Float64
import pandas as pd

# Define request source
request_source = RequestSource(
    name="vals_to_add",
    schema=[Field(name="val_to_add", dtype=Int64)]
)

@on_demand_feature_view(
    sources=[driver_hourly_stats_view, request_source],
    schema=[Field(name="conv_rate_plus_val1", dtype=Float64)],
    mode="pandas",
    write_to_online_store=False  # Transform on read
)
def transformed_conv_rate_original(features_df: pd.DataFrame) -> pd.DataFrame:
    """Original ODFV approach."""
    df = pd.DataFrame()
    df["conv_rate_plus_val1"] = features_df["conv_rate"] + features_df["val_to_add"]
    return df
```

### New Unified Approach with @transformation

```python
from feast.transformation import transformation
from feast import Field, RequestSource
from feast.types import Float64
import pandas as pd

@transformation(
    mode="pandas",
    when="on_read",  # Equivalent to write_to_online_store=False
    sources=[driver_hourly_stats_view, request_source],
    schema=[Field(name="conv_rate_plus_val1", dtype=Float64)],
    name="transformed_conv_rate_unified"
)
def transformed_conv_rate_unified(df: pd.DataFrame) -> pd.DataFrame:
    """New unified approach."""
    result = pd.DataFrame()
    result["conv_rate_plus_val1"] = df["conv_rate"] + df["val_to_add"]
    return result
```

### Parameter Mapping

| @on_demand_feature_view | @transformation | Description |
|------------------------|-----------------|-------------|
| `mode="pandas"` | `mode="pandas"` | Use Pandas for transformation |
| `mode="python"` | `mode="python"` | Use native Python for transformation |
| `write_to_online_store=False` | `when="on_read"` | Transform during feature retrieval |
| `write_to_online_store=True` | `when="on_write"` | Transform during data ingestion |
| `sources=[...]` | `sources=[...]` | Source feature views and request sources |
| `schema=[...]` | `schema=[...]` | Output feature schema |
| N/A | `online=True` | Enable dual registration (FeatureView + ODFV) |
| N/A | `entities=[...]` | Entities for auto-created feature views |

### Benefits of the Unified Approach

1. **Consistent API**: Same decorator works for regular FeatureViews, StreamFeatureViews, and OnDemandFeatureViews
2. **Training-Serving Consistency**: `online=True` automatically creates both training and serving versions
3. **Flexible Timing**: Easy to switch between `on_read` and `on_write` execution
4. **Future-Proof**: New transformation modes and execution engines are automatically supported

### When to Use Each Approach

**Use @on_demand_feature_view when:**
- You only need ODFVs (no training/batch use case)
- You prefer explicit, specialized APIs
- You're working with existing ODFV patterns

**Use @transformation when:**
- You need the same transformation for training and serving
- You want a unified transformation system
- You plan to change execution timing or modes
- You're starting new feature development

## CLI Commands
There are new CLI commands to manage on demand feature views:

feast on-demand-feature-views list: Lists all registered on demand feature views after feast apply is run.
feast on-demand-feature-views describe [NAME]: Describes the definition of an on demand feature view.


