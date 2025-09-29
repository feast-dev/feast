# Feature Transformation

A *feature transformation* is a function that takes some set of input data and
returns some set of output data. Feature transformations can happen on either raw data or derived data.

## Feature Transformation Engines
Feature transformations can be executed by three types of "transformation engines":

1. The Feast Feature Server
2. An Offline Store (e.g., Snowflake, BigQuery, DuckDB, Spark, etc.)
3. [A Compute Engine](../../reference/compute-engine/README.md)

The three transformation engines are coupled with the [communication pattern used for writes](write-patterns.md).

Importantly, this implies that different feature transformation code may be 
used under different transformation engines, so understanding the tradeoffs of 
when to use which transformation engine/communication pattern is extremely critical to 
the success of your implementation.

In general, we recommend transformation engines and network calls to be chosen by aligning it with what is most 
appropriate for the data producer, feature/model usage, and overall product.


## API
### feature_transformation
`feature_transformation` or `udf` are the core APIs for defining feature transformations in Feast. They allow you to specify custom logic that can be applied to the data during materialization or retrieval. Examples include:

```python
def remove_extra_spaces(df: DataFrame) -> DataFrame:
    df['name'] = df['name'].str.replace('\s+', ' ')
    return df

spark_transformation = SparkTransformation(
    mode=TransformationMode.SPARK,
    udf=remove_extra_spaces,
    udf_string="remove extra spaces",
)
feature_view = FeatureView(
    feature_transformation=spark_transformation,
    ...
)
```
OR
```python
spark_transformation = Transformation(
    mode=TransformationMode.SPARK_SQL,
    udf=remove_extra_spaces_sql,
    udf_string="remove extra spaces sql",
)
feature_view = FeatureView(
    feature_transformation=spark_transformation,
    ...
)
```
OR
```python
@transformation(mode=TransformationMode.SPARK)
def remove_extra_spaces_udf(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(name=df['name'].str.replace('\s+', ' '))

feature_view = FeatureView(
    feature_transformation=remove_extra_spaces_udf,
    ...
)
```

### Aggregation
Aggregation is builtin API for defining batch or streamable aggregations on data. It allows you to specify how to aggregate data over a time window, such as calculating the average or sum of a feature over a specified period. Examples include:
```python
from feast import Aggregation
feature_view = FeatureView(
    aggregations=[
        Aggregation(
            column="amount",
            function="sum"
        )
        Aggregation(
            column="amount",
            function="avg",
            time_window="1h"
        ),
    ]
    ...
)
```

### Filter
ttl: They amount of time that the features will be available for materialization or retrieval. The entity rows' timestamp higher that the current time minus the ttl will be used to filter the features. This is useful for ensuring that only recent data is used in feature calculations. Examples include:

```python
feature_view = FeatureView(
    ttl="1d",  # Features will be available for 1 day
    ...
)
```

### Join
Feast can join multiple feature views together to create a composite feature view. This allows you to combine features from different sources or views into a single view. Examples include:
```python
feature_view = FeatureView(
    name="composite_feature_view",
    entities=["entity_id"],
    source=[
        FeatureView(
            name="feature_view_1",
            features=["feature_1", "feature_2"],
            ...
        ),
        FeatureView(
            name="feature_view_2",
            features=["feature_3", "feature_4"],
            ...
        )
    ]
    ...
)
```
The underlying implementation of the join is an inner join by default, and join key is the entity id.

### Tiling

Tiling is a transformation pattern designed for efficient streaming feature engineering, inspired by Chronon's tiled architecture. It divides time-series data into manageable chunks (tiles) for processing temporal aggregations and derived features efficiently. This is particularly useful for:

- Temporal aggregations over sliding windows
- Chaining features across different time horizons  
- Handling late-arriving data in streaming scenarios
- Memory-efficient processing of large time-series data

Examples include:

```python
from feast.transformation import tiled_transformation
from datetime import timedelta

@tiled_transformation(
    tile_size=timedelta(hours=1),  # Process data in 1-hour tiles
    mode="pandas",                 # Use pandas processing mode
    overlap=timedelta(minutes=5),  # 5-minute overlap between tiles
    aggregation_functions=[
        lambda df: df.groupby('entity_id').agg({
            'transaction_amount': ['sum', 'mean', 'count']
        }).reset_index()
    ]
)
def hourly_transaction_features(df: pd.DataFrame) -> pd.DataFrame:
    """Transform transaction data within each hour tile."""
    return df.assign(
        rolling_avg=df['transaction_amount'].rolling(window=10).mean(),
        cumulative_amount=df['transaction_amount'].cumsum()
    )

# Usage in StreamFeatureView
stream_fv = StreamFeatureView(
    name="transaction_features",
    feature_transformation=hourly_transaction_features,
    source=kafka_source,
    ...
)
```

**Chaining Features Example:**
```python
@tiled_transformation(
    tile_size=timedelta(hours=1),
    mode="pandas",
    chaining_functions=[
        # Chain cumulative sums across tiles for continuity
        lambda prev_df, curr_df: curr_df.assign(
            global_cumsum=curr_df['amount'] + (prev_df['global_cumsum'].max() if not prev_df.empty else 0)
        )
    ]
)
def chained_cumulative_features(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        local_cumsum=df['amount'].cumsum()
    )
```

**Configuration Options:**
- `tile_size`: Duration of each time tile (e.g., `timedelta(hours=1)`)
- `mode`: Processing mode - currently supports `"pandas"`
- `overlap`: Optional overlap between tiles for continuity
- `max_tiles_in_memory`: Maximum number of tiles to keep in memory (default: 10)
- `enable_late_data_handling`: Whether to handle late-arriving data (default: True)
- `aggregation_functions`: Functions to apply within each tile
- `chaining_functions`: Functions to chain results across tiles for derived features