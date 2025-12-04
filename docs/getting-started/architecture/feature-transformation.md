# Feature Transformation

A *feature transformation* is a function that takes some set of input data and returns some set of output data. Feature transformations can happen on either raw data or derived data. Feast provides a unified transformation system that allows you to define transformations once and apply them across different execution contexts.

## Unified Transformation System

Feast's unified transformation system centers around the `@transformation` decorator, which provides a single, consistent API for defining feature transformations. This decorator supports multiple execution modes, timing controls, and automatic feature view creation.

### Key Benefits

- **Single API**: Define transformations once using the `@transformation` decorator
- **Multiple Modes**: Support for Python, Pandas, SQL, Spark, Ray, and Substrait transformations
- **Execution Timing Control**: Choose when transformations run (on read, on write, batch, streaming)
- **Training-Serving Consistency**: Dual registration ensures the same transformation logic is used for training and serving
- **Automatic Feature View Creation**: Enhanced decorator can automatically create FeatureViews when provided with additional parameters

## Transformation Execution

Feature transformations can be executed by three types of "transformation engines":

1. **The Feast Feature Server**: Executes transformations during online feature retrieval
2. **An Offline Store**: Executes transformations during historical feature retrieval (e.g., Snowflake, BigQuery, DuckDB, Spark)
3. **[A Compute Engine](../../reference/compute-engine/README.md)**: Executes transformations during batch processing or materialization

The choice of execution engine depends on the transformation timing (`when` parameter) and mode (`mode` parameter).

## The @transformation Decorator

The `@transformation` decorator is the primary API for defining feature transformations in Feast. It provides both backward compatibility with existing transformation patterns and new enhanced capabilities.

### Basic Usage (Backward Compatible)

```python
from feast.transformation import transformation, TransformationMode

@transformation(mode=TransformationMode.PANDAS)
def remove_extra_spaces(df: pd.DataFrame) -> pd.DataFrame:
    """Remove extra spaces from name column."""
    return df.assign(name=df['name'].str.replace(r'\s+', ' ', regex=True))

# Use in a FeatureView
feature_view = FeatureView(
    name="processed_drivers",
    entities=[driver_entity],
    source=driver_source,
    feature_transformation=remove_extra_spaces,
    ...
)
```

### Enhanced Usage (New Capabilities)

The decorator supports additional parameters that enable automatic FeatureView creation and advanced execution control:

```python
from feast.transformation import transformation, TransformationTiming

@transformation(
    mode="pandas",
    when="on_read",  # Execute during feature retrieval
    online=True,     # Enable dual registration for training-serving consistency
    sources=[driver_hourly_stats_view],
    schema=[
        Field(name="conv_rate_adjusted", dtype=Float64),
        Field(name="efficiency_score", dtype=Float64)
    ],
    entities=[driver_entity],
    name="driver_metrics_enhanced",
    description="Enhanced driver metrics with efficiency scoring"
)
def enhance_driver_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Enhance driver metrics with additional calculations."""
    result = pd.DataFrame()
    result["conv_rate_adjusted"] = df["conv_rate"] * 1.1
    result["efficiency_score"] = df["conv_rate"] * df["acc_rate"] / df["avg_daily_trips"]
    return result

# This automatically creates:
# 1. A FeatureView for batch/training use
# 2. An OnDemandFeatureView for online serving (when online=True)
```

### Parameters

The `@transformation` decorator supports several key parameters:

- **`mode`**: Transformation execution mode (`pandas`, `python`, `sql`, `spark`, `ray`, `substrait`)
- **`when`**: Execution timing (`on_read`, `on_write`, `batch`, `streaming`)
- **`online`**: Enable dual registration for training-serving consistency
- **`sources`**: Source FeatureViews for automatic feature view creation
- **`schema`**: Output schema when auto-creating feature views
- **`entities`**: Entities for auto-created feature views

## Legacy API (Still Supported)

The existing transformation APIs continue to work alongside the new unified system:

### Using Transformation Objects

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

### Using Generic Transformation Class

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

### Basic Decorator Usage

```python
@transformation(mode=TransformationMode.SPARK)
def remove_extra_spaces_udf(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(name=df['name'].str.replace('\s+', ' '))

feature_view = FeatureView(
    feature_transformation=remove_extra_spaces_udf,
    ...
)
```

## Migration Examples: Old vs New Patterns

### Example 1: Stream Feature View Transformations

**Old Way - Stream Feature View with Transformation**

```python
from feast import StreamFeatureView, Entity, Field
from feast.data_source import KafkaSource
from feast.types import Float64, Int64, String
from feast.transformation.pandas_transformation import PandasTransformation

# Define entities and sources
driver_entity = Entity(name="driver", join_keys=["driver_id"])

kafka_source = KafkaSource(
    name="driver_events",
    kafka_bootstrap_servers="localhost:9092",
    topic="driver_events",
    timestamp_field="event_timestamp",
    batch_source=FileSource(path="driver_events.parquet")
)

# Define transformation function
def calculate_driver_score(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate driver performance score."""
    df["driver_score"] = df["conv_rate"] * df["acc_rate"] * 100
    df["performance_tier"] = pd.cut(
        df["driver_score"],
        bins=[0, 30, 70, 100],
        labels=["low", "medium", "high"]
    )
    return df

# Create transformation object
driver_transformation = PandasTransformation(
    udf=calculate_driver_score,
    udf_string="calculate driver score"
)

# Create Stream Feature View
driver_stream_fv = StreamFeatureView(
    name="driver_stream_features",
    entities=[driver_entity],
    schema=[
        Field(name="conv_rate", dtype=Float64),
        Field(name="acc_rate", dtype=Float64),
        Field(name="driver_score", dtype=Float64),
        Field(name="performance_tier", dtype=String),
    ],
    source=kafka_source,
    feature_transformation=driver_transformation,
)
```

**New Way - Unified Transformation with Streaming**

```python
from feast.transformation import transformation

# Define the same transformation with unified decorator
@transformation(
    mode="pandas",
    when="streaming",  # Execute in streaming context
    online=True,       # Enable dual registration
    sources=[kafka_source],
    schema=[
        Field(name="driver_score", dtype=Float64),
        Field(name="performance_tier", dtype=String),
    ],
    entities=[driver_entity],
    name="driver_stream_features",
    description="Real-time driver performance scoring"
)
def calculate_driver_score_unified(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate driver performance score - unified approach."""
    result = pd.DataFrame()
    result["driver_score"] = df["conv_rate"] * df["acc_rate"] * 100
    result["performance_tier"] = pd.cut(
        result["driver_score"],
        bins=[0, 30, 70, 100],
        labels=["low", "medium", "high"]
    )
    return result

# Automatically creates both StreamFeatureView and OnDemandFeatureView
```

### Example 2: On Demand Feature View Transformations

**Old Way - Separate ODFV Definition**

```python
from feast.on_demand_feature_view import on_demand_feature_view
from feast import RequestSource, Field
from feast.types import Float64, Int64

# Define request source for real-time data
request_source = RequestSource(
    name="driver_request",
    schema=[
        Field(name="current_temp", dtype=Float64),
        Field(name="time_of_day", dtype=Int64),
    ]
)

# Define ODFV with transformation
@on_demand_feature_view(
    sources=[driver_hourly_stats_view, request_source],
    schema=[
        Field(name="weather_adjusted_score", dtype=Float64),
        Field(name="time_adjusted_conv_rate", dtype=Float64),
    ],
    mode="pandas",
    write_to_online_store=True  # Apply on write
)
def weather_adjusted_features(features_df: pd.DataFrame) -> pd.DataFrame:
    """Adjust features based on weather and time."""
    df = pd.DataFrame()

    # Weather adjustment
    weather_factor = 1.0 + (features_df["current_temp"] - 70) / 100
    df["weather_adjusted_score"] = features_df["conv_rate"] * weather_factor

    # Time of day adjustment
    time_factor = np.where(
        (features_df["time_of_day"] >= 6) & (features_df["time_of_day"] <= 18),
        1.1,  # Daytime boost
        0.9   # Nighttime reduction
    )
    df["time_adjusted_conv_rate"] = features_df["conv_rate"] * time_factor

    return df
```

**New Way - Unified Transformation**

```python
from feast.transformation import transformation

@transformation(
    mode="pandas",
    when="on_write",   # Apply during data ingestion
    online=True,       # Enable dual registration
    sources=[driver_hourly_stats_view, request_source],
    schema=[
        Field(name="weather_adjusted_score", dtype=Float64),
        Field(name="time_adjusted_conv_rate", dtype=Float64),
    ],
    entities=[driver_entity],
    name="contextual_driver_features",
    description="Driver features adjusted for weather and time context"
)
def weather_adjusted_features_unified(df: pd.DataFrame) -> pd.DataFrame:
    """Adjust features based on weather and time - unified approach."""
    result = pd.DataFrame()

    # Weather adjustment
    weather_factor = 1.0 + (df["current_temp"] - 70) / 100
    result["weather_adjusted_score"] = df["conv_rate"] * weather_factor

    # Time of day adjustment
    import numpy as np
    time_factor = np.where(
        (df["time_of_day"] >= 6) & (df["time_of_day"] <= 18),
        1.1,  # Daytime boost
        0.9   # Nighttime reduction
    )
    result["time_adjusted_conv_rate"] = df["conv_rate"] * time_factor

    return result

# This creates:
# 1. A FeatureView for batch processing
# 2. An OnDemandFeatureView for online serving
# Both use the same transformation logic!
```

### Example 3: Training-Serving Consistency

**Old Way - Duplicate Logic**

```python
# Training pipeline transformation
def training_feature_engineering(df: pd.DataFrame) -> pd.DataFrame:
    """Feature engineering for training."""
    df["interaction_score"] = df["conv_rate"] * df["acc_rate"]
    df["normalized_trips"] = df["avg_daily_trips"] / df["avg_daily_trips"].max()
    return df

# Separate serving transformation (risk of skew!)
@on_demand_feature_view(
    sources=[driver_stats_view],
    schema=[
        Field(name="interaction_score", dtype=Float64),
        Field(name="normalized_trips", dtype=Float64),
    ]
)
def serving_feature_engineering(features_df: pd.DataFrame) -> pd.DataFrame:
    """Feature engineering for serving - DUPLICATE LOGIC!"""
    df = pd.DataFrame()
    df["interaction_score"] = features_df["conv_rate"] * features_df["acc_rate"]
    df["normalized_trips"] = features_df["avg_daily_trips"] / 100  # Hardcoded max!
    return df
```

**New Way - Single Source of Truth**

```python
@transformation(
    mode="pandas",
    when="on_read",    # Fresh calculations
    online=True,       # Dual registration ensures consistency
    sources=[driver_stats_view],
    schema=[
        Field(name="interaction_score", dtype=Float64),
        Field(name="normalized_trips", dtype=Float64),
    ],
    entities=[driver_entity],
    name="consistent_driver_features"
)
def unified_feature_engineering(df: pd.DataFrame) -> pd.DataFrame:
    """Single transformation for both training and serving."""
    result = pd.DataFrame()
    result["interaction_score"] = df["conv_rate"] * df["acc_rate"]
    result["normalized_trips"] = df["avg_daily_trips"] / df["avg_daily_trips"].max()
    return result

# Same logic used for:
# - Historical feature retrieval (training)
# - Online feature serving (inference)
# - Batch materialization
```

### Benefits of the New Approach

1. **Reduced Code Duplication**: Single transformation definition vs multiple implementations
2. **Training-Serving Consistency**: Automatic dual registration eliminates skew
3. **Simplified Management**: One decorator handles all transformation contexts
4. **Better Maintainability**: Changes only need to be made in one place
5. **Flexible Execution**: Easy to change timing (`when` parameter) without rewriting logic

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