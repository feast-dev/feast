# This is an example feature definition file

from datetime import timedelta

import pandas as pd

from feast import (
    Entity,
    FeatureService,
    FeatureView,
    Field,
    FileSource,
    Project,
    PushSource,
    RequestSource,
)
from feast.feature_logging import LoggingConfig
from feast.infra.offline_stores.file_source import FileLoggingDestination
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64, Int64

# Define a project for the feature repo
project = Project(name="%PROJECT_NAME%", description="A project for driver statistics")

# Define an entity for the driver. You can think of an entity as a primary key used to
# fetch features.
driver = Entity(name="driver", join_keys=["driver_id"])

# Read data from parquet files. Parquet is convenient for local development mode. For
# production, you can use your favorite DWH, such as BigQuery. See Feast documentation
# for more info.
driver_stats_source = FileSource(
    name="driver_hourly_stats_source",
    path="%PARQUET_PATH%",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

# Our parquet files contain sample data that includes a driver_id column, timestamps and
# three feature column. Here we define a Feature View that will allow us to serve this
# data to our model online.
driver_stats_fv = FeatureView(
    # The unique name of this feature view. Two feature views in a single
    # project cannot have the same name
    name="driver_hourly_stats",
    entities=[driver],
    ttl=timedelta(days=1),
    # The list of features defined below act as a schema to both define features
    # for both materialization of features into a store, and are used as references
    # during retrieval for building a training dataset or serving features
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64, description="Average daily trips"),
    ],
    online=True,
    source=driver_stats_source,
    # Tags are user defined key/value pairs that are attached to each
    # feature view
    tags={"team": "driver_performance"},
)

# Define a request data source which encodes features / information only
# available at request time (e.g. part of the user initiated HTTP request)
input_request = RequestSource(
    name="vals_to_add",
    schema=[
        Field(name="val_to_add", dtype=Int64),
        Field(name="val_to_add_2", dtype=Int64),
    ],
)


# Define an on demand feature view which can generate new features based on
# existing feature views and RequestSource features
@on_demand_feature_view(
    sources=[driver_stats_fv, input_request],
    schema=[
        Field(name="conv_rate_plus_val1", dtype=Float64),
        Field(name="conv_rate_plus_val2", dtype=Float64),
    ],
)
def transformed_conv_rate(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["conv_rate_plus_val1"] = inputs["conv_rate"] + inputs["val_to_add"]
    df["conv_rate_plus_val2"] = inputs["conv_rate"] + inputs["val_to_add_2"]
    return df


# This groups features into a model version
driver_activity_v1 = FeatureService(
    name="driver_activity_v1",
    features=[
        driver_stats_fv[["conv_rate"]],  # Sub-selects a feature from a feature view
        transformed_conv_rate,  # Selects all features from the feature view
    ],
    logging_config=LoggingConfig(
        destination=FileLoggingDestination(path="%LOGGING_PATH%")
    ),
)
driver_activity_v2 = FeatureService(
    name="driver_activity_v2", features=[driver_stats_fv, transformed_conv_rate]
)

# Defines a way to push data (to be available offline, online or both) into Feast.
driver_stats_push_source = PushSource(
    name="driver_stats_push_source",
    batch_source=driver_stats_source,
)

# Defines a slightly modified version of the feature view from above, where the source
# has been changed to the push source. This allows fresh features to be directly pushed
# to the online store for this feature view.
driver_stats_fresh_fv = FeatureView(
    name="driver_hourly_stats_fresh",
    entities=[driver],
    ttl=timedelta(days=1),
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64),
    ],
    online=True,
    source=driver_stats_push_source,  # Changed from above
    tags={"team": "driver_performance"},
)


# Define an on demand feature view which can generate new features based on
# existing feature views and RequestSource features
@on_demand_feature_view(
    sources=[driver_stats_fresh_fv, input_request],  # relies on fresh version of FV
    schema=[
        Field(name="conv_rate_plus_val1", dtype=Float64),
        Field(name="conv_rate_plus_val2", dtype=Float64),
    ],
)
def transformed_conv_rate_fresh(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["conv_rate_plus_val1"] = inputs["conv_rate"] + inputs["val_to_add"]
    df["conv_rate_plus_val2"] = inputs["conv_rate"] + inputs["val_to_add_2"]
    return df


# Example of tiled transformation for streaming feature engineering
# This demonstrates Chronon-style tiling for efficient temporal processing
try:
    from feast.transformation import tiled_transformation
    from feast.aggregation import Aggregation
    
    @tiled_transformation(
        sources=[driver_stats_fv],                    # Source feature views for DAG construction
        schema=[                                      # Output schema for UI rendering
            Field(name="rolling_avg_trips", dtype=Float64),
            Field(name="cumulative_trips", dtype=Int64),
            Field(name="trip_velocity", dtype=Float64),
        ],
        tile_size=timedelta(hours=1),                 # Process data in 1-hour tiles
        window_size=timedelta(minutes=30),            # Window size for aggregations within tiles
        overlap=timedelta(minutes=5),                 # 5-minute overlap between tiles
        aggregations=[                                # Feast Aggregation objects
            Aggregation(column="avg_daily_trips", function="sum", time_window=timedelta(minutes=30)),
            Aggregation(column="avg_daily_trips", function="mean", time_window=timedelta(minutes=30)),
        ],
        max_tiles_in_memory=10,                      # Memory management
        enable_late_data_handling=True,              # Handle late-arriving data
    )
    def driver_tiled_features(df: pd.DataFrame) -> pd.DataFrame:
        """
        Tiled transformation for efficient streaming driver feature processing.
        
        This transformation processes driver statistics in temporal tiles,
        enabling efficient computation of rolling averages and cumulative metrics
        for streaming scenarios.
        """
        # Calculate rolling features within each tile
        df = df.sort_values('event_timestamp')
        df['rolling_avg_trips'] = df['avg_daily_trips'].rolling(window=3, min_periods=1).mean()
        
        # Calculate cumulative features within tile
        df['cumulative_trips'] = df['avg_daily_trips'].cumsum()
        
        # Calculate trip velocity (trips per time unit)
        time_diff = df['event_timestamp'].diff().dt.total_seconds() / 3600  # hours
        df['trip_velocity'] = df['avg_daily_trips'] / (time_diff.fillna(1) + 0.1)  # avoid division by zero
        
        return df

    # Note: In a streaming scenario, this would be used with StreamFeatureView:
    # stream_fv = StreamFeatureView(
    #     name="driver_tiled_features",
    #     feature_transformation=driver_tiled_features,
    #     source=stream_source,                     # Kafka or other streaming source
    #     mode="spark",                             # ComputeEngine mode specified here
    #     entities=[driver],
    #     aggregations=driver_tiled_features.aggregations  # Can reuse aggregations
    # )
    
except ImportError:
    # Tiled transformation not available - skip this example
    driver_tiled_features = None


driver_activity_v3 = FeatureService(
    name="driver_activity_v3",
    features=[driver_stats_fresh_fv, transformed_conv_rate_fresh],
)
