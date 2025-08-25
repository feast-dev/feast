# This is an example feature definition file

from datetime import timedelta

from feast import Entity, Feature, FeatureView, Field, FileSource, FeatureService, RequestSource, ValueType
from feast.feature_logging import LoggingConfig
from feast.infra.offline_stores.file_source import FileLoggingDestination
from feast.types import Float32, Float64, Int64, PrimitiveFeastType
from feast.on_demand_feature_view import on_demand_feature_view
import pandas as pd

# Read data from parquet files. Parquet is convenient for local development mode. For
# production, you can use your favorite DWH, such as BigQuery. See Feast documentation
# for more info.
driver_hourly_stats = FileSource(
    path="driver_stats.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

# Define an entity for the driver. You can think of an entity as a primary key used to
# fetch features.
driver = Entity(name="driver_id", value_type=ValueType.INT64, description="driver id")

# Our parquet files contain sample data that includes a driver_id column, timestamps and
# three feature column. Here we define a Feature View that will allow us to serve this
# data to our model online.
driver_hourly_stats_view = FeatureView(
    name="driver_hourly_stats",
    entities=[driver],
    ttl=timedelta(seconds=86400 * 365 * 10),
    schema=[
        Field(name="driver_id", dtype=Int64),
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64),
    ],
    online=True,
    source=driver_hourly_stats,
    tags={},
)

driver_stats_fs = FeatureService(
    name="test_service",
    features=[driver_hourly_stats_view],
    logging_config=LoggingConfig(destination=FileLoggingDestination(path=""))
)


# Define a request data source which encodes features / information only
# available at request time (e.g. part of the user initiated HTTP request)
input_request = RequestSource(
    name="vals_to_add",
    schema=[
        Field(name="val_to_add", dtype=PrimitiveFeastType.INT64),
        Field(name="val_to_add_2", dtype=PrimitiveFeastType.INT64),
    ]
)

# Use the input data and feature view features to create new features
@on_demand_feature_view(
   sources=[
       driver_hourly_stats_view,
       input_request
   ],
   schema=[
     Field(name='conv_rate_plus_val1', dtype=Float64),
     Field(name='conv_rate_plus_val2', dtype=Float64)
   ]
)
def transformed_conv_rate(features_df: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df['conv_rate_plus_val1'] = (features_df['conv_rate'] + features_df['val_to_add'])
    df['conv_rate_plus_val2'] = (features_df['conv_rate'] + features_df['val_to_add_2'])
    return df
