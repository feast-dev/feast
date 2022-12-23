# This is an example feature definition file

import pandas as pd
from datetime import datetime, timedelta

from feast import Entity, FeatureService, FeatureView, Field, FileSource, RequestSource
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64, Int64, UnixTimestamp

# Read data from parquet files. Parquet is convenient for local development mode. For
# production, you can use your favorite DWH, such as BigQuery. See Feast documentation
# for more info.
driver_hourly_stats = FileSource(
    name="driver_hourly_stats_source",
    path="/Users/francisco.arceo/github/feast/examples/quickstart/feature_repo/data/driver_stats.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

# Define an entity for the driver. You can think of entity as a primary key used to
# fetch features.
driver = Entity(name="driver", join_keys=["driver_id"])

# Our parquet files contain sample data that includes a driver_id column, timestamps and
# three feature column. Here we define a Feature View that will allow us to serve this
# data to our model online.
driver_hourly_stats_view = FeatureView(
    name="driver_hourly_stats",
    entities=[driver],
    ttl=timedelta(days=300),
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64),
        Field(name="created", dtype=UnixTimestamp),
    ],
    online=True,
    source=driver_hourly_stats,
    tags={},
)

driver_stats_fs = FeatureService(
    name="driver_activity",
    features=[driver_hourly_stats_view]
)

input_request = RequestSource(
    name="input_request",
    schema=[
        Field(name="int_val", dtype=Int64),
    ],
)

@on_demand_feature_view(    # noqa
    sources=[
        driver_hourly_stats_view,
        input_request,
    ],
    schema=[
        Field(name="output", dtype=Float64),
        Field(name="time_since_last_date", dtype=Int64),
    ],
)
def transformed_conv_rate(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df['output'] = inputs['conv_rate'] + inputs['int_val']
    df['time_since_last_date'] = (datetime.now() - pd.to_datetime(inputs['created'])).dt.seconds
    return df

feature_service = FeatureService( # noqa
    name="output_service",
    features=[
        driver_hourly_stats_view,
        transformed_conv_rate,
    ],
    owner="fja",
)
