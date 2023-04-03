from datetime import timedelta

import pandas as pd

from feast.data_source import RequestSource
from feast.entity import Entity
from feast.feature_service import FeatureService
from feast.feature_view import FeatureView
from feast.field import Field
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64, Int64
from feast.value_type import ValueType
from feast import FileSource

file_path = "driver_stats.parquet"
driver_hourly_stats = FileSource(
    path=file_path,
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

# Define an entity for the driver. You can think of an entity as a primary key used to
# fetch features.
driver = Entity(name="driver_id", description="driver id")

# Our parquet files contain sample data that includes a driver_id column, timestamps and
# three feature column. Here we define a Feature View that will allow us to serve this
# data to our model online.
driver_hourly_stats_view = FeatureView(
    name="driver_hourly_stats",
    entities=[driver],
    ttl=timedelta(seconds=86400 * 7),
    schema=[
        Field(name="conv_rate", dtype=Float64),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64),
        Field(name="driver_id", dtype=Int64),
    ],
    online=True,
    source=driver_hourly_stats,
    tags={},
)


input_request = RequestSource(
    name="vals_to_add",
    schema=[
        Field(name="val_to_add", dtype=Int64),
        Field(name="val_to_add_2", dtype=Int64),
        Field(name="avg_daily_trips", dtype=Int64),
    ],
)


@on_demand_feature_view(
    sources=[
        driver_hourly_stats_view,
        input_request,
    ],
    schema=[
        Field(name="conv_rate_plus_val1", dtype=Float64),
        Field(name="conv_rate_plus_val2", dtype=Float64),
    ],
)
def transformed_conv_rate(features_df: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["conv_rate_plus_val1"] = features_df["conv_rate"] + features_df["val_to_add"]
    df["conv_rate_plus_val2"] = features_df["conv_rate"] + features_df["val_to_add_2"]
    return df


generated_data_source = FileSource(
    path="benchmark_data.parquet", timestamp_field="event_timestamp",
)

entity = Entity(name="entity")

benchmark_feature_views = []
for i in range(25):
    fv = FeatureView(
        name=f"feature_view_{i}",
        entities=[entity],
        ttl=timedelta(seconds=86400),
        schema=[Field(name=f"feature_{10 * i + j}", dtype=Int64) for j in range(10)],
        online=True,
        source=generated_data_source,
    )
    benchmark_feature_views.append(fv)

benchmark_feature_service = FeatureService(
    name=f"benchmark_feature_service", features=benchmark_feature_views,
)
