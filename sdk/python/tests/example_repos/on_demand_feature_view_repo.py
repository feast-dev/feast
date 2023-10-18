from datetime import timedelta

import pandas as pd

from feast import Entity, FeatureView, Field, FileSource
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, String

driver_stats = FileSource(
    name="driver_stats_source",
    path="data/driver_stats_lat_lon.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
    description="A table describing the stats of a driver based on hourly logs",
    owner="test2@gmail.com",
)

driver = Entity(
    name="driver_id",
    description="driver id",
)

driver_daily_features_view = FeatureView(
    name="driver_daily_features",
    entities=[driver],
    ttl=timedelta(seconds=8640000000),
    schema=[
        Field(name="daily_miles_driven", dtype=Float32),
        Field(name="lat", dtype=Float32),
        Field(name="lon", dtype=Float32),
        Field(name="string_feature", dtype=String),
        Field(name="driver_id", dtype=Float32),
    ],
    online=True,
    source=driver_stats,
    tags={"production": "True"},
    owner="test2@gmail.com",
)


@on_demand_feature_view(
    sources=[driver_daily_features_view],
    schema=[
        Field(name="first_char", dtype=String),
        Field(name="concat_string", dtype=String),
    ],
)
def location_features_from_push(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["concat_string"] = inputs.apply(
        lambda x: x.string_feature + "hello", axis=1
    ).astype("string")
    df["first_char"] = inputs["string_feature"].str[:1].astype("string")
    return df
