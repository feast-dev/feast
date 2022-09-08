from datetime import timedelta

import pandas as pd

from feast.data_source import RequestSource
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64, Int64, String
from feast.field import Field

from feast import Entity, FileSource, FeatureView

driver_hourly_stats = FileSource(
    path="data/driver_stats_with_string.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)
driver = Entity(name="driver_id", description="driver id",)
driver_hourly_stats_view = FeatureView(
    name="driver_hourly_stats",
    entities=[driver],
    ttl=timedelta(days=365),
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64),
        Field(name="string_feature", dtype=String),
    ],
    online=True,
    source=driver_hourly_stats,
    tags={},
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
    sources=[
        driver_hourly_stats_view,
        input_request,
    ],
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

