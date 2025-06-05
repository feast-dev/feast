# # # # # # # # # # # # # # # # # # # # # # # #
# This is an example feature definition file  #
# # # # # # # # # # # # # # # # # # # # # # # #

from datetime import timedelta
from pathlib import Path

import pandas as pd
from feast.on_demand_feature_view import on_demand_feature_view
from feast import Entity, FeatureService, FeatureView, Field, RequestSource
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)
from feast.types import Float32, Float64, Int64

# Constants related to the generated data sets
CURRENT_DIR = Path(__file__).parent


# Entity definitions
driver = Entity(name="driver", description="driver id", join_keys=["driver_id"])
customer = Entity(name="customer", description="customer id", join_keys=["customer_id"])

# Sources
driver_hourly_stats = SparkSource(
    name="driver_hourly_stats",
    path=f"{CURRENT_DIR}/data/driver_hourly_stats.parquet",
    file_format="parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)
customer_daily_profile = SparkSource(
    name="customer_daily_profile",
    path=f"{CURRENT_DIR}/data/customer_daily_profile.parquet",
    file_format="parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

# Feature Views
driver_hourly_stats_view = FeatureView(
    name="driver_hourly_stats",
    entities=[driver],
    ttl=timedelta(days=7),
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64),
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

customer_daily_profile_view = FeatureView(
    name="customer_daily_profile",
    entities=[customer],
    ttl=timedelta(days=7),
    schema=[
        Field(name="current_balance", dtype=Float32),
        Field(name="avg_passenger_count", dtype=Float32),
        Field(name="lifetime_trip_count", dtype=Int64),
    ],
    online=True,
    source=customer_daily_profile,
    tags={},
)

driver_stats_fs = FeatureService(
    name="driver_activity",
    features=[driver_hourly_stats_view, customer_daily_profile_view],
)


# Define an on demand feature view which can generate new features based on
# existing feature views and RequestSource features
@on_demand_feature_view(
    sources=[driver_hourly_stats_view, input_request],
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
