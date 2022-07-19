# # # # # # # # # # # # # # # # # # # # # # # #
# This is an example feature definition file  #
# # # # # # # # # # # # # # # # # # # # # # # #

from datetime import timedelta
from pathlib import Path

from feast import Entity, FeatureService, FeatureView, Field
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)
from feast.types import Float32, Int64

# Constants related to the generated data sets
CURRENT_DIR = Path(__file__).parent


# Entity definitions
driver = Entity(
    name="driver",
    description="driver id",
)
customer = Entity(
    name="customer",
    description="customer id",
)

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
