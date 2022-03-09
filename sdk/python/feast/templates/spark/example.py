# # # # # # # # # # # # # # # # # # # # # # # #
# This is an example feature definition file  #
# # # # # # # # # # # # # # # # # # # # # # # #

from pathlib import Path

from google.protobuf.duration_pb2 import Duration

from feast import Entity, Feature, FeatureView, ValueType
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)

# Constants related to the generated data sets
CURRENT_DIR = Path(__file__).parent


# Entity definitions
driver = Entity(name="driver_id", value_type=ValueType.INT64, description="driver id",)
customer = Entity(
    name="customer_id", value_type=ValueType.INT64, description="customer id",
)

# Sources
driver_hourly_stats = SparkSource(
    name="driver_hourly_stats",
    path=f"{CURRENT_DIR}/data/driver_hourly_stats.parquet",
    file_format="parquet",
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created",
)
customer_daily_profile = SparkSource(
    name="customer_daily_profile",
    path=f"{CURRENT_DIR}/data/customer_daily_profile.parquet",
    file_format="parquet",
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created",
)

# Feature Views
driver_hourly_stats_view = FeatureView(
    name="driver_hourly_stats",
    entities=["driver_id"],
    ttl=Duration(seconds=86400 * 7),  # one week
    features=[
        Feature(name="conv_rate", dtype=ValueType.FLOAT),
        Feature(name="acc_rate", dtype=ValueType.FLOAT),
        Feature(name="avg_daily_trips", dtype=ValueType.INT64),
    ],
    online=True,
    batch_source=driver_hourly_stats,
    tags={},
)
customer_daily_profile_view = FeatureView(
    name="customer_daily_profile",
    entities=["customer_id"],
    ttl=Duration(seconds=86400 * 7),  # one week
    features=[
        Feature(name="current_balance", dtype=ValueType.FLOAT),
        Feature(name="avg_passenger_count", dtype=ValueType.FLOAT),
        Feature(name="lifetime_trip_count", dtype=ValueType.INT64),
    ],
    online=True,
    batch_source=customer_daily_profile,
    tags={},
)
