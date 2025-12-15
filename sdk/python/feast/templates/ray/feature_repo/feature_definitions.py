# # # # # # # # # # # # # # # # # # # # # # # #
# This is an example feature definition file  #
# showcasing Ray offline store and compute     #
# engine capabilities                          #
# # # # # # # # # # # # # # # # # # # # # # # #

from datetime import timedelta
from pathlib import Path

from feast import Entity, FeatureService, FeatureView, Field, ValueType
from feast.infra.offline_stores.file_source import FileSource
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64, Int64

# Constants related to the generated data sets
CURRENT_DIR = Path(__file__).parent

# Entity definitions
driver = Entity(
    name="driver",
    description="driver id",
    value_type=ValueType.INT64,
    join_keys=["driver_id"],
)

customer = Entity(
    name="customer",
    description="customer id",
    value_type=ValueType.INT64,
    join_keys=["customer_id"],
)

# Data sources - Ray offline store works with FileSource
# These will be processed by Ray for efficient distributed data access
driver_hourly_stats = FileSource(
    name="driver_hourly_stats",
    path=f"{CURRENT_DIR}/%PARQUET_PATH%",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

customer_daily_profile = FileSource(
    name="customer_daily_profile",
    path=f"{CURRENT_DIR}/data/customer_daily_profile.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

# Feature Views - These leverage Ray compute engine for distributed processing
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
    tags={"team": "driver_performance", "processing": "ray"},
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
    tags={"team": "customer_analytics", "processing": "ray"},
)


# On-demand feature view showcasing Ray compute engine capabilities
# This demonstrates real-time feature transformations using Ray
@on_demand_feature_view(
    sources=[driver_hourly_stats_view],
    schema=[
        Field(name="conv_rate_plus_acc_rate", dtype=Float64),
        Field(name="trips_per_day_normalized", dtype=Float64),
    ],
)
def driver_activity_v2(inputs: dict):
    """
    On-demand feature transformations processed by Ray compute engine.
    These calculations happen in real-time and can leverage Ray's
    distributed processing capabilities.
    """
    import pandas as pd

    conv_rate = inputs["conv_rate"]
    acc_rate = inputs["acc_rate"]
    avg_daily_trips = inputs["avg_daily_trips"]

    # Feature engineering using Ray's distributed processing
    conv_rate_plus_acc_rate = conv_rate + acc_rate

    # Normalize trips per day (example of more complex transformation)
    max_trips = avg_daily_trips.max() if len(avg_daily_trips) > 0 else 1
    trips_per_day_normalized = avg_daily_trips / max_trips

    return pd.DataFrame(
        {
            "conv_rate_plus_acc_rate": conv_rate_plus_acc_rate,
            "trips_per_day_normalized": trips_per_day_normalized,
        }
    )


# Feature Service - Groups related features for serving
# Ray compute engine optimizes the retrieval of these feature combinations
driver_activity_v1 = FeatureService(
    name="driver_activity_v1",
    features=[
        driver_hourly_stats_view,
        customer_daily_profile_view,
    ],
    tags={"version": "v1", "compute_engine": "ray"},
)

driver_activity_v2_service = FeatureService(
    name="driver_activity_v2",
    features=[
        driver_hourly_stats_view,
        customer_daily_profile_view,
        driver_activity_v2,  # Includes on-demand transformations
    ],
    tags={"version": "v2", "compute_engine": "ray", "transformations": "on_demand"},
)
