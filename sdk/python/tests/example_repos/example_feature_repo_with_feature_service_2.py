from datetime import timedelta

from feast import Entity, FeatureService, FeatureView, Field, FileSource
from feast.types import Float32, Int32, Int64

driver_hourly_stats = FileSource(
    path="data/driver_stats.parquet",  # Fake path
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

driver = Entity(
    name="driver_id",
)

driver_hourly_stats_view = FeatureView(
    name="driver_hourly_stats",
    entities=[driver],
    ttl=timedelta(days=1),
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64),
        Field(name="driver_id", dtype=Int32),
    ],
    online=True,
    source=driver_hourly_stats,
    tags={},
)

global_daily_stats = FileSource(
    path="data/global_stats.parquet",  # Fake path
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

global_stats_feature_view = FeatureView(
    name="global_daily_stats",
    entities=[],
    ttl=timedelta(days=1),
    schema=[
        Field(name="num_rides", dtype=Int32),
        Field(name="avg_ride_length", dtype=Float32),
    ],
    online=True,
    source=global_daily_stats,
    tags={},
)

all_stats_service = FeatureService(
    name="all_stats",
    features=[driver_hourly_stats_view, global_stats_feature_view],
    tags={"release": "production"},
)

some_stats_service = FeatureService(
    name="some_stats",
    features=[
        driver_hourly_stats_view[["conv_rate"]],
        global_stats_feature_view[["num_rides"]],
    ],
    tags={"release": "production"},
)
