from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource, ValueType
from feast.types import Float32, Int32, Int64

driver_hourly_stats = FileSource(
    path="%PARQUET_PATH%",  # placeholder to be replaced by the test
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

driver = Entity(name="driver_id", value_type=ValueType.INT64, description="driver id",)


driver_hourly_stats_view = FeatureView(
    name="driver_hourly_stats",
    entities=["driver_id"],
    ttl=timedelta(days=1),
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64),
    ],
    online=True,
    batch_source=driver_hourly_stats,
    tags={},
)


global_daily_stats = FileSource(
    path="%PARQUET_PATH_GLOBAL%",  # placeholder to be replaced by the test
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
    batch_source=global_daily_stats,
    tags={},
)
