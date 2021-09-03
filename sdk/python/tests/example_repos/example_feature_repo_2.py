from datetime import timedelta

from feast import Entity, Feature, FeatureView, FileSource, ValueType

driver_hourly_stats = FileSource(
    path="%PARQUET_PATH%",  # placeholder to be replaced by the test
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created",
)

driver = Entity(name="driver_id", value_type=ValueType.INT64, description="driver id",)


driver_hourly_stats_view = FeatureView(
    name="driver_hourly_stats",
    entities=["driver_id"],
    ttl=timedelta(days=1),
    features=[
        Feature(name="conv_rate", dtype=ValueType.FLOAT),
        Feature(name="acc_rate", dtype=ValueType.FLOAT),
        Feature(name="avg_daily_trips", dtype=ValueType.INT64),
    ],
    online=True,
    batch_source=driver_hourly_stats,
    tags={},
)


global_daily_stats = FileSource(
    path="%PARQUET_PATH_GLOBAL%",  # placeholder to be replaced by the test
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created",
)


global_stats_feature_view = FeatureView(
    name="global_daily_stats",
    entities=[],
    ttl=timedelta(days=1),
    features=[
        Feature(name="num_rides", dtype=ValueType.INT32),
        Feature(name="avg_ride_length", dtype=ValueType.FLOAT),
    ],
    online=True,
    batch_source=global_daily_stats,
    tags={},
)
