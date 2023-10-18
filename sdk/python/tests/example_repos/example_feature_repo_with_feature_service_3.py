from datetime import timedelta

from feast import Entity, FeatureService, FeatureView, FileSource

driver_hourly_stats = FileSource(
    path="%PARQUET_PATH%",  # placeholder to be replaced by the test
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
    online=True,
    source=driver_hourly_stats,
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
