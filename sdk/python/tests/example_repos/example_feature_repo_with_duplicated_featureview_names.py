from datetime import timedelta

from feast import FeatureView, FileSource

driver_hourly_stats = FileSource(
    path="driver_stats.parquet",  # this parquet is not real and will not be read
)

driver_hourly_stats_view = FeatureView(
    name="driver_hourly_stats",  # Intentionally use the same FeatureView name
    entities=["driver_id"],
    online=False,
    source=driver_hourly_stats,
    ttl=timedelta(days=1),
    tags={},
)

driver_hourly_stats_view_dup1 = FeatureView(
    name="driver_hourly_stats",  # Intentionally use the same FeatureView name
    entities=["driver_id"],
    online=False,
    source=driver_hourly_stats,
    ttl=timedelta(days=1),
    tags={},
)
