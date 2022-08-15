from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource
from feast.types import Float32, Int32, Int64

driver = Entity(
    name="driver_id",
    description="driver id",
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
    source=FileSource(
        path="data/driver_stats.parquet",  # Fake path
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    ),
    tags={},
)
