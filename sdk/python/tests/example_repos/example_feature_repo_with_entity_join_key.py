from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource, ValueType
from feast.types import Float32, Int64

driver_hourly_stats = FileSource(
    path="%PARQUET_PATH%",  # placeholder to be replaced by the test
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)


# The join key here is deliberately different from the parquet file to test the failure path.
driver = Entity(
    name="driver_id",
    value_type=ValueType.INT64,
    description="driver id",
    join_keys=["driver"],
)


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
