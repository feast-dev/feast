from datetime import timedelta

from feast import Entity, FeatureView, Field
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
    PostgreSQLSource,
)
from feast.types import Float32, Int64

driver = Entity(name="driver_id", join_keys=["driver_id"],)


driver_stats_source = PostgreSQLSource(
    name="feast_driver_hourly_stats",
    query="SELECT * FROM feast_driver_hourly_stats",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

driver_stats_fv = FeatureView(
    name="driver_hourly_stats",
    entities=["driver_id"],
    ttl=timedelta(weeks=52),
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64),
    ],
    source=driver_stats_source,
)
