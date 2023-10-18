from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource, KafkaSource
from feast.data_format import AvroFormat
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
    source=KafkaSource(
        name="kafka",
        timestamp_field="event_timestamp",
        kafka_bootstrap_servers="",
        message_format=AvroFormat(""),
        topic="topic",
        batch_source=FileSource(
            path="data/driver_stats.parquet",  # Fake path
            timestamp_field="event_timestamp",
            created_timestamp_column="created",
        ),
        watermark_delay_threshold=timedelta(days=1),
    ),
    tags={},
)
