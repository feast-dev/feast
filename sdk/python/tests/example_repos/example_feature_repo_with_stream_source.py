from datetime import timedelta

from feast import FileSource, KafkaSource
from feast.data_format import AvroFormat

stream_source = KafkaSource(
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
)
