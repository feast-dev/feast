from datetime import timedelta

from feast import (
    FileSource,
    KafkaSource,
)
from feast.data_format import JsonFormat

# Feast also supports pulling data from data warehouses like BigQuery, Snowflake, Redshift and data lakes (e.g. via Redshift Spectrum, Trino, Spark)
customer_stats_batch_source = FileSource(
    name="customer_stats_source",
    path="data/customer_stats.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
    description="A table describing the stats of a customer based on daily logs",
    owner="test1@gmail.com",
)

driver_stats_batch_source = FileSource(
    name="driver_stats_source",
    path="data/driver_stats.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
    description="A table describing the stats of a driver based on hourly logs",
    owner="test2@gmail.com",
)

driver_stats_stream_source = KafkaSource(
    name="driver_stats_stream",
    kafka_bootstrap_servers="localhost:9092",
    topic="drivers",
    timestamp_field="event_timestamp",
    batch_source=driver_stats_batch_source,
    message_format=JsonFormat(
        schema_json="driver_id integer, event_timestamp timestamp, conv_rate double, acc_rate double, created timestamp"
    ),
    watermark_delay_threshold=timedelta(minutes=5),
    description="The Kafka stream containing the driver stats",
    owner="test3@gmail.com",
)
