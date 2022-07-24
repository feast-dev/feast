# Stream feature view

## Stream feature views

A stream feature view is an extension of a normal feature view. The primary difference is that stream feature views have both stream and batch data sources, whereas a normal feature view only has a batch data source.

Stream feature views should be used instead of normal feature views when there are stream data sources (e.g. Kafka and Kinesis) available to provide fresh features in an online setting. Here is an example definition of a stream feature view with an attached transformation:

```python
from datetime import timedelta

from feast import Field, FileSource, KafkaSource, stream_feature_view
from feast.data_format import JsonFormat
from feast.types import Float32

driver_stats_batch_source = FileSource(
    name="driver_stats_source",
    path="data/driver_stats.parquet",
    timestamp_field="event_timestamp",
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
)

@stream_feature_view(
    entities=[driver],
    ttl=timedelta(seconds=8640000000),
    mode="spark",
    schema=[
        Field(name="conv_percentage", dtype=Float32),
        Field(name="acc_percentage", dtype=Float32),
    ],
    timestamp_field="event_timestamp",
    online=True,
    source=driver_stats_stream_source,
)
def driver_hourly_stats_stream(df: DataFrame):
    from pyspark.sql.functions import col

    return (
        df.withColumn("conv_percentage", col("conv_rate") * 100.0)
        .withColumn("acc_percentage", col("acc_rate") * 100.0)
        .drop("conv_rate", "acc_rate")
    )
```

See [here](https://github.com/feast-dev/streaming-tutorial) for a example of how to use stream feature views.
