# Kafka source

**Warning**: This is an _experimental_ feature. It's intended for early testing and feedback, and could change without warnings in future releases.

## Description

Kafka sources allow users to register Kafka streams as a data source. Feast currently does not manage the ingestion of data from Kafka streams; users must manage the ingestion themselves and write feature values to the online store through [FeatureStore.write_to_online_store](https://rtd.feast.dev/en/latest/index.html#feast.feature_store.FeatureStore.write_to_online_store).

Kafka sources must have a batch source specified, since that's the source used when retrieving historical features. When using a Kafka source as a stream source in the definition of a feature view, a batch source doesn't need to be specified in the definition explicitly.

## Stream sources
Streaming data sources are important sources of feature values. A typical setup with streaming data looks like:

1. Raw events come in (stream 1)
2. Streaming transformations applied (e.g. generating features like `last_N_purchased_categories`) (stream 2)
3. Write stream 2 values to an offline store as a historical log for training
4. Write stream 2 values to an online store for low latency feature serving
5. Periodically materialize feature values from the offline store into the online store for improved correctness

Feast now allows users to push features previously registered in a feature view to the online store for fresher features.

## Example
### Defining a Kafka source
Note that the Kafka source has a batch source.
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

### Pushing data
See [here](https://github.com/feast-dev/streaming-tutorial) for a example of how to ingest data from a Kafka source into Feast.
