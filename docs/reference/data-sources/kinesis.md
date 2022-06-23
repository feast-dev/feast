# Kinesis source

**Warning**: This is an _experimental_ feature. It's intended for early testing and feedback, and could change without warnings in future releases.

## Description

Kinesis sources allow users to register Kinesis streams as data sources. Feast currently does not launch or monitor jobs to ingest data from Kinesis. Users are responsible for launching and monitoring their own ingestion jobs, which should write feature values to the online store through [FeatureStore.write_to_online_store](https://rtd.feast.dev/en/latest/index.html#feast.feature_store.FeatureStore.write_to_online_store). An example of how to launch such a job with Spark to ingest from Kafka can be found [here](https://github.com/feast-dev/feast/tree/master/sdk/python/feast/infra/contrib); by using a different plugin, the example can be adapted to Kinesis. Feast also provides functionality to write to the offline store using the `write_to_offline_store` functionality.

Kinesis sources must have a batch source specified. The batch source will be used for retrieving historical features. Thus users are also responsible for writing data from their Kinesis streams to a batch data source such as a data warehouse table. When using a Kinesis source as a stream source in the definition of a feature view, a batch source doesn't need to be specified in the feature view definition explicitly.

## Stream sources
Streaming data sources are important sources of feature values. A typical setup with streaming data looks like:

1. Raw events come in (stream 1)
2. Streaming transformations applied (e.g. generating features like `last_N_purchased_categories`) (stream 2)
3. Write stream 2 values to an offline store as a historical log for training (optional)
4. Write stream 2 values to an online store for low latency feature serving
5. Periodically materialize feature values from the offline store into the online store for decreased training-serving skew and improved model performance

## Example
### Defining a Kinesis source
Note that the Kinesis source has a batch source.
```python
from datetime import timedelta

from feast import Field, FileSource, KinesisSource, stream_feature_view
from feast.data_format import JsonFormat
from feast.types import Float32

driver_stats_batch_source = FileSource(
    name="driver_stats_source",
    path="data/driver_stats.parquet",
    timestamp_field="event_timestamp",
)

driver_stats_stream_source = KinesisSource(
    name="driver_stats_stream",
    stream_name="drivers",
    timestamp_field="event_timestamp",
    batch_source=driver_stats_batch_source,
    record_format=JsonFormat(
        schema_json="driver_id integer, event_timestamp timestamp, conv_rate double, acc_rate double, created timestamp"
    ),
    watermark_delay_threshold=timedelta(minutes=5),
)
```

### Using the Kinesis source in a stream feature view
The Kinesis source can be used in a stream feature view.
```python
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

### Ingesting data
See [here](https://github.com/feast-dev/streaming-tutorial) for a example of how to ingest data from a Kafka source into Feast. The approach used in the tutorial can be easily adapted to work for Kinesis as well.
