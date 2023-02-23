# Hazelcast source

**Warning**: This is an _experimental_ feature. It's intended for early testing and feedback, and could change without warnings in future releases.

## Description

Hazelcast sources allow users to register Hazelcast streams as data sources. 
Feast currently does not launch or monitor jobs to ingest data from Hazelcast. 
Users are responsible for launching and monitoring their own ingestion jobs, which should write feature values to the online store through [FeatureStore.write_to_online_store](https://rtd.feast.dev/en/latest/index.html#feast.feature_store.FeatureStore.write_to_online_store). 
An example of how to launch such a job can be found [here](https://github.com/feast-dev/feast/tree/master/sdk/python/feast/infra/contrib). 
Feast also provides functionality to write to the offline store using the `write_to_offline_store` functionality.

Hazelcast sources must have a batch source specified. The batch source will be used for retrieving historical features. 
Thus users are also responsible for writing data from their Hazelcast streams to a batch data source such as a data warehouse table. 
When using a Hazelcast source as a stream source in the definition of a feature view, a batch source doesn't need to be specified in the feature view definition explicitly.


## Stream sources

Streaming data sources are important sources of feature values. A typical setup with streaming data looks like:

1. Raw events come in (stream 1)
2. Streaming transformations applied in Hazelcast pipeline (e.g. generating features like `last_N_purchased_categories`) (stream 2)
3. Write stream 2 values to an offline store as a historical log for training (optional)
4. Write stream 2 values to an online store for low latency feature serving
5. Periodically materialize feature values from the offline store into the online store for decreased training-serving skew and improved model performance


## Example

### Defining a Hazelcast source
Note that the Hazelcast source has a batch source.
```python
from datetime import timedelta

from feast import FileSource, HazelcastSource
from feast.data_format import JsonFormat

driver_stats_batch_source = FileSource(
    name="driver_stats_source",
    path="data/driver_stats.parquet",
    timestamp_field="event_timestamp",
)

driver_stats_stream_source = HazelcastSource(
    name="driver_stats_stream",
    imap_name="localhost:9092",
    websocket_url="drivers",
    timestamp_field="event_timestamp",
    batch_source=driver_stats_batch_source,
    stream_format=JsonFormat(
        schema_json="driver_id integer, event_timestamp timestamp, conv_percentage double, acc_percentage double, created timestamp"
    ),
    watermark_delay_threshold=timedelta(minutes=5),
)

```

### Using the Hazelcast source in a stream feature view
The Hazelcast source can be used in a stream feature view.
```python
from feast import StreamFeatureView

stream_driver_stats_fv = StreamFeatureView(
    name="stream_driver_hourly_stats",
    source=driver_stats_stream_source,
    entities=[driver],
    ttl=timedelta(seconds=8640000000),
    mode="jet",
    online=True,
)
```

### Ingesting data

You can ingest data from Hazelcast stream to Feast using two way:
1. Start an ingestion job using HazelcastStreamProcessor
2. Listen the sink of Hazelcast stream using Websocket client
