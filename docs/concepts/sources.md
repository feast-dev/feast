# Sources

### Overview

A `source` is a data source that can be used to find feature data. Users define sources as part of [feature tables](feature-tables.md). Currently, Feast supports the following source types:

* Batch Source
  * File
  * [BigQuery](https://cloud.google.com/bigquery)
* Stream Source
  * [Kafka](https://kafka.apache.org/)
  * [Kinesis](https://aws.amazon.com/kinesis/)

### Structure of a Source

For both batch and stream sources, the following configurations are **necessary**:

* **created\_timestamp\_column**: Name of column containing timestamp when data is created.
* **event\_timestamp\_column**: Name of column containing timestamp when event data occurred.

When configuring data source options, see the [Feast Python API documentation](https://api.docs.feast.dev/python/) for more details.

Some valid source specifications are shown below:

{% tabs %}
{% tab title="batch\_sources.py" %}
```python
from feast import FileSource
from feast.data_format import ParquetFormat

batch_file_source = FileSource(
    file_format=ParquetFormat(),
    file_url="file://feast/*",
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created_timestamp",
)
```
{% endtab %}

{% tab title="stream\_sources.py" %}
```python
from feast import KafkaSource
from feast.data_format import ProtoFormat

stream_kafka_source = KafkaSource(
    bootstrap_servers="localhost:9094",
    message_format=ProtoFormat(class_path="class.path"),
    topic="driver_trips",
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created_timestamp",
)
```
{% endtab %}
{% endtabs %}

The [Feast Python API documentation](https://api.docs.feast.dev/python/) provides more information about options to specify for the above sources.

{% hint style="info" %}
When creating a Feature Table for use in training datasets, specify a batch source already containing materialized data.
{% endhint %}

### Working with a Source

#### Creating a Source

Sources are required when specifying a [feature table](feature-tables.md):

```python
batch_bigquery_source = BigQuerySource(
    table_ref="gcp_project:bq_dataset.bq_table",
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created_timestamp",
)

stream_kinesis_source = KinesisSource(
    bootstrap_servers="localhost:9094",
    record_format=ProtoFormat(class_path="class.path"),
    region="us-east-1",
    stream_name="driver_trips",
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created_timestamp",
)
```

Feast ensures that the source complies with the schema of the feature table. These specified data sources can then be included inside a feature table specification and registered to Feast Core.

