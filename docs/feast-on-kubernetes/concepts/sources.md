# Sources

### Overview

Sources are descriptions of external feature data and are registered to Feast as part of [feature tables](feature-tables.md). Once registered, Feast can ingest feature data from these sources into stores.

Currently, Feast supports the following source types:

#### Batch Source

* File \(as in Spark\): Parquet \(only\).
* BigQuery

#### Stream Source

* Kafka
* Kinesis

The following encodings are supported on streams

* Avro
* Protobuf

### Structure of a Source

For both batch and stream sources, the following configurations are necessary:

* **Event timestamp column**: Name of column containing timestamp when event data occurred. Used during point-in-time join of feature values to [entity timestamps]().
* **Created timestamp column**: Name of column containing timestamp when data is created. Used to deduplicate data when multiple copies of the same [entity key]() is ingested.

Example data source specifications:

{% tabs %}
{% tab title="batch\_sources.py" %}
```python
from feast import FileSource
from feast.data_format import ParquetFormat

batch_file_source = FileSource(
    file_format=ParquetFormat(),
    file_url="file:///feast/customer.parquet",
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

### Working with a Source

#### Creating a Source

Sources are defined as part of [feature tables](feature-tables.md):

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

