# Data source

The data source refers to raw underlying data (e.g. a table in BigQuery).

Feast uses a time-series data model to represent data. This data model is used to interpret feature data in data sources in order to build training datasets or when materializing features into an online store.

Below is an example data source with a single entity (`driver`) and two features (`trips_today`, and `rating`).

![Ride-hailing data source](<../../.gitbook/assets/image (16).png>)

Feast supports primarily **time-stamped** tabular data as data sources. There are many kinds of possible data sources:

* **Batch data sources:** ideally, these live in data warehouses (BigQuery, Snowflake, Redshift), but can be in data lakes (S3, GCS, etc). Feast supports ingesting and querying data across both.&#x20;
* **Stream data sources**: Feast does **not** have native stream integrations with streams. It does however facilitate making streaming features available in different environments. There are two kinds of sources:
  * **Push sources** allow users to push features into Feast, and make it available for training / batch scoring ("offline"), for realtime feature serving ("online") or both.
  * **\[Alpha] Stream sources** allow users to register metadata from Kafka or Kinesis sources. The onus is on the user to ingest from these sources, though Feast provides some limited helper methods to ingest directly from Kafka / Kinesis topics.&#x20;
