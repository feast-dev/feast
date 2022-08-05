# Data ingestion

### Data source

The data source refers to raw underlying data (e.g. a table in BigQuery).

Feast uses a time-series data model to represent data. This data model is used to interpret feature data in data sources in order to build training datasets or when materializing features into an online store.

Below is an example data source with a single entity (`driver`) and two features (`trips_today`, and `rating`).

![Ride-hailing data source](<../../.gitbook/assets/image (16).png>)

Feast supports primarily **time-stamped** tabular data as data sources. There are many kinds of possible data sources:

* **Batch data sources:** ideally, these live in data warehouses (BigQuery, Snowflake, Redshift), but can be in data lakes (S3, GCS, etc). Feast supports ingesting and querying data across both.
* **Stream data sources**: Feast does **not** have native streaming integrations. It does however facilitate making streaming features available in different environments. There are two kinds of sources:
  * **Push sources** allow users to push features into Feast, and make it available for training / batch scoring ("offline"), for realtime feature serving ("online") or both.&#x20;
  * **\[Alpha] Stream sources** allow users to register metadata from Kafka or Kinesis sources. The onus is on the user to ingest from these sources, though Feast provides some limited helper methods to ingest directly from Kafka / Kinesis topics.
* **(Experimental) Request data sources:** This is data that is only available at request time (e.g. from a user action that needs an immediate model prediction response). This is primarily relevant as an input into **on-demand feature views**, which allow light-weight feature engineering and combining features across sources.

### Batch data ingestion

Ingesting from batch sources is only necessary to power real-time models. This is done through **materialization**. Under the hood, Feast manages an _offline store_ (to scalably generate training data from batch sources) and an _online store_ (to provide low-latency access to features for real-time models).

A key command to use in Feast is the `materialize_incremental` command, which fetches the latest values for all entities in the batch source and ingests these values into the online store.&#x20;

Materialization can be called programmatically or through the CLI:

<details>

<summary>Code example: programmatic scheduled materialization</summary>

This snippet creates a feature store object which points to the registry (which knows of all defined features) and the online store (DynamoDB in this case), and&#x20;

```python
# Define Python callable
def materialize():
  repo_config = RepoConfig(
    registry=RegistryConfig(path="s3://[YOUR BUCKET]/registry.pb"),
    project="feast_demo_aws",
    provider="aws",
    offline_store="file",
    online_store=DynamoDBOnlineStoreConfig(region="us-west-2")
  )
  store = FeatureStore(config=repo_config)
  store.materialize_incremental(datetime.datetime.now())

# (In production) Use Airflow PythonOperator
materialize_python = PythonOperator(
    task_id='materialize_python',
    python_callable=materialize,
)
```

</details>

<details>

<summary>Code example: CLI based materialization</summary>

#### How to run this in the CLI

```bash
CURRENT_TIME=$(date -u +"%Y-%m-%dT%H:%M:%S") 
feast materialize-incremental $CURRENT_TIME
```

#### How to run this on Airflow

```python
# Use BashOperator
materialize_bash = BashOperator(
    task_id='materialize',
    bash_command=f'feast materialize-incremental {datetime.datetime.now().replace(microsecond=0).isoformat()}',
)
```

</details>

### Stream data ingestion

Ingesting from stream sources happens either via a Push API or via a contrib processor that leverages an existing Spark context.

* To **push data into the offline or online stores**: see [push sources](../../reference/data-sources/push.md) for details.
* (experimental) To **use a contrib Spark processor** to ingest from a topic, see [Tutorial: Building streaming features](../../tutorials/building-streaming-features.md)

