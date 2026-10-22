# Spark

## Description

Spark Compute Engine provides a distributed execution engine for batch materialization operations (`materialize` and `materialize-incremental`) and historical retrieval operations (`get_historical_features`).

It is designed to handle large-scale data processing and can be used with various offline stores, such as Snowflake, BigQuery, and Spark SQL.

### Design
The Spark Compute engine is implemented as a subclass of `feast.infra.compute_engine.ComputeEngine`.
Offline store is used to read and write data, while the Spark engine is used to perform transformations and aggregations on the data.
The engine supports the following features:
- Support for reading different data sources, such as Spark SQL, BigQuery, and Snowflake.
- Distributed execution of feature transformations and aggregations.
- Support for custom transformations using Spark SQL or UDFs.


## Example

{% code title="feature_store.yaml" %}
```yaml
...
offline_store:
  type: snowflake.offline
...
batch_engine:
  type: spark.engine
  partitions: 10 # number of partitions when writing to the online or offline store
  spark_conf:
    spark.master: "local[*]"
    spark.app.name: "Feast Spark Engine"
    spark.sql.shuffle.partitions: 100
    spark.executor.memory: "4g"
```
{% endcode %}

## Example in Python

{% code title="feature_store.py" %}
```python
from feast import FeatureStore, RepoConfig
from feast.repo_config import RegistryConfig
from feast.infra.online_stores.dynamodb import DynamoDBOnlineStoreConfig
from feast.infra.offline_stores.contrib.spark_offline_store.spark import SparkOfflineStoreConfig

repo_config = RepoConfig(
    registry="s3://[YOUR_BUCKET]/feast-registry.db",
    project="feast_repo",
    provider="aws",
    offline_store=SparkOfflineStoreConfig(
      spark_conf={
        "spark.ui.enabled": "false",
        "spark.eventLog.enabled": "false",
        "spark.sql.catalogImplementation": "hive",
        "spark.sql.parser.quotedRegexColumnNames": "true",
        "spark.sql.session.timeZone": "UTC"
      }
    ),
    batch_engine={
      "type": "spark.engine",
      "partitions": 10
    },
    online_store=DynamoDBOnlineStoreConfig(region="us-west-1"),
    entity_key_serialization_version=3
)

store = FeatureStore(config=repo_config)
```
{% endcode %}