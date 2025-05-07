# Spark (alpha)

## Description

The Spark batch materialization engine is considered alpha status. It relies on the offline store to output feature values to S3 via `to_remote_storage`, and then loads them into the online store.

See [SparkMaterializationEngine](https://rtd.feast.dev/en/master/index.html?highlight=SparkMaterializationEngine#feast.infra.materialization.spark.spark_materialization_engine.SparkMaterializationEngineConfig) for configuration options.

## Example

{% code title="feature_store.yaml" %}
```yaml
...
offline_store:
  type: snowflake.offline
...
batch_engine:
  type: spark.engine
  partitions: [optional num partitions to use to write to online store]
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
    entity_key_serialization_version=2
)

store = FeatureStore(config=repo_config)
```
{% endcode %}