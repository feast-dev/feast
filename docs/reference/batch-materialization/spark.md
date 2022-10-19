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
