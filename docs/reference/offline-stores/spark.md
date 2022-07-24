# Spark (contrib)

## Description

The Spark offline store is an offline store currently in alpha development that provides support for reading [SparkSources](../data-sources/spark.md).

## Disclaimer

This Spark offline store still does not achieve full test coverage and continues to fail some integration tests when integrating with the feast universal test suite. Please do NOT assume complete stability of the API.

* Spark tables and views are allowed as sources that are loaded in from some Spark store(e.g in Hive or in memory).
* Entity dataframes can be provided as a SQL query or can be provided as a Pandas dataframe. Pandas dataframes will be converted to a Spark dataframe and processed as a temporary view.
* A `SparkRetrievalJob` is returned when calling `get_historical_features()`.
  * This allows you to call
     * `to_df` to retrieve the pandas dataframe.
     * `to_arrow` to retrieve the dataframe as a pyarrow Table.
     * `to_spark_df` to retrieve the dataframe the spark.

## Example

{% code title="feature_store.yaml" %}
```yaml
project: my_project
registry: data/registry.db
provider: local
offline_store:
    type: spark
    spark_conf:
        spark.master: "local[*]"
        spark.ui.enabled: "false"
        spark.eventLog.enabled: "false"
        spark.sql.catalogImplementation: "hive"
        spark.sql.parser.quotedRegexColumnNames: "true"
        spark.sql.session.timeZone: "UTC"
online_store:
    path: data/online_store.db
```
{% endcode %}
