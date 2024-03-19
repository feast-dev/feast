# Spark offline store (contrib)

## Description

The Spark offline store provides support for reading [SparkSources](../data-sources/spark.md).

* Entity dataframes can be provided as a SQL query, Pandas dataframe or can be provided as a Pyspark dataframe. A Pandas dataframes will be converted to a Spark dataframe and processed as a temporary view.

## Disclaimer

The Spark offline store does not achieve full test coverage.
Please do not assume complete stability.

## Getting started
In order to use this offline store, you'll need to run `pip install 'feast[spark]'`. You can get started by then running `feast init -t spark`.

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
        spark.sql.execution.arrow.fallback.enabled: "true"
        spark.sql.execution.arrow.pyspark.enabled: "true"
online_store:
    path: data/online_store.db
```
{% endcode %}

The full set of configuration options is available in [SparkOfflineStoreConfig](https://rtd.feast.dev/en/master/#feast.infra.offline_stores.contrib.spark_offline_store.spark.SparkOfflineStoreConfig).

## Functionality Matrix

The set of functionality supported by offline stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the Spark offline store.

|                                                                    | Spark |
| :----------------------------------------------------------------- | :---- |
| `get_historical_features` (point-in-time correct join)             | yes   |
| `pull_latest_from_table_or_query` (retrieve latest feature values) | yes   |
| `pull_all_from_table_or_query` (retrieve a saved dataset)          | yes   |
| `offline_write_batch` (persist dataframes to offline store)        | no    |
| `write_logged_features` (persist logged features to offline store) | no    |

Below is a matrix indicating which functionality is supported by `SparkRetrievalJob`.

|                                                       | Spark |
| ----------------------------------------------------- | ----- |
| export to dataframe                                   | yes   |
| export to arrow table                                 | yes   |
| export to arrow batches                               | no    |
| export to SQL                                         | no    |
| export to data lake (S3, GCS, etc.)                   | no    |
| export to data warehouse                              | no    |
| export as Spark dataframe                             | yes   |
| local execution of Python-based on-demand transforms  | no    |
| remote execution of Python-based on-demand transforms | no    |
| persist results in the offline store                  | yes   |
| preview the query plan before execution               | yes   |
| read partitioned data                                 | yes   |

To compare this set of functionality against other offline stores, please see the full [functionality matrix](overview.md#functionality-matrix).
