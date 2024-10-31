# Clickhouse offline store (contrib)

## Description

The Clickhouse offline store provides support for reading [ClickhouseSource](../data-sources/clickhouse.md).
* Entity dataframes can be provided as a SQL query or can be provided as a Pandas dataframe. A Pandas dataframes will be uploaded to Clickhouse as a table (temporary table by default) in order to complete join operations.

## Disclaimer

The Clickhouse offline store does not achieve full test coverage.
Please do not assume complete stability.

## Getting started
In order to use this offline store, you'll need to run `pip install 'feast[clickhouse]'`.

## Example

{% code title="feature_store.yaml" %}
```yaml
project: my_project
registry: data/registry.db
provider: local
offline_store:
  type: feast.infra.offline_stores.contrib.clickhouse_offline_store.clickhouse.ClickhouseOfflineStore
  host: DB_HOST
  port: DB_PORT
  database: DB_NAME
  user: DB_USERNAME
  password: DB_PASSWORD
  use_temporary_tables_for_entity_df: true
online_store:
    path: data/online_store.db
```
{% endcode %}

Note that `use_temporary_tables_for_entity_df` is an optional parameter.
The full set of configuration options is available in [ClickhouseOfflineStoreConfig](https://rtd.feast.dev/en/master/#feast.infra.offline_stores.contrib.clickhouse_offline_store.clickhouse.ClickhouseOfflineStore).

## Functionality Matrix

The set of functionality supported by offline stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the Clickhouse offline store.

|                                                                    | Clickhouse |
| :----------------------------------------------------------------- |:-----------|
| `get_historical_features` (point-in-time correct join)             | yes        |
| `pull_latest_from_table_or_query` (retrieve latest feature values) | yes        |
| `pull_all_from_table_or_query` (retrieve a saved dataset)          | no         |
| `offline_write_batch` (persist dataframes to offline store)        | no         |
| `write_logged_features` (persist logged features to offline store) | no         |

Below is a matrix indicating which functionality is supported by `ClickhouseRetrievalJob`.

|                                                       | Clickhouse |
| ----------------------------------------------------- |------------|
| export to dataframe                                   | yes        |
| export to arrow table                                 | yes        |
| export to arrow batches                               | no         |
| export to SQL                                         | yes        |
| export to data lake (S3, GCS, etc.)                   | yes        |
| export to data warehouse                              | yes        |
| export as Spark dataframe                             | no         |
| local execution of Python-based on-demand transforms  | yes        |
| remote execution of Python-based on-demand transforms | no         |
| persist results in the offline store                  | yes        |
| preview the query plan before execution               | yes        |
| read partitioned data                                 | yes        |

To compare this set of functionality against other offline stores, please see the full [functionality matrix](overview.md#functionality-matrix).
