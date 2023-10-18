# Snowflake offline store

## Description

The [Snowflake](https://trial.snowflake.com) offline store provides support for reading [SnowflakeSources](../data-sources/snowflake.md).
* All joins happen within Snowflake.
* Entity dataframes can be provided as a SQL query or can be provided as a Pandas dataframe. A Pandas dataframes will be uploaded to Snowflake as a temporary table in order to complete join operations.

## Getting started
In order to use this offline store, you'll need to run `pip install 'feast[snowflake]'`.

If you're using a file based registry, then you'll also need to install the relevant cloud extra (`pip install 'feast[snowflake, CLOUD]'` where `CLOUD` is one of `aws`, `gcp`, `azure`)

You can get started by then running `feast init -t snowflake`.

## Example

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
offline_store:
  type: snowflake.offline
  account: snowflake_deployment.us-east-1
  user: user_login
  password: user_password
  role: SYSADMIN
  warehouse: COMPUTE_WH
  database: FEAST
  schema: PUBLIC
```
{% endcode %}

The full set of configuration options is available in [SnowflakeOfflineStoreConfig](https://rtd.feast.dev/en/latest/#feast.infra.offline_stores.snowflake.SnowflakeOfflineStoreConfig).

## Functionality Matrix

The set of functionality supported by offline stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the Snowflake offline store.

|                                                                    | Snowflake |
| :----------------------------------------------------------------- | :-------- |
| `get_historical_features` (point-in-time correct join)             | yes       |
| `pull_latest_from_table_or_query` (retrieve latest feature values) | yes       |
| `pull_all_from_table_or_query` (retrieve a saved dataset)          | yes       |
| `offline_write_batch` (persist dataframes to offline store)        | yes       |
| `write_logged_features` (persist logged features to offline store) | yes       |

Below is a matrix indicating which functionality is supported by `SnowflakeRetrievalJob`.

|                                                       | Snowflake |
| ----------------------------------------------------- | --------- |
| export to dataframe                                   | yes       |
| export to arrow table                                 | yes       |
| export to arrow batches                               | yes       |
| export to SQL                                         | yes       |
| export to data lake (S3, GCS, etc.)                   | yes       |
| export to data warehouse                              | yes       |
| export as Spark dataframe                             | yes       |
| local execution of Python-based on-demand transforms  | yes       |
| remote execution of Python-based on-demand transforms | no        |
| persist results in the offline store                  | yes       |
| preview the query plan before execution               | yes       |
| read partitioned data                                 | yes       |

To compare this set of functionality against other offline stores, please see the full [functionality matrix](overview.md#functionality-matrix).
