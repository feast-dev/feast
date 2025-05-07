# PostgreSQL offline store (contrib)

## Description

The PostgreSQL offline store provides support for reading [PostgreSQLSources](../data-sources/postgres.md).
* Entity dataframes can be provided as a SQL query or can be provided as a Pandas dataframe. A Pandas dataframes will be uploaded to Postgres as a table in order to complete join operations.

## Disclaimer

The PostgreSQL offline store does not achieve full test coverage.
Please do not assume complete stability.

## Getting started
In order to use this offline store, you'll need to run `pip install 'feast[postgres]'`. You can get started by then running `feast init -t postgres`.

## Example

{% code title="feature_store.yaml" %}
```yaml
project: my_project
registry: data/registry.db
provider: local
offline_store:
  type: postgres
  host: DB_HOST
  port: DB_PORT
  database: DB_NAME
  db_schema: DB_SCHEMA
  user: DB_USERNAME
  password: DB_PASSWORD
  sslmode: verify-ca
  sslkey_path: /path/to/client-key.pem
  sslcert_path: /path/to/client-cert.pem
  sslrootcert_path: /path/to/server-ca.pem
  entity_select_mode: temp_table
online_store:
    path: data/online_store.db
```
{% endcode %}

Note that `sslmode`, `sslkey_path`, `sslcert_path`, and `sslrootcert_path` are optional parameters.
The full set of configuration options is available in [PostgreSQLOfflineStoreConfig](https://rtd.feast.dev/en/master/#feast.infra.offline_stores.contrib.postgres_offline_store.postgres.PostgreSQLOfflineStoreConfig).

Additionally, a new optional parameter `entity_select_mode` was added to tell how Postgres should load the entity data. By default(`temp_table`), a temporary table is created and the entity data frame or sql is loaded into that table. A new value of `embed_query` was added to allow directly loading the SQL query into a CTE, providing improved performance and skipping the need to CREATE and DROP the temporary table.

## Functionality Matrix

The set of functionality supported by offline stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the PostgreSQL offline store.

|                                                                    | Postgres |
| :----------------------------------------------------------------- | :------- |
| `get_historical_features` (point-in-time correct join)             | yes      |
| `pull_latest_from_table_or_query` (retrieve latest feature values) | yes      |
| `pull_all_from_table_or_query` (retrieve a saved dataset)          | yes      |
| `offline_write_batch` (persist dataframes to offline store)        | no       |
| `write_logged_features` (persist logged features to offline store) | no       |

Below is a matrix indicating which functionality is supported by `PostgreSQLRetrievalJob`.

|                                                       | Postgres |
| ----------------------------------------------------- | -------- |
| export to dataframe                                   | yes      |
| export to arrow table                                 | yes      |
| export to arrow batches                               | no       |
| export to SQL                                         | yes      |
| export to data lake (S3, GCS, etc.)                   | yes      |
| export to data warehouse                              | yes      |
| export as Spark dataframe                             | no       |
| local execution of Python-based on-demand transforms  | yes      |
| remote execution of Python-based on-demand transforms | no       |
| persist results in the offline store                  | yes      |
| preview the query plan before execution               | yes      |
| read partitioned data                                 | yes      |

To compare this set of functionality against other offline stores, please see the full [functionality matrix](overview.md#functionality-matrix).
