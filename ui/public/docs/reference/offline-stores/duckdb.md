# DuckDB offline store

## Description

The duckdb offline store provides support for reading [FileSources](../data-sources/file.md). It can read both Parquet and Delta formats. DuckDB offline store uses [ibis](https://ibis-project.org/) under the hood to convert offline store operations to DuckDB queries.

* Entity dataframes can be provided as a Pandas dataframe.

## Getting started
In order to use this offline store, you'll need to run `pip install 'feast[duckdb]'`.

## Example

{% code title="feature_store.yaml" %}
```yaml
project: my_project
registry: data/registry.db
provider: local
offline_store:
    type: duckdb
online_store:
    path: data/online_store.db
```
{% endcode %}

## Functionality Matrix

The set of functionality supported by offline stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the DuckDB offline store.

|                                                                    | DuckdDB |
| :----------------------------------------------------------------- | :----   |
| `get_historical_features` (point-in-time correct join)             | yes     |
| `pull_latest_from_table_or_query` (retrieve latest feature values) | yes     |
| `pull_all_from_table_or_query` (retrieve a saved dataset)          | yes     |
| `offline_write_batch` (persist dataframes to offline store)        | yes     |
| `write_logged_features` (persist logged features to offline store) | yes     |

Below is a matrix indicating which functionality is supported by `IbisRetrievalJob`.

|                                                       | DuckDB|
| ----------------------------------------------------- | ----- |
| export to dataframe                                   | yes   |
| export to arrow table                                 | yes   |
| export to arrow batches                               | no    |
| export to SQL                                         | no    |
| export to data lake (S3, GCS, etc.)                   | no    |
| export to data warehouse                              | no    |
| export as Spark dataframe                             | no    |
| local execution of Python-based on-demand transforms  | yes   |
| remote execution of Python-based on-demand transforms | no    |
| persist results in the offline store                  | yes   |
| preview the query plan before execution               | no    |
| read partitioned data                                 | yes   |

To compare this set of functionality against other offline stores, please see the full [functionality matrix](overview.md#functionality-matrix).
