# SQLite offline store

## Description

The SQLite offline store provides support for reading and writing feature data using SQLite databases. It's a lightweight, file-based offline store option that's ideal for local development, testing, and small-scale deployments.

* Entity dataframes can be provided as a Pandas dataframe or as a SQL query.
* The SQLite offline store supports all core Feast functionality including point-in-time joins.

## Getting started
In order to use this offline store, you'll need to run `pip install 'feast[sqlite]'`.

## Example

{% code title="feature_store.yaml" %}
```yaml
project: my_project
registry: data/registry.db
provider: local
offline_store:
    type: sqlite
    path: data/offline.db
online_store:
    path: data/online_store.db
```
{% endcode %}

## Functionality Matrix

The set of functionality supported by offline stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the SQLite offline store.

|                                                                    | SQLite |
| :----------------------------------------------------------------- | :----- |
| `get_historical_features` (point-in-time correct join)             | yes    |
| `pull_latest_from_table_or_query` (retrieve latest feature values) | yes    |
| `pull_all_from_table_or_query` (retrieve a saved dataset)          | yes    |
| `offline_write_batch` (persist dataframes to offline store)        | yes    |
| `write_logged_features` (persist logged features to offline store) | yes    |

Below is a matrix indicating which functionality is supported by `SqliteRetrievalJob`.

|                                                       | SQLite |
| ----------------------------------------------------- | ------ |
| export to dataframe                                   | yes    |
| export to arrow table                                 | yes    |
| export to arrow batches                               | no     |
| export to SQL                                         | no     |
| export to data lake (S3, GCS, etc.)                   | no     |
| export to data warehouse                              | no     |
| export as Spark dataframe                             | no     |
| local execution of Python-based on-demand transforms  | yes    |
| remote execution of Python-based on-demand transforms | no     |
| persist results in the offline store                  | yes    |
| preview the query plan before execution               | no     |
| read partitioned data                                 | no     |

To compare this set of functionality against other offline stores, please see the full [functionality matrix](overview.md#functionality-matrix).
