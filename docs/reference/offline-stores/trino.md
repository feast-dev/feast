# Trino offline store (contrib)

## Description

The Trino offline store provides support for reading [TrinoSources](../data-sources/trino.md).
* Entity dataframes can be provided as a SQL query or can be provided as a Pandas dataframe. A Pandas dataframes will be uploaded to Trino as a table in order to complete join operations.

## Disclaimer

The Trino offline store does not achieve full test coverage.
Please do not assume complete stability.

## Getting started
In order to use this offline store, you'll need to run `pip install 'feast[trino]'`. You can then run `feast init`, then swap out `feature_store.yaml` with the below example to connect to Trino.

## Example

{% code title="feature_store.yaml" %}
```yaml
project: feature_repo
registry: data/registry.db
provider: local
offline_store:
    type: feast_trino.trino.TrinoOfflineStore
    host: localhost
    port: 8080
    catalog: memory
    connector:
        type: memory
online_store:
    path: data/online_store.db
```
{% endcode %}

The full set of configuration options is available in [TrinoOfflineStoreConfig](https://rtd.feast.dev/en/master/#trino-offline-store).

## Functionality Matrix

The set of functionality supported by offline stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the Trino offline store.

|                                                                    | Trino |
| :----------------------------------------------------------------- | :---- |
| `get_historical_features` (point-in-time correct join)             | yes   |
| `pull_latest_from_table_or_query` (retrieve latest feature values) | yes   |
| `pull_all_from_table_or_query` (retrieve a saved dataset)          | yes   |
| `offline_write_batch` (persist dataframes to offline store)        | no    |
| `write_logged_features` (persist logged features to offline store) | no    |

Below is a matrix indicating which functionality is supported by `TrinoRetrievalJob`.

|                                                       | Trino |
| ----------------------------------------------------- | ----- |
| export to dataframe                                   | yes   |
| export to arrow table                                 | yes   |
| export to arrow batches                               | no    |
| export to SQL                                         | yes   |
| export to data lake (S3, GCS, etc.)                   | no    |
| export to data warehouse                              | no    |
| export as Spark dataframe                             | no    |
| local execution of Python-based on-demand transforms  | yes   |
| remote execution of Python-based on-demand transforms | no    |
| persist results in the offline store                  | no    |
| preview the query plan before execution               | yes   |
| read partitioned data                                 | yes   |

To compare this set of functionality against other offline stores, please see the full [functionality matrix](overview.md#functionality-matrix).
