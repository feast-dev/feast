# MsSQL/Synapse offline store (contrib)

## Description

The MsSQL offline store provides support for reading [MsSQL Sources](../data-sources/mssql.md). Specifically, it is developed to read from [Synapse SQL](https://docs.microsoft.com/en-us/azure/synapse-analytics/sql/overview-features) on Microsoft Azure

* Entity dataframes can be provided as a SQL query or can be provided as a Pandas dataframe.

## Getting started
In order to use this offline store, you'll need to run `pip install 'feast[azure]'`. You can get started by then following this [tutorial](https://github.com/feast-dev/feast/blob/master/docs/tutorials/azure/README.md).

## Disclaimer

The MsSQL offline store does not achieve full test coverage.
Please do not assume complete stability.

## Example

{% code title="feature_store.yaml" %}
```yaml
registry:
  registry_store_type: AzureRegistryStore
  path: ${REGISTRY_PATH} # Environment Variable
project: production
provider: azure
online_store:
    type: redis
    connection_string: ${REDIS_CONN} # Environment Variable
offline_store:
    type: mssql
    connection_string: ${SQL_CONN}  # Environment Variable
```
{% endcode %}

## Functionality Matrix

The set of functionality supported by offline stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the Spark offline store.

|                                                                    | MsSql |
| :----------------------------------------------------------------- | :---- |
| `get_historical_features` (point-in-time correct join)             | yes   |
| `pull_latest_from_table_or_query` (retrieve latest feature values) | yes   |
| `pull_all_from_table_or_query` (retrieve a saved dataset)          | yes   |
| `offline_write_batch` (persist dataframes to offline store)        | no    |
| `write_logged_features` (persist logged features to offline store) | no    |

Below is a matrix indicating which functionality is supported by `MsSqlServerRetrievalJob`.

|                                                       | MsSql |
| ----------------------------------------------------- | ----- |
| export to dataframe                                   | yes   |
| export to arrow table                                 | yes   |
| export to arrow batches                               | no    |
| export to SQL                                         | no    |
| export to data lake (S3, GCS, etc.)                   | no    |
| export to data warehouse                              | no    |
| local execution of Python-based on-demand transforms  | no    |
| remote execution of Python-based on-demand transforms | no    |
| persist results in the offline store                  | yes   |

To compare this set of functionality against other offline stores, please see the full [functionality matrix](overview.md#functionality-matrix).
