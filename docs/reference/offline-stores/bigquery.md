# BigQuery offline store

## Description

The BigQuery offline store provides support for reading [BigQuerySources](../data-sources/bigquery.md).

* All joins happen within BigQuery. 
* Entity dataframes can be provided as a SQL query or can be provided as a Pandas dataframe. A Pandas dataframes will be uploaded to BigQuery as a table (marked for expiration) in order to complete join operations.

## Getting started
In order to use this offline store, you'll need to run `pip install 'feast[gcp]'`. You can get started by then running `feast init -t gcp`.

## Example

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: gs://my-bucket/data/registry.db
provider: gcp
offline_store:
  type: bigquery
  dataset: feast_bq_dataset
```
{% endcode %}

The full set of configuration options is available in [BigQueryOfflineStoreConfig](https://rtd.feast.dev/en/latest/index.html#feast.infra.offline_stores.bigquery.BigQueryOfflineStoreConfig).

## Functionality Matrix

The set of functionality supported by offline stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the BigQuery offline store.

|                                                                    | BigQuery |
| :----------------------------------------------------------------- | :------- |
| `get_historical_features` (point-in-time correct join)             | yes      |
| `pull_latest_from_table_or_query` (retrieve latest feature values) | yes      |
| `pull_all_from_table_or_query` (retrieve a saved dataset)          | yes      |
| `offline_write_batch` (persist dataframes to offline store)        | yes      |
| `write_logged_features` (persist logged features to offline store) | yes      |

Below is a matrix indicating which functionality is supported by `BigQueryRetrievalJob`.

|                                                       | BigQuery |
| ----------------------------------------------------- | -------- |
| export to dataframe                                   | yes      |
| export to arrow table                                 | yes      |
| export to arrow batches                               | no       |
| export to SQL                                         | yes      |
| export to data lake (S3, GCS, etc.)                   | no       |
| export to data warehouse                              | yes      |
| export as Spark dataframe                             | no       |
| local execution of Python-based on-demand transforms  | yes      |
| remote execution of Python-based on-demand transforms | no       |
| persist results in the offline store                  | yes      |
| preview the query plan before execution               | yes      |
| read partitioned data*                                | partial  |

*See [GitHub issue](https://github.com/feast-dev/feast/issues/2530) for details on proposed solutions for enabling the BigQuery offline store to understand tables that use `_PARTITIONTIME` as the partition column.

To compare this set of functionality against other offline stores, please see the full [functionality matrix](overview.md#functionality-matrix).
