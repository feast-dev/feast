# Couchbase Columnar offline store (contrib)

## Description

The Couchbase Columnar offline store provides support for reading [CouchbaseColumnarSources](../data-sources/couchbase.md). **Note that Couchbase Columnar is available through [Couchbase Capella](https://cloud.couchbase.com/).**
* Entity dataframes can be provided as a SQL++ query or can be provided as a Pandas dataframe. A Pandas dataframe will be uploaded to Couchbase Capella Columnar as a collection.

## Disclaimer

The Couchbase Columnar offline store does not achieve full test coverage.
Please do not assume complete stability.

## Getting started

In order to use this offline store, you'll need to run `pip install 'feast[couchbase]'`. You can get started by then running `feast init -t couchbase`.

## Example

{% code title="feature_store.yaml" %}
```yaml
project: my_project
registry: data/registry.db
provider: local
offline_store:
  type: couchbase
  connection_string: COUCHBASE_COLUMNAR_CONNECTION_STRING # Copied from 'Connect' page in Capella Columnar console, starts with couchbases://
  user: COUCHBASE_COLUMNAR_USER # Couchbase username from access credentials
  password: COUCHBASE_COLUMNAR_PASSWORD # Couchbase password from access credentials
  timeout: 120 # Timeout in seconds for Columnar operations, optional
online_store:
    path: data/online_store.db
```
{% endcode %}

Note that `timeout`is an optional parameter.
The full set of configuration options is available in [CouchbaseColumnarOfflineStoreConfig](https://rtd.feast.dev/en/master/#feast.infra.offline_stores.contrib.couchbase_offline_store.couchbase.CouchbaseColumnarOfflineStoreConfig).


## Functionality Matrix

The set of functionality supported by offline stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the Couchbase Columnar offline store.

|                                                                    | Couchbase Columnar |
| :----------------------------------------------------------------- |:-------------------|
| `get_historical_features` (point-in-time correct join)             | yes                |
| `pull_latest_from_table_or_query` (retrieve latest feature values) | yes                |
| `pull_all_from_table_or_query` (retrieve a saved dataset)          | yes                |
| `offline_write_batch` (persist dataframes to offline store)        | no                 |
| `write_logged_features` (persist logged features to offline store) | no                 |

Below is a matrix indicating which functionality is supported by `CouchbaseColumnarRetrievalJob`.

|                                                       | Couchbase Columnar |
| ----------------------------------------------------- |--------------------|
| export to dataframe                                   | yes                |
| export to arrow table                                 | yes                |
| export to arrow batches                               | no                 |
| export to SQL                                         | yes                |
| export to data lake (S3, GCS, etc.)                   | yes                |
| export to data warehouse                              | yes                |
| export as Spark dataframe                             | no                 |
| local execution of Python-based on-demand transforms  | yes                |
| remote execution of Python-based on-demand transforms | no                 |
| persist results in the offline store                  | yes                |
| preview the query plan before execution               | yes                |
| read partitioned data                                 | yes                |

To compare this set of functionality against other offline stores, please see the full [functionality matrix](overview.md#functionality-matrix).
