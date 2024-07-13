# Dask offline store

## Description

The Dask offline store provides support for reading [FileSources](../data-sources/file.md).

{% hint style="warning" %}
All data is downloaded and joined using Python and therefore may not scale to production workloads.
{% endhint %}

## Example

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
offline_store:
  type: dask
```
{% endcode %}

The full set of configuration options is available in [DaskOfflineStoreConfig](https://rtd.feast.dev/en/latest/#feast.infra.offline_stores.dask.DaskOfflineStoreConfig).

## Functionality Matrix

The set of functionality supported by offline stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the dask offline store.

| | Dask |
| :-------------------------------- | :-- |
| `get_historical_features` (point-in-time correct join)             | yes |
| `pull_latest_from_table_or_query` (retrieve latest feature values) | yes |
| `pull_all_from_table_or_query` (retrieve a saved dataset)          | yes |
| `offline_write_batch` (persist dataframes to offline store)        | yes |
| `write_logged_features` (persist logged features to offline store) | yes |

Below is a matrix indicating which functionality is supported by `DaskRetrievalJob`.

| | Dask |
| --------------------------------- | --- |
| export to dataframe                                   | yes |
| export to arrow table                                 | yes |
| export to arrow batches                               | no  |
| export to SQL                                         | no  |
| export to data lake (S3, GCS, etc.)                   | no  |
| export to data warehouse                              | no  |
| export as Spark dataframe                             | no  |
| local execution of Python-based on-demand transforms  | yes |
| remote execution of Python-based on-demand transforms | no  |
| persist results in the offline store                  | yes |
| preview the query plan before execution               | yes |
| read partitioned data                                 | yes |

To compare this set of functionality against other offline stores, please see the full [functionality matrix](overview.md#functionality-matrix).
