# BigQuery

### Description

The BigQuery offline store provides support for reading [BigQuerySources](../data-sources/bigquery.md).

* BigQuery tables and views are allowed as sources.
* All joins happen within BigQuery. 
* Entity dataframes can be provided as a SQL query or can be provided as a Pandas dataframe. Pandas dataframes will be uploaded to BigQuery in order to complete join operations.
* A [BigQueryRetrievalJob](https://github.com/feast-dev/feast/blob/c50a36ec1ad5b8d81c6f773c23204db7c7a7d218/sdk/python/feast/infra/offline_stores/bigquery.py#L210) is returned when calling `get_historical_features()`.

### Example

{% code title="feature\_store.yaml" %}
```yaml
project: my_feature_repo
registry: gs://my-bucket/data/registry.db
provider: gcp
offline_store:
  type: bigquery
  dataset: feast_bq_dataset
```
{% endcode %}

Configuration options are available [here](https://rtd.feast.dev/en/latest/#feast.repo_config.BigQueryOfflineStoreConfig).

