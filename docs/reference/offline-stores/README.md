# Offline stores

Please see [Offline Store](../../getting-started/architecture-and-components/offline-store.md) for a conceptual explanation of offline stores.

Here is a list of functionality supported by some or all offline stores:

* Point-in-time correct retrieval of historical features. Data scientists use this to get historical training data.
* Retrieval of most recent feature values. [Providers](../../getting-started/architecture-and-components/provider.md) use this to materialize features into the online store.
* Persisting a dataframe. ML engineers use this to persist data from streaming pipelines into an offline store.

Below is a matrix indicating which offline stores support what functionality.

| | historical retrieval | retrieval of most recent features | persisting a dataframe |
| ------------------- | --- | --- | ------ |
| File                | yes | yes | yes |
| BigQuery            | yes | yes | yes |
| Snowflake           | yes | yes | yes |
| Redshift            | yes | yes | yes |
| Postgres (contrib)  | yes | yes | no  |
| Spark (contrib)     | yes | yes | no  |
| Trino (contrib)     | yes | yes | no  |

Retrieving features generates a `RetrievalJob` specific to an offline store, such as a `SnowflakeRetrievalJob`. Here is a list of functionality supported by some or all `RetrievalJob`s:
* Persisting into the corresponding offline store. This is used for saving training datasets. See this [tutorial](../../tutorials/validating-historical-features.md) for more info.
* Converting to a pandas dataframe. This is to allow the features to be consumed locally, e.g. for training a model in a notebook.
* Converting to a pyarrow table. This is to allow the features to be consumed locally, e.g. for training a model in a notebook.
* Exporting to remote storage (e.g. S3, GCS). This is to allow the features to be consumed in a distributed fashion, e.g. for distributed training or distributed materialization of features into the online store.

Below is a matrix indicating which `RetrievalJob`s support what functionality.

|   | write to offline store | convert to pandas | convert to pyarrow | export to remote storage |
| ------------------- | --- | --- | --- | ------------ |
| File                | yes | yes | yes | no           |
| BigQuery            | yes | yes | yes | to GCS       |
| Snowflake           | yes | yes | yes | to Snowflake |
| Redshift            | yes | yes | yes | to S3        |
| Postgres (contrib)  | no  | yes | yes | no           |
| Spark (contrib)     | no  | yes | yes | no           |
| Trino (contrib)     | no  | yes | yes | no           |


{% page-ref page="file.md" %}

{% page-ref page="snowflake.md" %}

{% page-ref page="bigquery.md" %}

{% page-ref page="redshift.md" %}

{% page-ref page="spark.md" %}

{% page-ref page="postgres.md" %}
