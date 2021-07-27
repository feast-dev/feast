# Redshift

### Description

The Redshift offline store provides support for reading [RedshiftSources](../data-sources/redshift.md).

* Redshift tables and views are allowed as sources.
* All joins happen within Redshift. 
* Entity dataframes can be provided as a SQL query or can be provided as a Pandas dataframe. Pandas dataframes will be uploaded to Redshift in order to complete join operations.
* A [RedshiftRetrievalJob](https://github.com/feast-dev/feast/blob/bf557bcb72c7878a16dccb48443bbbe9dc3efa49/sdk/python/feast/infra/offline_stores/redshift.py#L161) is returned when calling `get_historical_features()`.

### Example

{% code title="feature\_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: aws
offline_store:
  type: redshift
  region: us-west-2
  cluster_id: feast-cluster
  database: feast-database
  user: redshift-user
  s3_staging_location: s3://feast-bucket/redshift
  iam_role: arn:aws:iam::123456789012:role/redshift_s3_access_role
```
{% endcode %}

Configuration options are available [here](https://github.com/feast-dev/feast/blob/bf557bcb72c7878a16dccb48443bbbe9dc3efa49/sdk/python/feast/infra/offline_stores/redshift.py#L22).

