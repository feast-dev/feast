# Amazon Web Services

## Description

* Offline Store: Uses the **Redshift** offline store by default. Also supports File as the offline store.
* Online Store: Uses the **DynamoDB** online store by default. Also supports Sqlite as an online store.

## Example

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: aws
online_store:
  type: dynamodb
  region: us-west-2
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
