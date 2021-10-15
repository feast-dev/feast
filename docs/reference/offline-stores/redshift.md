# Redshift

## Description

The Redshift offline store provides support for reading [RedshiftSources](../data-sources/redshift.md).

* Redshift tables and views are allowed as sources.
* All joins happen within Redshift. 
* Entity dataframes can be provided as a SQL query or can be provided as a Pandas dataframe. Pandas dataframes will be uploaded to Redshift in order to complete join operations.
* A [RedshiftRetrievalJob](https://github.com/feast-dev/feast/blob/bf557bcb72c7878a16dccb48443bbbe9dc3efa49/sdk/python/feast/infra/offline_stores/redshift.py#L161) is returned when calling `get_historical_features()`.

## Example

{% code title="feature_store.yaml" %}
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

## Permissions

Feast requires the following permissions in order to execute commands for Redshift offline store:

| **Command**                 | Permissions                                                                      | Resources                                                                                                                                                                                                                                                                                                                           |
| --------------------------- | -------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Apply**                   | <p>redshift-data:DescribeTable</p><p>redshift:GetClusterCredentials</p>          | <p>arn:aws:redshift:&#x3C;region>:&#x3C;account_id>:dbuser:&#x3C;redshift_cluster_id>/&#x3C;redshift_username></p><p>arn:aws:redshift:&#x3C;region>:&#x3C;account_id>:dbname:&#x3C;redshift_cluster_id>/&#x3C;redshift_database_name></p><p>arn:aws:redshift:&#x3C;region>:&#x3C;account_id>:cluster:&#x3C;redshift_cluster_id></p> |
| **Materialize**             | redshift-data:ExecuteStatement                                                   | arn:aws:redshift:\<region>:\<account_id>:cluster:\<redshift_cluster_id>                                                                                                                                                                                                                                                             |
| **Materialize**             | redshift-data:DescribeStatement                                                  | \*                                                                                                                                                                                                                                                                                                                                  |
| **Materialize**             | <p>s3:ListBucket</p><p>s3:GetObject</p><p>s3:DeleteObject</p>                    | <p>arn:aws:s3:::&#x3C;bucket_name></p><p>arn:aws:s3:::&#x3C;bucket_name>/*</p>                                                                                                                                                                                                                                                      |
| **Get Historical Features** | <p>redshift-data:ExecuteStatement</p><p>redshift:GetClusterCredentials</p>       | <p>arn:aws:redshift:&#x3C;region>:&#x3C;account_id>:dbuser:&#x3C;redshift_cluster_id>/&#x3C;redshift_username></p><p>arn:aws:redshift:&#x3C;region>:&#x3C;account_id>:dbname:&#x3C;redshift_cluster_id>/&#x3C;redshift_database_name></p><p>arn:aws:redshift:&#x3C;region>:&#x3C;account_id>:cluster:&#x3C;redshift_cluster_id></p> |
| **Get Historical Features** | redshift-data:DescribeStatement                                                  | \*                                                                                                                                                                                                                                                                                                                                  |
| **Get Historical Features** | <p>s3:ListBucket</p><p>s3:GetObject</p><p>s3:PutObject</p><p>s3:DeleteObject</p> | <p>arn:aws:s3:::&#x3C;bucket_name></p><p>arn:aws:s3:::&#x3C;bucket_name>/*</p>                                                                                                                                                                                                                                                      |

The following inline policy can be used to grant Feast the necessary permissions:

```javascript
{
    "Statement": [
        {
            "Action": [
                "s3:ListBucket",
                "s3:PutObject",
                "s3:GetObject",
                "s3:DeleteObject"
            ],
            "Effect": "Allow",
            "Resource": [
                "arn:aws:s3:::<bucket_name>/*",
                "arn:aws:s3:::<bucket_name>"
            ]
        },
        {
            "Action": [
                "redshift-data:DescribeTable",
                "redshift:GetClusterCredentials",
                "redshift-data:ExecuteStatement"
            ],
            "Effect": "Allow",
            "Resource": [
                "arn:aws:redshift:<region>:<account_id>:dbuser:<redshift_cluster_id>/<redshift_username>",
                "arn:aws:redshift:<region>:<account_id>:dbname:<redshift_cluster_id>/<redshift_database_name>",
                "arn:aws:redshift:<region>:<account_id>:cluster:<redshift_cluster_id>"
            ]
        },
        {
            "Action": [
                "redshift-data:DescribeStatement"
            ],
            "Effect": "Allow",
            "Resource": "*"
        }
    ],
    "Version": "2012-10-17"
}
```

In addition to this, Redshift offline store requires an IAM role that will be used by Redshift itself to interact with S3. More concretely, Redshift has to use this IAM role to run [UNLOAD](https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html) and [COPY](https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html) commands. Once created, this IAM role needs to be configured in `feature_store.yaml` file as `offline_store: iam_role`.

The following inline policy can be used to grant Redshift necessary permissions to access S3:

```javascript
{
    "Statement": [
        {
            "Action": "s3:*",
            "Effect": "Allow",
            "Resource": [
                "arn:aws:s3:::feast-integration-tests",
                "arn:aws:s3:::feast-integration-tests/*"
            ]
        }
    ],
    "Version": "2012-10-17"
}
```

While the following trust relationship is necessary to make sure that Redshift, and only Redshift can assume this role:

```javascript
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "redshift.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```
