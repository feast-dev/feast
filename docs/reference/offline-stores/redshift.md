# Redshift offline store

## Description

The Redshift offline store provides support for reading [RedshiftSources](../data-sources/redshift.md).

* All joins happen within Redshift. 
* Entity dataframes can be provided as a SQL query or can be provided as a Pandas dataframe. A Pandas dataframes will be uploaded to Redshift temporarily in order to complete join operations.

## Getting started
In order to use this offline store, you'll need to run `pip install 'feast[aws]'`. You can get started by then running `feast init -t aws`.

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

The full set of configuration options is available in [RedshiftOfflineStoreConfig](https://rtd.feast.dev/en/master/#feast.infra.offline_stores.redshift.RedshiftOfflineStoreConfig).

## Functionality Matrix

The set of functionality supported by offline stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the Redshift offline store.

|                                                                    | Redshift |
| :----------------------------------------------------------------- | :------- |
| `get_historical_features` (point-in-time correct join)             | yes      |
| `pull_latest_from_table_or_query` (retrieve latest feature values) | yes      |
| `pull_all_from_table_or_query` (retrieve a saved dataset)          | yes      |
| `offline_write_batch` (persist dataframes to offline store)        | yes      |
| `write_logged_features` (persist logged features to offline store) | yes      |

Below is a matrix indicating which functionality is supported by `RedshiftRetrievalJob`.

|                                                       | Redshift |
| ----------------------------------------------------- | -------- |
| export to dataframe                                   | yes      |
| export to arrow table                                 | yes      |
| export to arrow batches                               | yes      |
| export to SQL                                         | yes      |
| export to data lake (S3, GCS, etc.)                   | no       |
| export to data warehouse                              | yes      |
| export as Spark dataframe                             | no       |
| local execution of Python-based on-demand transforms  | yes      |
| remote execution of Python-based on-demand transforms | no       |
| persist results in the offline store                  | yes      |
| preview the query plan before execution               | yes      |
| read partitioned data                                 | yes      |

To compare this set of functionality against other offline stores, please see the full [functionality matrix](overview.md#functionality-matrix).

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


## Redshift Serverless

In order to use [AWS Redshift Serverless](https://aws.amazon.com/redshift/redshift-serverless/), specify a workgroup instead of a cluster_id and user.

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: aws
offline_store:
  type: redshift
  region: us-west-2
  workgroup: feast-workgroup
  database: feast-database
  s3_staging_location: s3://feast-bucket/redshift
  iam_role: arn:aws:iam::123456789012:role/redshift_s3_access_role
```
{% endcode %}

Please note that the IAM policies above will need the [redshift-serverless](https://aws.permissions.cloud/iam/redshift-serverless) version, rather than the standard [redshift](https://aws.permissions.cloud/iam/redshift).