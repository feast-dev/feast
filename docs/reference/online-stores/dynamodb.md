# DynamoDB online store

## Description

The [DynamoDB](https://aws.amazon.com/dynamodb/) online store provides support for materializing feature values into AWS DynamoDB.

## Getting started
In order to use this online store, you'll need to run `pip install 'feast[aws]'`. You can then get started with the command `feast init REPO_NAME -t aws`.

## Example

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: aws
online_store:
  type: dynamodb
  region: us-west-2
```
{% endcode %}

The full set of configuration options is available in [DynamoDBOnlineStoreConfig](https://rtd.feast.dev/en/master/#feast.infra.online_stores.dynamodb.DynamoDBOnlineStoreConfig).

## Permissions

Feast requires the following permissions in order to execute commands for DynamoDB online store:

| **Command**             | Permissions                                                                         | Resources                                         |
| ----------------------- | ----------------------------------------------------------------------------------- | ------------------------------------------------- |
| **Apply**               | <p>dynamodb:CreateTable</p><p>dynamodb:DescribeTable</p><p>dynamodb:DeleteTable</p> | arn:aws:dynamodb:\<region>:\<account_id>:table/\* |
| **Materialize**         | dynamodb.BatchWriteItem                                                             | arn:aws:dynamodb:\<region>:\<account_id>:table/\* |
| **Get Online Features** | dynamodb.BatchGetItem                                                               | arn:aws:dynamodb:\<region>:\<account_id>:table/\* |

The following inline policy can be used to grant Feast the necessary permissions:

```javascript
{
    "Statement": [
        {
            "Action": [
                "dynamodb:CreateTable",
                "dynamodb:DescribeTable",
                "dynamodb:DeleteTable",
                "dynamodb:BatchWriteItem",
                "dynamodb:BatchGetItem"
            ],
            "Effect": "Allow",
            "Resource": [
                "arn:aws:dynamodb:<region>:<account_id>:table/*"
            ]
        }
    ],
    "Version": "2012-10-17"
}
```

Lastly, this IAM role needs to be associated with the desired Redshift cluster. Please follow the official AWS guide for the necessary steps [here](https://docs.aws.amazon.com/redshift/latest/dg/c-getting-started-using-spectrum-add-role.html).

## Functionality Matrix

The set of functionality supported by online stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the DynamoDB online store.

|                                                           | DynamoDB |
| :-------------------------------------------------------- | :------- |
| write feature values to the online store                  | yes      |
| read feature values from the online store                 | yes      |
| update infrastructure (e.g. tables) in the online store   | yes      |
| teardown infrastructure (e.g. tables) in the online store | yes      |
| generate a plan of infrastructure changes                 | no       |
| support for on-demand transforms                          | yes      |
| readable by Python SDK                                    | yes      |
| readable by Java                                          | no       |
| readable by Go                                            | no       |
| support for entityless feature views                      | yes      |
| support for concurrent writing to the same key            | no       |
| support for ttl (time to live) at retrieval               | no       |
| support for deleting expired data                         | no       |
| collocated by feature view                                | yes      |
| collocated by feature service                             | no       |
| collocated by entity key                                  | no       |

To compare this set of functionality against other online stores, please see the full [functionality matrix](overview.md#functionality-matrix).
