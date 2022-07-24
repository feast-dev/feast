# DynamoDB

## Description

The [DynamoDB](https://aws.amazon.com/dynamodb/) online store provides support for materializing feature values into AWS DynamoDB.

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

Configuration options are available [here](https://github.com/feast-dev/feast/blob/17bfa6118d6658d2bff53d7de8e2ccef5681714d/sdk/python/feast/infra/online_stores/dynamodb.py#L36).

## Permissions

Feast requires the following permissions in order to execute commands for DynamoDB online store:

| **Command**             | Permissions                                                                         | Resources                                         |
| ----------------------- | ----------------------------------------------------------------------------------- | ------------------------------------------------- |
| **Apply**               | <p>dynamodb:CreateTable</p><p>dynamodb:DescribeTable</p><p>dynamodb:DeleteTable</p> | arn:aws:dynamodb:\<region>:\<account_id>:table/\* |
| **Materialize**         | dynamodb.BatchWriteItem                                                             | arn:aws:dynamodb:\<region>:\<account_id>:table/\* |
| **Get Online Features** | dynamodb.BatchGetItem                                                                    | arn:aws:dynamodb:\<region>:\<account_id>:table/\* |

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
