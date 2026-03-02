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

## Configuration

Below is a example with performance tuning options:

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: aws
online_store:
  type: dynamodb
  region: us-west-2
  batch_size: 100
  max_read_workers: 10
  consistent_reads: false
```
{% endcode %}

### Configuration Options

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `region` | string | | AWS region for DynamoDB |
| `table_name_template` | string | `{project}.{table_name}` | Template for table names |
| `batch_size` | int | `40` | Number of items per BatchGetItem/BatchWriteItem request (max 100) |
| `max_read_workers` | int | `10` | Maximum parallel threads for batch read operations. Higher values improve throughput for large batch reads but increase resource usage |
| `consistent_reads` | bool | `false` | Whether to use strongly consistent reads (higher latency, guaranteed latest data) |
| `tags` | dict | `null` | AWS resource tags added to each table |
| `session_based_auth` | bool | `false` | Use AWS session-based client authentication |

### Performance Tuning

**Parallel Batch Reads**: When reading features for many entities, DynamoDB's BatchGetItem is limited to 100 items per request. For 500 entities, this requires 5 batch requests. The `max_read_workers` option controls how many of these batches execute in parallel:

- **Sequential (old behavior)**: 5 batches × 10ms = 50ms total
- **Parallel (with `max_read_workers: 10`)**: 5 batches in parallel ≈ 10ms total

For high-throughput workloads with large entity counts, increase `max_read_workers` (up to 20-30) based on your DynamoDB capacity and network conditions.

**Batch Size**: Increase `batch_size` up to 100 to reduce the number of API calls. However, larger batches may hit DynamoDB's 16MB response limit for tables with large feature values.

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
