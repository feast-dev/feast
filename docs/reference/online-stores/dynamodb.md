# DynamoDB

### Description

The [DynamoDB](https://aws.amazon.com/dynamodb/) online store provides support for materializing feature values into AWS DynamoDB.
<!---

TODO: Add DynamoDB to online store format document and point to it.

The data model used to store feature values in DynamoDB is described in more detail [here](https://github.com/feast-dev/feast/blob/master/docs/specs/online_store_format.md#google-datastore-online-store-format).

-->

### Example

{% code title="feature\_store.yaml" %}
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

### Permissions

Feast requires the following permissions in order to execute commands for DynamoDB online store:

<table>
  <thead>
    <tr>
      <th style="text-align:left"><b>Command</b></th>
      <th style="text-align:left">Permissions</th>
      <th style="text-align:left">Resources</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align:left"><b>Apply</b></td>
      <td style="text-align:left">
        <p>dynamodb:CreateTable</p>
        <p>dynamodb:DescribeTable</p>
        <p>dynamodb:DeleteTable</p>
      </td>
      <td style="text-align:left">arn:aws:dynamodb:&lt;region&gt;:&lt;account_id&gt;:table/*</td>
    </tr>
    <tr>
      <td style="text-align:left"><b>Materialize</b></td>
      <td style="text-align:left">
        <p>dynamodb.BatchWriteItem</p>
      </td>
      <td style="text-align:left">arn:aws:dynamodb:&lt;region&gt;:&lt;account_id&gt;:table/*</td>
    </tr>
    <tr>
      <td style="text-align:left"><b>Get Online Features</b></td>
      <td style="text-align:left">dynamodb.GetItem</td>
      <td style="text-align:left">arn:aws:dynamodb:&lt;region&gt;:&lt;account_id&gt;:table/*</td>
    </tr>
  </tbody>
</table>

The following inline policy can be used to grant Feast the necessary permissions:

```json
{
    "Statement": [
        {
            "Action": [
                "dynamodb:CreateTable",
                "dynamodb:DescribeTable",
                "dynamodb:DeleteTable",
                "dynamodb:BatchWriteItem",
                "dynamodb:GetItem"
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