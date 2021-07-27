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
