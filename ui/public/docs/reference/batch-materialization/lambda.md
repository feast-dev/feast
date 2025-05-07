# AWS Lambda (alpha)

## Description

The AWS Lambda batch materialization engine is considered alpha status. It relies on the offline store to output feature values to S3 via `to_remote_storage`, and then loads them into the online store.

See [LambdaMaterializationEngineConfig](https://rtd.feast.dev/en/master/index.html?highlight=LambdaMaterializationEngine#feast.infra.materialization.aws_lambda.lambda_engine.LambdaMaterializationEngineConfig) for configuration options.

See also [Dockerfile](https://github.com/feast-dev/feast/blob/master/sdk/python/feast/infra/materialization/aws_lambda/Dockerfile) for a Dockerfile that can be used below with `materialization_image`.

## Example

{% code title="feature_store.yaml" %}
```yaml
...
offline_store:
  type: snowflake.offline
...
batch_engine:
  type: lambda
  lambda_role: [your iam role]
  materialization_image: [image uri of above Docker image]
```
{% endcode %}
