# Feast 0.12 adds AWS Redshift and DynamoDB stores

*August 11, 2021* | *Jules S. Damji, Tsotne Tabidze, and Achal Shah*

We are delighted to announce [Feast 0.12](https://github.com/feast-dev/feast/blob/master/CHANGELOG.md) is released! With this release, Feast users can take advantage of AWS technologies such as Redshift and DynamoDB as feature store backends to power their machine learning models. We want to share three key additions that extend Feast's ecosystem and facilitate a convenient way to group features via a Feature Service for serving:

1. Adding [AWS Redshift](https://aws.amazon.com/redshift/), a cloud data warehouse, as an offline store, which supports features serving for training and batch inference at high throughput

Let's briefly take a peek at each and how easily you can use them through simple declarative APIs and configuration changes.

### AWS Redshift as a feature store data source and an offline store

Redshift data source allows you to fetch historical feature values from Redshift for building training datasets and materializing features into an online store (see below how to materialize). A data source is defined as part of the [Feast Declarative API](https://rtd.feastsite.wpenginepowered.com/en/latest/) in the feature repo directory's Python files. For example, `aws_datasource.py` defines a table from which we want to fetch features.

```python
from feast import RedshiftSource

my_redshift_source = RedshiftSource(table="redshift_driver_table")
```

### AWS DynamoDB as an online store

To allow teams to scale up and support high volumes of online transactions requests for machine learning (ML) predictions, Feast now supports a scalable DynamoDB to serve fresh features to your model in production in the AWS cloud. To enable DynamoDB as your online store, just change `featore_store.yaml`:

```yaml
project: fraud_detection
registry: data/registry.db
provider: aws
online_store:
  type: dynamodb
  region: us-west-2
```

To materialize your features into your DynamoDB online store, simply issue the command:

```bash
$ feast materialize
```

Use a Feature Service when you want to logically group features from multiple Feature Views. This way, when requested from Feast, all features will be returned from the feature store. `feature_store.get_historical_features(...)` and `feature_store.get_online_features(...)`

### What's next

We are working on a Feast tutorial use case on AWS, meanwhile you can check out other [tutorials in documentation](https://docs.feastsite.wpenginepowered.com/). For more documentation about the aforementioned features, check the following Feast links:

* [Online stores](https://docs.feastsite.wpenginepowered.com/reference/online-stores/)
