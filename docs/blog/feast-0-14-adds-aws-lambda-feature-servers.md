# Feast 0.14 adds AWS Lambda feature servers

*October 23, 2021* | *Tsotne Tabidze, Felix Wang*

We are delighted to announce the release of [Feast 0.14](https://github.com/feast-dev/feast/releases/tag/v0.14.0), which introduces a new feature and several important improvements:

* [Experimental] AWS Lambda feature servers, which allow you to quickly deploy an HTTP server to serve online features on AWS Lambda. GCP Cloud Run and Java feature servers are coming soon!
* Bug fixes around performance. The core online serving path is now significantly faster.
* Improvements for developer experience. The integration tests are now faster, and temporary tables created during integration tests are immediately dropped after the test.

Experimental features are subject to API changes in the near future as we collect feedback. If you have thoughts, please don't hesitate to reach out to the Feast team!

### [Experimental] AWS Lambda feature servers

Prior to Feast 0.13, the only way for users to retrieve online features was to use the Python SDK. This was restrictive, so Feast 0.13 introduced local Python feature servers, allowing users to deploy a local HTTP server to serve their online features. Feast 0.14 now allows users to deploy a feature server on AWS Lambda to quickly serve features at scale. The new AWS Lambda feature servers are available for feature stores using the AWS provider.

To deploy a feature server to AWS Lambda, they must be enabled and be given the appropriate permissions:

```yaml
project: dev
registry: s3://feast/registries/dev
provider: aws
online_store:
  region: us-west-2
offline_store:
  cluster_id: feast
  region: us-west-2
  user: admin
  database: feast
  s3_staging_location: s3://feast/redshift/tests/staging_location
  iam_role: arn:aws:iam::{aws_account}:role/redshift_s3_access_role
flags:
  alpha_features: true
  aws_lambda_feature_server: true
feature_server:
  enabled: True
  execution_role_name: arn:aws:iam::{aws_account}:role/lambda_execution_role
```

Calling `feast apply` will then deploy the feature server. The precise endpoint can be determined with by calling `feast endpoint`, and the endpoint can then be queried as follows:

See [AWS Lambda feature server](https://docs.feastsite.wpenginepowered.com/reference/feature-servers/aws-lambda) for detailed info on how to use this functionality.

### Performance bug fixes and developer experience improvements

The provider for a feature store is now cached instead of being instantiated repeatedly, making the core online serving path 30% faster.

Integration tests now run significantly faster on Github Actions due to caching. Also, tables created during integration tests were previously not always cleaned up properly; now they are always deleted immediately after the integration tests finish.

### What's next

We are collaborating with the community on supporting streaming sources, low latency serving, a Python feature transformation server for on demand transforms, improved support for Kubernetes deployments, and more.

In addition, there is active community work on building Hive, Snowflake, Azure, Astra, Presto, and Alibaba Cloud connectors. If you have thoughts on what to build next in Feast, please fill out this [form](https://docs.google.com/forms/d/e/1FAIpQLSfa1nR).

Download Feast 0.14 today from [PyPI](https://pypi.org/project/feast/) (or pip install feast) and try it out! Let us know on our [slack channel](http://slack.feastsite.wpenginepowered.com/).
