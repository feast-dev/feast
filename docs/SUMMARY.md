# Table of contents

* [Introduction](README.md)
* [Community](community.md)
* [Roadmap](roadmap.md)
* [Changelog](https://github.com/feast-dev/feast/blob/master/CHANGELOG.md)

## Getting started

* [Quickstart](getting-started/quickstart.md)
* [Concepts](getting-started/concepts/README.md)
  * [Overview](getting-started/concepts/overview.md)
  * [Data model](getting-started/concepts/data-model-and-concepts/README.md)
    * [Data source](getting-started/concepts/data-model-and-concepts/data-source.md)
    * [Entity](getting-started/concepts/data-model-and-concepts/entity.md)
    * [Feature view](getting-started/concepts/data-model-and-concepts/feature-view.md)
    * [Feature service](getting-started/concepts/data-model-and-concepts/feature-service.md)
    * [Feature retrieval](getting-started/concepts/data-model-and-concepts/feature-retrieval.md)
  * [Architecture](getting-started/concepts/architecture-and-components/README.md)
    * [Overview](getting-started/concepts/architecture-and-components/overview.md)
    * [Feature repository](getting-started/concepts/architecture-and-components/feature-repository.md)
    * [Offline store](getting-started/concepts/architecture-and-components/offline-store.md)
    * [Online store](getting-started/concepts/architecture-and-components/online-store.md)
    * [Provider](getting-started/concepts/architecture-and-components/provider.md)
* [FAQ](getting-started/faq.md)

## Tutorials

* [Overview](tutorials/tutorials-overview.md)
* [Driver Ranking](tutorials/driver-ranking-with-feast.md)
* [Fraud Detection](tutorials/fraud-detection.md)

## How-to Guides

* [Running Feast with GCP/AWS](how-to-guides/feast-gcp-aws/README.md)
  * [Install Feast](how-to-guides/feast-gcp-aws/install-feast.md)
  * [Create a feature repository](how-to-guides/feast-gcp-aws/create-a-feature-repository.md)
  * [Deploy a feature store](how-to-guides/feast-gcp-aws/deploy-a-feature-store.md)
  * [Build a training dataset](how-to-guides/feast-gcp-aws/build-a-training-dataset.md)
  * [Load data into the online store](how-to-guides/feast-gcp-aws/load-data-into-the-online-store.md)
  * [Read features from the online store](how-to-guides/feast-gcp-aws/read-features-from-the-online-store.md)
* [Running Feast in production](how-to-guides/running-feast-in-production.md)
* [Upgrading from Feast 0.9](https://docs.google.com/document/u/1/d/1AOsr_baczuARjCpmZgVd8mCqTF4AZ49OEyU4Cn-uTT0/edit)
* [Adding a custom provider](how-to-guides/creating-a-custom-provider.md)
* [Adding a new online store](how-to-guides/adding-support-for-a-new-online-store.md)
* [Adding a new offline store](how-to-guides/adding-a-new-offline-store.md)

## Reference

* [Data sources](reference/data-sources/README.md)
  * [File](reference/data-sources/file.md)
  * [BigQuery](reference/data-sources/bigquery.md)
  * [Redshift](reference/data-sources/redshift.md)
* [Offline stores](reference/offline-stores/README.md)
  * [File](reference/offline-stores/file.md)
  * [BigQuery](reference/offline-stores/bigquery.md)
  * [Redshift](reference/offline-stores/redshift.md)
* [Online stores](reference/online-stores/README.md)
  * [SQLite](reference/online-stores/sqlite.md)
  * [Redis](reference/online-stores/redis.md)
  * [Datastore](reference/online-stores/datastore.md)
  * [DynamoDB](reference/online-stores/dynamodb.md)
* [Providers](reference/providers/README.md)
  * [Local](reference/providers/local.md)
  * [Google Cloud Platform](reference/providers/google-cloud-platform.md)
  * [Amazon Web Services](reference/providers/amazon-web-services.md)
* [Feature repository](reference/feature-repository/README.md)
  * [feature\_store.yaml](reference/feature-repository/feature-store-yaml.md)
  * [.feastignore](reference/feature-repository/feast-ignore.md)
* [Feast CLI reference](reference/feast-cli-commands.md)
* [Python API reference](http://rtd.feast.dev/)
* [Usage](reference/usage.md)

## Project

* [Contribution process](project/contributing.md)
* [Development guide](project/development-guide.md)
* [Versioning policy](project/versioning-policy.md)
* [Release process](project/release-process.md)
* [Feast 0.9 vs Feast 0.10+](project/feast-0.9-vs-feast-0.10+.md)

