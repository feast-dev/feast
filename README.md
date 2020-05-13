# Feast - Feature Store for Machine Learning

[![Unit Tests](https://github.com/gojek/feast/workflows/unit%20tests/badge.svg?branch=master)](https://github.com/gojek/feast/actions?query=workflow%3A%22unit+tests%22+branch%3Amaster)
[![Code Standards](https://github.com/gojek/feast/workflows/code%20standards/badge.svg?branch=master)](https://github.com/gojek/feast/actions?query=workflow%3A%22code+standards%22+branch%3Amaster)
[![Docs latest](https://img.shields.io/badge/Docs-latest-blue.svg)](https://docs.feast.dev/)
[![GitHub Release](https://img.shields.io/github/release/gojek/feast.svg?style=flat)](https://github.com/gojek/feast/releases)

## Overview

Feast (Feature Store) is a tool for managing and serving machine learning features. Feast is the bridge between models and data.

Feast aims to:
* Provide a unified means of managing feature data from a single person to large enterprises.
* Provide scalable and performant access to feature data when training and serving models.
* Provide consistent and point-in-time correct access to feature data.
* Enable discovery, documentation, and insights into your features.

![](docs/.gitbook/assets/feast-docs-overview-diagram-2.svg)

TL;DR: Feast decouples feature engineering from feature usage. Features that are added to Feast become available immediately for training and serving. Models can retrieve the same features used in training from a low latency online store in production.
This means that new ML projects start with a process of feature selection from a catalog instead of having to do feature engineering from scratch.

```
# Setting things up
fs = feast.Client('feast.example.com')
customer_features = ['CreditScore', 'Balance', 'Age', 'NumOfProducts', 'IsActive']

# Training your model (typically from a notebook or pipeline)
data = fs.get_batch_features(customer_features, customer_entities)
my_model = ml.fit(data)

# Serving predictions (when serving the model in production)
prediction = my_model.predict(fs.get_online_features(customer_features, customer_entities))
```

## Getting Started with Docker Compose
The following commands will start Feast in online-only mode. 
```
git clone https://github.com/gojek/feast.git
cd feast/infra/docker-compose
cp .env.sample .env
docker-compose -f docker-compose.yml -f docker-compose.online.yml up -d
```

This will start a local Feast deployment with online serving. Additionally, a [Jupyter Notebook](http://localhost:8888/tree/feast/examples) with Feast examples.

Please see the links below to set up Feast for batch/historical serving with BigQuery.

## Important resources

Please refer to the official documentation at <https://docs.feast.dev>

 * [Why Feast?](https://docs.feast.dev/introduction/why-feast)
 * [Concepts](https://docs.feast.dev/concepts/concepts)
 * [Installation](https://docs.feast.dev/installation/overview)
 * [Examples](https://github.com/gojek/feast/blob/master/examples/)
 * [Roadmap](https://docs.feast.dev/roadmap)
 * [Change Log](https://github.com/gojek/feast/blob/master/CHANGELOG.md)
 * [Slack (#Feast)](https://join.slack.com/t/kubeflow/shared_invite/zt-cpr020z4-PfcAue_2nw67~iIDy7maAQ)

## Notice

Feast is a community project and is still under active development. Your feedback and contributions are important to us. Please have a look at our [contributing guide](docs/contributing/contributing.md) for details.
