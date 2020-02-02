# Feast - Feature Store for Machine Learning

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
docker-compose up -d
```

A [Jupyter Notebook](http://localhost:8888/tree/feast/examples) is now available to start using Feast.

Please see the links below to set up Feast for batch/historical serving with BigQuery.

## Important resources

Please refer to the official documentation at <https://docs.feast.dev>

 * [Why Feast?](https://docs.feast.dev/why-feast)
 * [Concepts](https://docs.feast.dev/concepts)
 * [Installation](https://docs.feast.dev/installing-feast/overview)
 * [Examples](https://github.com/gojek/feast/blob/master/examples/)
 * [Change Log](https://github.com/gojek/feast/blob/master/CHANGELOG.md)
 * [Slack (#Feast)](https://join.slack.com/t/kubeflow/shared_invite/enQtNDg5MTM4NTQyNjczLTdkNTVhMjg1ZTExOWI0N2QyYTQ2MTIzNTJjMWRiOTFjOGRlZWEzODc1NzMwNTMwM2EzNjY1MTFhODczNjk4MTk)

## Notice

Feast is a community project and is still under active development. Your feedback and contributions are important to us. Please have a look at our [contributing guide](docs/contributing.md) for details.
