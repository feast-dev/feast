# Feast - Feature Store for Machine Learning

## Gradient Deployment

NOTE: This guide assumes your `FEAST_GKE_CLUSTER_NAME` is `feast`. Adjust as necessary when creating a brand new cluster.

### Deploying a new cluster

#### 0. Pre-reqs

1. [Google Cloud SDK ](https://cloud.google.com/sdk/install)installed, authenticated, and configured to the project you will use.
2. [Kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/) installed.
3. [Helm](https://helm.sh/3) \(2.16.0 or greater\) installed on your local machine with Tiller installed in your cluster. *Helm 3 has not been tested yet, so install helm 2.*

```bash
brew install helm@2
```

Add the following environment variables:
```bash
# FEAST
export FEAST_GCP_PROJECT_ID=gradient-decision
export FEAST_GCP_REGION=us-east1
export FEAST_GCP_ZONE=us-east1-a
export FEAST_BIGQUERY_DATASET_ID=feast
export FEAST_GCS_BUCKET=${FEAST_GCP_PROJECT_ID}_feast_bucket
export FEAST_GKE_CLUSTER_NAME=feast
export FEAST_S_ACCOUNT_NAME=feast-sa
export FEAST_HOME_DIR=/Users/lionel/code/feast
export FEAST_IP=$(kubectl describe nodes | grep ExternalIP | awk '{print $2}' | head -n 1)
export FEAST_CORE_HOST=${FEAST_IP}
export FEAST_CORE_GRPC_PORT=32090
export FEAST_CORE_URL=${FEAST_IP}:32090
export FEAST_ONLINE_SERVING_URL=${FEAST_IP}:32091
export FEAST_BATCH_SERVING_URL=${FEAST_IP}:32092
```

#### 1. Follow [steps 1-3 in original GKE guide](https://docs.feast.dev/getting-started/installing-feast#3-set-up-helm) to provision GCP resources and set up Helm.

#### 2. Install feast onto the GKE cluster using helm.

```bash
git clone https://github.com/gojek/feast.git && cd feast && \
git checkout 0.3-dev && \
export FEAST_HOME_DIR=$(pwd) && \
cd infra/charts/feast
helm install --name feast -f gradient-values.yaml .
```

#### 3. Install and configure feast Python SDK

Install via pip:
```bash
pip install feast
```

Configure the Feast Python SDK:
```

```bash
feast config set core_url ${FEAST_CORE_URL}
feast config set serving_url ${FEAST_ONLINE_SERVING_URL}
```

### Updating a cluster

#### 1. Make changes to helm chart

```bash
infra/charts/feast/gradient-values.yaml
```

#### 2. Deploy helm chart

```
helm upgrade --name feast -f infra/charts/feast/gradient-values.yaml
```

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

## Important resources

Please refer to the official docs at <https://feast.dev>

 * [Why Feast?](https://docs.feast.dev/why-feast)
 * [Concepts](https://docs.feast.dev/concepts)
 * [Installation](https://docs.feast.dev/getting-started/installing-feast)
 * [Getting Help](https://docs.feast.dev/getting-help)
 * [Example Notebook](https://github.com/gojek/feast/blob/master/examples/basic/basic.ipynb)

## Notice

Feast is a community project and is still under active development. Your feedback and contributions are important to us. Please have a look at our [contributing guide](docs/contributing.md) for details.
