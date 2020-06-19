# Docker Compose

### Overview

This guide will bring Feast up using Docker Compose. This will allow you to:

* Create, register, and manage [feature sets](../../user-guide/feature-sets.md)
* Ingest feature data into Feast
* Retrieve features for online serving
* Retrieve features for batch serving \(only if using Google Cloud Platform\)

### 0. Requirements

* [Docker Compose](https://docs.docker.com/compose/install/) should be installed
* [GCP service account](https://cloud.google.com/iam/docs/creating-managing-service-account-keys) that has access to [Google Cloud Storage](https://cloud.google.com/storage) and [BigQuery](https://cloud.google.com/bigquery) \(for historical serving only\).
* [Google Cloud SDK ](https://cloud.google.com/sdk/install)installed, authenticated, and configured to the GCP project you want to use \(for historical serving only\).

## 1. Set up environment

Clone the [Feast repository](https://github.com/gojek/feast/) and navigate to the `infra/docker-compose` sub-directory:

Make a copy of the `.env.sample` file:

```bash
cp .env.sample .env
```

## 2. Start Feast for Online Serving

Run the following command if you would like to start both Feast Core and Feast Serving at the same time. The Feast Serving online deployment will use Redis as its database.

```javascript
docker-compose \
-f docker-compose.yml \
-f docker-compose.online.yml \
up -d
```

Your Feast deployment should now be starting up. Have a look at the Jupyter docker logs to know when your notebook is ready to be used:

```text
docker logs feast_jupyter_1
```

Once it is ready you should be able to connect to a local notebook that contains Feast examples. This may take a few minutes.

```text
[I 05:50:22.991 NotebookApp] The Jupyter Notebook is running at:
[I 05:50:22.991 NotebookApp] http://localhost:8888/
```

Navigate to the Jupyter Notebook container at the following URL when the container is ready.

{% embed url="http://localhost:8888/tree/feast/examples" caption="" %}

## 3. Start Feast for Training and Online Serving

{% hint style="info" %}
Batch serving requires Google Cloud Platform to function, specifically Google Cloud Storage \(GCP\) and BigQuery.
{% endhint %}

### 3.1 Set up Google Cloud Platform

Create a [service account ](https://cloud.google.com/iam/docs/creating-managing-service-accounts)and copy it to the `infra/docker-compose/gcp-service-accounts` folder:

```javascript
cp my-service-account.json ${FEAST_HOME_DIR}/infra/docker-compose/gcp-service-accounts
```

Create a Google Cloud Storage bucket. Make sure that your service account above has read/write permissions to this bucket:

```bash
gsutil mb gs://my-feast-staging-bucket
```

### 3.2 Configure .env

Configure the `.env` file based on your environment. At the very least you have to modify:

| Parameter | Description |
| :--- | :--- |
| `FEAST_CORE_GCP_SERVICE_ACCOUNT_KEY` | This should be your service account file name, for example `key.json`. |
| `FEAST_BATCH_SERVING_GCP_SERVICE_ACCOUNT_KEY` | This should be your service account file name, for example `key.json` |
| `FEAST_JUPYTER_GCP_SERVICE_ACCOUNT_KEY` | This should be your service account file name, for example `key.json` |

### 3.3 Configure Historical Serving

We will also need to configure the `batch-serving.yml` file inside `infra/docker-compose/serving/`. This configuration is used to retrieve training datasets from Feast. At a minimum you will need to set:

| Parameter | Description |
| :--- | :--- |
| `feast.stores.config.project_id` | This is your [GCP project Id](https://cloud.google.com/resource-manager/docs/creating-managing-projects). |
| `feast.stores.config.dataset_id` | This is the name of the BigQuery dataset that feature data will be stored in. |
| `feast.stores.config.staging_location` | This is the staging location on Google Cloud Storage for retrieval of training datasets |

### 3.4 Start Feast \(with batch retrieval support\)

Start Feast:

```javascript
docker-compose \
-f docker-compose.yml \
-f docker-compose.online.yml \
-f docker-compose.batch.yml \
up -d
```

A Jupyter Notebook  should become available within a few minutes:

{% embed url="http://localhost:8888/tree/feast/examples" %}



