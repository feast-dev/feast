# Docker Compose

### Overview

This guide will bring Feast up using Docker Compose. This will allow you to:

* Create, register, and manage [feature sets](../../user-guide/feature-sets.md)
* Ingest feature data into Feast
* Retrieve features for online serving
* Retrieve features for historical serving \(training\)

### 0. Requirements

* [Docker Compose](https://docs.docker.com/compose/install/) should be installed.
* a [GCP service account](https://cloud.google.com/iam/docs/creating-managing-service-account-keys) that has access to [Google Cloud Storage](https://cloud.google.com/storage) and [BigQuery](https://cloud.google.com/bigquery) \(for historical serving only\).
* [Google Cloud SDK ](https://cloud.google.com/sdk/install)installed, authenticated, and configured to the GCP project you want to use \(for historical serving only\).

## 1. Set up environment

Clone the latest stable version of the [Feast repository](https://github.com/gojek/feast/) and navigate to the `infra/docker-compose` sub-directory:

```
git clone --depth 1 --branch v0.6.2 https://github.com/feast-dev/feast.git
cd feast/infra/docker-compose
```

Make a copy of the `.env.sample` file:

```bash
cp .env.sample .env
```

## 2. Start Feast for Online Serving

Run the following command if you would like to start both Feast Core and Feast Serving at the same time. The Feast Serving online deployment will use Redis as its database.

```javascript
docker-compose up -d
```

Once Feast comes up you should be able to connect to a local Jupyter notebook that contains Feast examples. This may take a few minutes.

{% embed url="http://localhost:8888/tree?" %}

## 3. Start Feast for Training and Online Serving

{% hint style="info" %}
Historical serving requires Google Cloud Platform to function, specifically Google Cloud Storage \(GCS\) and BigQuery.
{% endhint %}

### 3.1 Set up Google Cloud Platform

Create a [service account ](https://cloud.google.com/iam/docs/creating-managing-service-accounts)and add it to your docker compose configuration:

```javascript
cp key.json ${FEAST_HOME_DIR}/infra/docker-compose/gcp-service-accounts
```

Temporarily activate the service account to setup your GCS bucket and BigQuery dataset, or alternatively you will need to grant access to these if you use a different account for provisioning the resources.

```text
gcloud auth activate-service-account --key-file=key.json
```

Create a Google Cloud Storage bucket that Feast will use to load data into and out of BigQuery

```bash
gsutil mb gs://my-feast-staging-bucket
```

Create a BigQuery dataset for Feast to store historical data:

```bash
bq --location=US mk --dataset my_project:feast
```

### 3.2 Configure Docker Compose

Configure the `.env` file based on your environment. At the very least you have to modify:

| Parameter | Description |
| :--- | :--- |
| `GCP_SERVICE_ACCOUNT` | This should be your service account file path, for example `./gcp-service-accounts/key.json`. |
| `FEAST_HISTORICAL_SERVING_ENABLED` | Set this to `true` to enable historical serving \(BigQuery\) |

### 3.3 Configure Services

The following configuration has to be set in `serving/historical-serving.yml`

| Parameter | Description |
| :--- | :--- |
| `feast.stores.config.project_id` | This is your [GCP project Id](https://cloud.google.com/resource-manager/docs/creating-managing-projects). |
| `feast.stores.config.dataset_id` | This is the name of the BigQuery dataset that you created above |
| `feast.stores.config.staging_location` | This is the staging location on Google Cloud Storage for retrieval of training datasets, created above. Keep a suffix. |

The following configuration has to be set in `core/core.yml`

| Parameter | Description |
| :--- | :--- |
| `feast.jobs.runners.options.tempLocation` | Beam ingestion jobs will persist data here before loading it into BigQuery. Use the same bucket as above and keep a suffix. |

### 3.4 Start Feast

Start Feast:

```javascript
docker-compose up -d
```

Once Feast comes up you should be able to connect to a local Jupyter notebook that contains Feast examples. This may take a few minutes.

{% embed url="http://localhost:8888/tree?" %}

## 

