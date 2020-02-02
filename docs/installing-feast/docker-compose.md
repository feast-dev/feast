# Docker Compose

### Overview

This guide will bring Feast up using Docker Compose. This will allow you to:

* Create, register, and manage feature sets
* Ingest feature data into Feast
* Retrieve features for online serving
* Retrieve features for batch serving \(only if using Google Cloud Platform\)

This guide is split into three parts:

1. Setting up your environment
2. Starting Feast with **online serving support only** \(does not require GCP\).
3. Starting Feast with support for **both online and batch** serving \(requires GCP\)

{% hint style="info" %}
The docker compose setup uses Direct Runner for the Apache Beam jobs that populate data stores. Running Beam with the Direct Runner means it does not need a dedicated runner like Flink or Dataflow, but this comes at the cost of performance. We recommend the use of a dedicated runner when running Feast with very large workloads.
{% endhint %}

### 0. Requirements

* [Docker compose](https://docs.docker.com/compose/install/) must be installed.
* The following list of TCP ports must be free:
  * 6565, 6566, 8888, and 9094.
  * Alternatively it is possible to modify port mappings in  `/docker-compose/docker-compose.yml`.
* \(for batch serving only\) For batch serving you will also need a [GCP service account key](https://cloud.google.com/iam/docs/creating-managing-service-account-keys) that has access to [Google Cloud Storage](https://cloud.google.com/storage) and [BigQuery](https://cloud.google.com/bigquery).
* \(for batch serving only\) [Google Cloud SDK ](https://cloud.google.com/sdk/install)installed, authenticated, and configured to the project you will use.

## 1. Set up environment

Clone the [Feast repository](https://github.com/gojek/feast/) and navigate to the `docker-compose` sub-directory:

```bash
git clone https://github.com/gojek/feast.git && \
cd feast && export FEAST_HOME_DIR=$(pwd) && \
cd infra/docker-compose
```

Make a copy of the `.env.sample` file:

```bash
cp .env.sample .env
```

## 2. Docker Compose for Online Serving Only

### 2.1 Start Feast \(without batch retrieval support\)

If you do not require batch serving, then its possible to simply bring up Feast:

```javascript
docker-compose up -d
```

A Jupyter Notebook environment is now available to use Feast:

[http://localhost:8888/tree/feast/examples](http://localhost:8888/tree/feast/examples)

## 3. Docker Compose for Online and Batch Serving

{% hint style="info" %}
Batch serving requires Google Cloud Storage to function, specifically Google Cloud Storage \(GCP\) and BigQuery.
{% endhint %}

### 3.1 Set up Google Cloud Platform

Create a [service account ](https://cloud.google.com/iam/docs/creating-managing-service-accounts)from the GCP console and copy it to the `infra/docker-compose/gcp-service-accounts` folder:

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
| FEAST\_CORE\_GCP\_SERVICE\_ACCOUNT\_KEY | This should be your service account file name, for example `key.json`. |
| FEAST\_BATCH\_SERVING\_GCP\_SERVICE\_ACCOUNT\_KEY | This should be your service account file name, for example `key.json` |
| FEAST\_JUPYTER\_GCP\_SERVICE\_ACCOUNT\_KEY | This should be your service account file name, for example `key.json` |
| FEAST\_JOB\_STAGING\_LOCATION | Google Cloud Storage bucket that Feast will use to stage data exports and batch retrieval requests, for example `gs://your-gcs-bucket/staging` |

### 3.3 Configure .bq-store.yml

We will also need to configure the `bq-store.yml` file inside `infra/docker-compose/serving/` to configure the BigQuery storage configuration as well as the feature sets that the store subscribes to. At a minimum you will need to set:

| Parameter | Description |
| :--- | :--- |
| bigquery\_config.project\_id | This is you [GCP project Id](https://cloud.google.com/resource-manager/docs/creating-managing-projects). |
| bigquery\_config.dataset\_id | This is the name of the BigQuery dataset that tables will be created in. Each feature set will have one table in BigQuery. |

### 3.4 Start Feast \(with batch retrieval support\)

Start Feast:

```javascript
docker-compose up -d
```

A Jupyter Notebook environment is now available to use Feast:

[http://localhost:8888/tree/feast/examples](http://localhost:8888/tree/feast/examples)

