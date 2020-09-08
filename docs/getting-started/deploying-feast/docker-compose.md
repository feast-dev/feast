# Docker Compose

### Overview

This guide will give a walk-though on deploying Feast using Docker Compose. 

The Docker Compose setup is recommended if you are running Feast locally to try things out. It includes a built in Jupyter Notebook Server that is preloaded with Feast example notebooks to get you started. 

{% hint style="info" %}
The Docker Compose deployment will take some time fully startup. During this time you may see some connection failures which should be automatically corrected a few minutes.
{% endhint %}

## 0. Requirements

* [Docker Compose](https://docs.docker.com/compose/install/) should be installed.
* Additional Requirements for using Feast Historical Serving:
  * a [GCP service account](https://cloud.google.com/iam/docs/creating-managing-service-account-keys) that has access to [Google Cloud Storage](https://cloud.google.com/storage) and [BigQuery](https://cloud.google.com/bigquery).
  * [Google Cloud SDK ](https://cloud.google.com/sdk/install)installed, authenticated, and configured to the GCP project you want to use.

## 1. Set up environment

Clone the latest stable version of the [Feast repository](https://github.com/gojek/feast/) and setup before we deploy:

```text
git clone --depth 1 --branch v0.6.2 https://github.com/feast-dev/feast.git
cd feast/infra/docker-compose
cp .env.sample .env
```

## 2. Start Feast for Online Serving

Use Docker Compose deploy Feast for Online Serving only:

```javascript
docker-compose up -d
```

Once deployed, you should be able to connect at `localhost:8888` to the bundled Jupyer Notebook Server with example notebooks.

{% embed url="http://localhost:8888/tree?" caption="" %}

## 3. Start Feast for Training and Online Serving

{% hint style="info" %}
Historical serving currently requires Google Cloud Platform to function, specifically a Service Account with access to Google Cloud Storage \(GCS\) and BigQuery. 
{% endhint %}

### 3.1 Set up Google Cloud Platform

Create a service account for Feast to use. Make sure to copy the JSON key to `infra/docker-compose/gcp-service-accounts/key.json` under the cloned Feast repository. 

```bash
gcloud iam service-accounts create feast-service-account

gcloud projects add-iam-policy-binding my-gcp-project \
  --member serviceAccount:feast-service-account@my-gcp-project.iam.gserviceaccount.com \
  --role roles/editor
  

gcloud iam service-accounts keys create credentials.json --iam-account \
feast-service-account@my-gcp-project.iam.gserviceaccount.com

cp credentials.json ${FEAST_REPO}/infra/docker-compose/gcp-service-accounts/key.json
```

Create a Google Cloud Storage Bucket that Feast will use to load data into and out of BigQuery

```bash
gsutil mb gs://my-feast-staging-bucket
```

Create a BigQuery Dataset for Feast to store historical data:

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
| `feast.stores.config.staging_location` | This is the staging location on Google Cloud Storage for retrieval of training datasets, created above. Make sure you append a suffix \(ie `gs://mybucket/suffix`\) |

The following configuration has to be set in `core/core.yml`

| Parameter | Description |
| :--- | :--- |
| `feast.jobs.runners.options.tempLocation` | Beam ingestion jobs will persist data here before loading it into BigQuery. Use the same bucket as above and make sure you append a different suffix \(ie `gs://mybucket/anothersuffix`\). |

### 3.4 Start Feast

Use Docker Compose deploy Feast:

```javascript
docker-compose up -d
```

Once deployed, you should be able to connect at `localhost:8888` to the bundled Jupyer Notebook Server with example notebooks.

{% embed url="http://localhost:8888/tree?" caption="" %}

## 6. Further Reading

* [Feast Concepts](../../user-guide/overview.md)
* [Feast Examples/Tutorials](https://github.com/feast-dev/feast/tree/master/examples)
* [Configuring Feast Components](../../reference/configuration-reference.md)

