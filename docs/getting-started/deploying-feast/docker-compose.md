# Docker Compose

### Overview

This guide will give a walk-though on deploying Feast using Docker Compose. 

The Docker Compose setup is recommended if you are running Feast locally to try things out. It includes a built in Jupyter Notebook Server that is preloaded with Feast example notebooks to get you started. 

## 0. Requirements

* [Docker Compose](https://docs.docker.com/compose/install/) should be installed.
* Additional Requirements for using Feast Historical Serving:
  * a [GCP service account](https://cloud.google.com/iam/docs/creating-managing-service-account-keys) that has access to [Google Cloud Storage](https://cloud.google.com/storage) and [BigQuery](https://cloud.google.com/bigquery).
  * [Google Cloud SDK ](https://cloud.google.com/sdk/install)installed, authenticated, and configured to the GCP project you want to use.

## 1. Set up environment

Clone the latest stable version of the [Feast repository](https://github.com/gojek/feast/) and setup before we deploy:

```text
git clone --depth 1 --branch v0.7.0 https://github.com/feast-dev/feast.git
export FEAST_REPO=$(pwd)
cd feast/infra/docker-compose
cp .env.sample .env
```

## 2. Start Feast for Online Serving

Use Docker Compose deploy Feast for Online Serving only:

```javascript
docker-compose up
```

{% hint style="info" %}
The Docker Compose deployment will take some time fully startup:

* During this time you may see some connection failures and container restarts which should be automatically corrected a few minutes.
* If container restarts do not stop after 10 minutes, try redeploying by
  * Terminating the current deployment with `Ctrl-C`
  * Deleting any attached volumes with `docker-compose down` -v
  * Redeploying with `docker-compose up`
{% endhint %}

{% hint style="info" %}
 You may see `feast_historical_serving` exiting with code 1, this expected and does not affect the functionality of Feast for Online Serving.
{% endhint %}

Once deployed, you should be able to connect at `localhost:8888` to the bundled Jupyter Notebook Server and follow in the Online Serving sections of the example notebooks:

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
# Required to prevent permissions error in Feast Jupyter:
chown 1000:1000 ${FEAST_REPO}/infra/docker-compose/gcp-service-accounts/key.json
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

Configure the `.env` file under `${FEAST_REPO}/infra/docker-compose/` based on your environment. At the very least you have to modify:

| Parameter | Description |
| :--- | :--- |
| `FEAST_HISTORICAL_SERVING_ENABLED` | Set this to `true` to enable historical serving \(BigQuery\) |

### 3.3 Configure Services

The following configuration has to be set in `serving/historical-serving.yml`

| Parameter | Description |
| :--- | :--- |
| `feast.stores.config.project_id` | This is your [GCP project Id](https://cloud.google.com/resource-manager/docs/creating-managing-projects). |
| `feast.stores.config.dataset_id` | This is the **dataset name** of the BigQuery dataset to use. |
| `feast.stores.config.staging_location` | This is the staging location on Google Cloud Storage for retrieval of training datasets. Make sure you append a suffix \(ie `gs://mybucket/suffix`\) |

The following configuration has to be set in `jobcontroller/jobcontroller.yml`

| Parameter | Description |
| :--- | :--- |
| `feast.jobs.runners.options.tempLocation` | Beam ingestion jobs will persist data here before loading it into BigQuery. Use the same bucket as above and make sure you append a different suffix \(ie `gs://mybucket/anothersuffix`\). |

### 3.4 Start Feast

Use Docker Compose deploy Feast:

```javascript
docker-compose up
```

Once deployed, you should be able to connect at `localhost:8888` to the bundled Jupyter Notebook Server with example notebooks.

{% embed url="http://localhost:8888/tree?" caption="" %}

## 6. Further Reading

* [Feast Concepts](../../user-guide/overview.md)
* [Feast Examples/Tutorials](https://github.com/feast-dev/feast/tree/master/examples)
* [Configuring Feast Components](../../reference/configuration-reference.md)

