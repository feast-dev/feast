# Quickstart

## Overview

This guide will give a walkthrough on deploying Feast using Docker Compose, which allows the user to quickly explore the functionalities in Feast with minimal infrastructure setup. It includes a built in Jupyter Notebook Server that is preloaded with PySpark and Feast SDK, as well as Feast example notebooks to get you started.

## 0. Requirements

* [Docker Compose](https://docs.docker.com/compose/install/) should be installed.
* Optional dependancies:
  * a [GCP service account](https://cloud.google.com/iam/docs/creating-managing-service-account-keys) that has access to [Google Cloud Storage](https://cloud.google.com/storage).

## 1. Set up environment

Clone the latest stable version of the [Feast repository](https://github.com/gojek/feast/) and setup before we deploy:

```text
git clone https://github.com/feast-dev/feast.git
cd feast/infra/docker-compose
cp .env.sample .env
```

## 2. Start Feast Services

Start the Feast services. Make sure that the following ports are free on the host machines: 6565, 6566, 8888, 9094, 5432. Alternatively, change the port mapping to use a different port on the host.

```javascript
docker-compose up -d
```

{% hint style="info" %}
The Docker Compose deployment will take some time fully startup:

* During this time Feast Serving container may restart, which should be automatically corrected after Feast Core is up and ready.
* If container restarts do not stop after 10 minutes, check the docker compose log to see if there is any error that prevents Feast Core from starting successfully.
{% endhint %}

Once deployed, you should be able to connect at `localhost:8888` to the bundled Jupyter Notebook Server and follow the example notebooks:

{% embed url="http://localhost:8888/tree?" caption="" %}

## 3. Optional dependancies

### 3.1 Set up Google Cloud Platform

The example Jupyter notebook does not require any GCP dependancies by default. If you would like to modify the example such that a GCP service is required \(eg. Google Cloud Storage\), you would need to set up a [service account](https://cloud.google.com/iam/docs/creating-managing-service-accounts) that is associated with the notebook. Make sure that the service account has sufficient privileges to access the required GCP services.

Once the service account is created, download the associated JSON key file and copy the file to the path configured in `.env` , under `GCP_SERVICE_ACCOUNT` . 

## 4. Further Reading

* [Feast Concepts](../concepts/overview.md)
* [Feast Examples/Tutorials](https://github.com/feast-dev/feast/tree/master/examples)
* [Configuring Feast Components](../reference/configuration-reference.md)

