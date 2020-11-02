# Quickstart

### Overview

This brief introduction shows you how to deploy Feast using [Docker Compose](https://docs.docker.com/get-started/). Docker Compose enables you to explore the functionality provided by Feast while requiring only minimal infrastructure. It includes a built in Jupyter Notebook Server that is preloaded with PySpark and the Feast Python SDK, as well as Feast example notebooks to get you started.

### 1. Requirements

* Install [Docker Compose](https://docs.docker.com/compose/install/)
* Optional dependencies:
  * a [GCP service account](https://cloud.google.com/iam/docs/creating-managing-service-account-keys) that has access to [Google Cloud Storage](https://cloud.google.com/storage)

### 2. Configure Environment

Before starting, clone the latest stable version of Feast from the [Feast repository](https://github.com/gojek/feast/), and configure.

```text
git clone https://github.com/feast-dev/feast.git
cd feast/infra/docker-compose
cp .env.sample .env
```

### 3. Start Feast Services

Start the Feast services. Ensure that the following ports are free on the host machines: `6565`, `6566`, `8888`, `9094`, `5432`. Alternatively, change port mapping to use a different port on the host.

```javascript
docker-compose up -d
```

{% hint style="info" %}
Docker Compose takes a minute to launch:

* While the deployment is initiating, Feast Online Serving container may restart. Once Feast Core has launched, any container restarts should be auto-corrected.
* If container restarts continue for more than 10 minutes, check the Docker Compose log for any errors that prevent Feast Core from starting successfully.
{% endhint %}

You can now connect to the bundled Jupyter Notebook Server at `localhost:8888` and follow the example notebooks.

{% embed url="http://localhost:8888/tree?" caption="" %}

### 4. Optional Dependencies

#### 4.1 Configure Google Cloud Platform \(GCP\)

By default, the example Jupyter notebook does not require any GCP dependencies. If you modify the example to require GCP services \(for example Google Cloud Storage\), you need to establish a [service account](https://cloud.google.com/iam/docs/creating-managing-service-accounts) that is associated with the Jupyter notebook. 

{% hint style="warning" %}
Confirm that the GCP service account has sufficient privileges to access the required GCP services.
{% endhint %}

After you create the service account, download the associated JSON key file and copy the file to the path configured in `.env` under `GCP_SERVICE_ACCOUNT` . 

### 5. Further Reading

* [Feast Concepts](concepts/overview.md)
* [Feast Examples/Tutorials](https://github.com/feast-dev/feast/tree/master/examples)
* [Configuring Feast Components](reference/configuration-reference.md)
* [Configuration Reference](https://app.gitbook.com/@feast/s/docs/v/master/reference/configuration-reference)

