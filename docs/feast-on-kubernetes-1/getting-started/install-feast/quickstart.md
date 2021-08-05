# Docker Compose

{% hint style="success" %}
This guide is meant for exploratory purposes only. It allows users to run Feast locally using Docker Compose instead of Kubernetes. The goal of this guide is for users to be able to quickly try out the full Feast stack without needing to deploy to Kubernetes. It is not meant for production use.
{% endhint %}

## Overview

This guide shows you how to deploy Feast using [Docker Compose](https://docs.docker.com/get-started/). Docker Compose allows you to explore the functionality provided by Feast while requiring only minimal infrastructure.

This guide includes the following containerized components:

* [A complete Feast deployment](../../concepts/architecture.md)
  * Feast Core with Postgres
  * Feast Online Serving with Redis.
  * Feast Job Service
* A Jupyter Notebook Server with built in Feast example\(s\). For demo purposes only.
* A Kafka cluster for testing streaming ingestion. For demo purposes only.

## Get Feast

Clone the latest stable version of Feast from the [Feast repository](https://github.com/feast-dev/feast/):

```text
git clone https://github.com/feast-dev/feast.git
cd feast/infra/docker-compose
```

Create a new configuration file:

```text
cp .env.sample .env
```

## Start Feast

Start Feast with Docker Compose:

```text
docker-compose pull && docker-compose up -d
```

Wait until all all containers are in a running state:

```text
docker-compose ps
```

## Try our example\(s\)

You can now connect to the bundled Jupyter Notebook Server running at `localhost:8888` and follow the example Jupyter notebook.

{% embed url="http://localhost:8888/tree?" caption="" %}

## Troubleshooting

### Open ports

Please ensure that the following ports are available on your host machine:

* `6565` 
* `6566`
* `8888`
* `9094`
* `5432`

If a port conflict cannot be resolved, you can modify the port mappings in the provided [docker-compose.yml](https://github.com/feast-dev/feast/tree/master/infra/docker-compose) file to use different ports on the host.

### Containers are restarting or unavailable

If some of the containers continue to restart, or you are unable to access a service, inspect the logs using the following command:

```javascript
docker-compose logs -f -t
```

If you are unable to resolve the problem, visit [GitHub](https://github.com/feast-dev/feast/issues) to create an issue.

## Configuration

The Feast Docker Compose setup can be configured by modifying properties in your `.env` file.

### Accessing Google Cloud Storage \(GCP\)

To access Google Cloud Storage as a data source, the Docker Compose installation requires access to a GCP service account.

* Create a new [service account](https://cloud.google.com/iam/docs/creating-managing-service-accounts) and save a JSON key.
* Grant the service account access to your bucket\(s\).
* Copy the service account to the path you have configured in `.env` under `GCP_SERVICE_ACCOUNT`.
* Restart your Docker Compose setup of Feast.

