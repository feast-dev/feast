# Quickstart

### Overview

This guide shows you how to deploy Feast using [Docker Compose](https://docs.docker.com/get-started/). Docker Compose enables you to explore the functionality provided by Feast while requiring only minimal infrastructure.

This guide includes the following containerized components:

* A complete Feast deployment \(Feast Core with Postgres, Feast Online Serving with Redis\).
* A Jupyter Notebook Server with built in Feast example\(s\).
* A Kafka cluster for testing streaming ingestion.

### 1. Requirements

1. Install [Docker Compose](https://docs.docker.com/compose/install/)

### 2. Configure Environment

Clone the latest stable version of Feast from the [Feast repository](https://github.com/gojek/feast/):

```text
git clone https://github.com/feast-dev/feast.git
cd feast/infra/docker-compose
```

Create a new configuration file:

```text
cp .env.sample .env
```

### 3. Start Feast Services

Start Feast with Docker Compose:

```text
docker-compose up -d
```

Wait until all all containers are in a running state:

```text
docker-compose ps
```

You can now connect to the bundled Jupyter Notebook Server running at `localhost:8888` and follow the example Jupyter notebook.

{% embed url="http://localhost:8888/tree?" caption="" %}

### 4. Troubleshooting

#### 4.1 Open ports

Please ensure that the following ports are available on your host machine:  `6565`, `6566`, `8888`, `9094`, `5432`.

If a port conflict cannot be resolved, you can modify all port mappings in the provided [docker-compose.yml](https://github.com/feast-dev/feast/tree/master/infra/docker-compose) file to use a different port on the host.

#### 4.2 Containers are restarting or unavailable

If some of the containers continue to restart, or you are unable to access a service, inspect the logs using the following command:

```javascript
docker-compose logs -f -t
```

If you are unable to resolve the problem, visit [GitHub](https://github.com/feast-dev/feast/issues) to create an issue.

### 5. Configuration

The Feast Docker Compose setup can be configured by modifying properties in your `.env` file.

#### 5.1 Accessing Google Cloud Storage \(GCP\)

To access Google Cloud Storage as a data source, the Docker Compose installation requires access to a GCP service account.

* Create a new [service account](https://cloud.google.com/iam/docs/creating-managing-service-accounts) and save a JSON key.
* Grant the service account access to your bucket\(s\).
* Copy the service account to the path you have configured in `.env` under `GCP_SERVICE_ACCOUNT`.
* Restart your Docker Compose setup of Feast.

