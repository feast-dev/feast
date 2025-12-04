# Roadmap

The list below contains the functionality that contributors are planning to develop for Feast.

* We welcome contribution to all items in the roadmap!

* **Natural Language Processing**
  * [x] Vector Search (Alpha release. See [RFC](https://docs.google.com/document/d/18IWzLEA9i2lDWnbfbwXnMCg3StlqaLVI-uRpQjr_Vos/edit#heading=h.9gaqqtox9jg6))
  * [ ] [Enhanced Feature Server and SDK for native support for NLP](https://github.com/feast-dev/feast/issues/4964)
* **Data Sources**
  * [x] [Snowflake source](https://docs.feast.dev/reference/data-sources/snowflake)
  * [x] [Redshift source](https://docs.feast.dev/reference/data-sources/redshift)
  * [x] [BigQuery source](https://docs.feast.dev/reference/data-sources/bigquery)
  * [x] [Parquet file source](https://docs.feast.dev/reference/data-sources/file)
  * [x] [Azure Synapse + Azure SQL source (contrib plugin)](https://docs.feast.dev/reference/data-sources/mssql)
  * [x] [Hive (community plugin)](https://github.com/baineng/feast-hive)
  * [x] [Postgres (contrib plugin)](https://docs.feast.dev/reference/data-sources/postgres)
  * [x] [Spark (contrib plugin)](https://docs.feast.dev/reference/data-sources/spark)
  * [x] [Couchbase (contrib plugin)](https://docs.feast.dev/reference/data-sources/couchbase)
  * [x] Kafka / Kinesis sources (via [push support into the online store](https://docs.feast.dev/reference/data-sources/push))
* **Offline Stores**
  * [x] [Snowflake](https://docs.feast.dev/reference/offline-stores/snowflake)
  * [x] [Redshift](https://docs.feast.dev/reference/offline-stores/redshift)
  * [x] [BigQuery](https://docs.feast.dev/reference/offline-stores/bigquery)
  * [x] [Azure Synapse + Azure SQL (contrib plugin)](https://docs.feast.dev/reference/offline-stores/mssql.md)
  * [x] [Hive (community plugin)](https://github.com/baineng/feast-hive)
  * [x] [Postgres (contrib plugin)](https://docs.feast.dev/reference/offline-stores/postgres)
  * [x] [Trino (contrib plugin)](https://github.com/Shopify/feast-trino)
  * [x] [Spark (contrib plugin)](https://docs.feast.dev/reference/offline-stores/spark)
  * [x] [Couchbase (contrib plugin)](https://docs.feast.dev/reference/offline-stores/couchbase)
  * [x] [In-memory / Pandas](https://docs.feast.dev/reference/offline-stores/file)
  * [x] [Custom offline store support](https://docs.feast.dev/how-to-guides/customizing-feast/adding-a-new-offline-store)
* **Online Stores**
  * [x] [Snowflake](https://docs.feast.dev/reference/online-stores/snowflake)
  * [x] [DynamoDB](https://docs.feast.dev/reference/online-stores/dynamodb)
  * [x] [Redis](https://docs.feast.dev/reference/online-stores/redis)
  * [x] [Datastore](https://docs.feast.dev/reference/online-stores/datastore)
  * [x] [Bigtable](https://docs.feast.dev/reference/online-stores/bigtable)
  * [x] [SQLite](https://docs.feast.dev/reference/online-stores/sqlite)
  * [x] [Dragonfly](https://docs.feast.dev/reference/online-stores/dragonfly)
  * [x] [IKV - Inlined Key Value Store](https://docs.feast.dev/reference/online-stores/ikv)
  * [x] [Azure Cache for Redis (community plugin)](https://github.com/Azure/feast-azure)
  * [x] [Postgres (contrib plugin)](https://docs.feast.dev/reference/online-stores/postgres)
  * [x] [Cassandra / AstraDB (contrib plugin)](https://docs.feast.dev/reference/online-stores/cassandra)
  * [x] [ScyllaDB (contrib plugin)](https://docs.feast.dev/reference/online-stores/scylladb)
  * [x] [Couchbase (contrib plugin)](https://docs.feast.dev/reference/online-stores/couchbase)
  * [x] [Custom online store support](https://docs.feast.dev/how-to-guides/customizing-feast/adding-support-for-a-new-online-store)
* **Feature Engineering**
  * [x] On-demand Transformations (On Read) (Beta release. See [RFC](https://docs.google.com/document/d/1lgfIw0Drc65LpaxbUu49RCeJgMew547meSJttnUqz7c/edit#))
  * [x] Streaming Transformations (Alpha release. See [RFC](https://docs.google.com/document/d/1UzEyETHUaGpn0ap4G82DHluiCj7zEbrQLkJJkKSv4e8/edit))
  * [x] Batch transformation (Completed via unified transformation system. See [Feature Transformation](https://docs.feast.dev/getting-started/architecture/feature-transformation))
  * [x] On-demand Transformations (On Write) (Beta release. See [GitHub Issue](https://github.com/feast-dev/feast/issues/4376))
* **Streaming**
  * [x] [Custom streaming ingestion job support](https://docs.feast.dev/how-to-guides/customizing-feast/creating-a-custom-provider)
  * [x] [Push based streaming data ingestion to online store](https://docs.feast.dev/reference/data-sources/push)
  * [x] [Push based streaming data ingestion to offline store](https://docs.feast.dev/reference/data-sources/push)
* **Deployments**
  * [x] AWS Lambda (Alpha release. See [RFC](https://docs.google.com/document/d/1eZWKWzfBif66LDN32IajpaG-j82LSHCCOzY6R7Ax7MI/edit))
  * [x] Kubernetes (See [guide](https://docs.feast.dev/how-to-guides/running-feast-in-production))
* **Feature Serving**
  * [x] Python Client
  * [x] [Python feature server](https://docs.feast.dev/reference/feature-servers/python-feature-server)
  * [x] [Feast Operator (alpha)](https://github.com/feast-dev/feast/blob/master/infra/feast-operator/README.md)
  * [x] [Java feature server (alpha)](https://github.com/feast-dev/feast/blob/master/infra/charts/feast/README.md)
  * [x] [Go feature server (alpha)](https://docs.feast.dev/reference/feature-servers/go-feature-server)
  * [x] [Offline Feature Server (alpha)](https://docs.feast.dev/reference/feature-servers/offline-feature-server)
  * [x] [Registry server (alpha)](https://github.com/feast-dev/feast/blob/master/docs/reference/feature-servers/registry-server.md)
* **Data Quality Management (See [RFC](https://docs.google.com/document/d/110F72d4NTv80p35wDSONxhhPBqWRwbZXG4f9mNEMd98/edit))**
  * [x] Data profiling and validation (Great Expectations)
* **Feature Discovery and Governance**
  * [x] Python SDK for browsing feature registry
  * [x] CLI for browsing feature registry
  * [x] Model-centric feature tracking (feature services)
  * [x] Amundsen integration (see [Feast extractor](https://github.com/amundsen-io/amundsen/blob/main/databuilder/databuilder/extractor/feast_extractor.py))
  * [x] DataHub integration (see [DataHub Feast docs](https://datahubproject.io/docs/generated/ingestion/sources/feast/))
  * [x] Feast Web UI (Beta release. See [docs](https://docs.feast.dev/reference/alpha-web-ui))
  * [ ] Feast Lineage Explorer
