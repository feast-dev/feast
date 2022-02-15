# Roadmap

The list below contains the functionality that contributors are planning to develop for Feast

* Items below that are in development (or planned for development) will be indicated in parentheses.
* We welcome contribution to all items in the roadmap!
* Want to influence our roadmap and prioritization? Submit your feedback to [this form](https://docs.google.com/forms/d/e/1FAIpQLSfa1nRQ0sKz-JEFnMMCi4Jseag\_yDssO\_3nV9qMfxfrkil-wA/viewform).
* Want to speak to a Feast contributor? We are more than happy to jump on a call. Please schedule a time using [Calendly](https://calendly.com/d/x2ry-g5bb/meet-with-feast-team).

* **Data Sources**
  * [x] [Snowflake source](https://docs.feast.dev/reference/data-sources/snowflake)
  * [x] [Redshift source](https://docs.feast.dev/reference/data-sources/redshift)
  * [x] [BigQuery source](https://docs.feast.dev/reference/data-sources/bigquery)
  * [x] [Parquet file source](https://docs.feast.dev/reference/data-sources/file)
  * [x] [Synapse source (community plugin)](https://github.com/Azure/feast-azure)
  * [x] [Hive (community plugin)](https://github.com/baineng/feast-hive)
  * [x] [Postgres (community plugin)](https://github.com/nossrannug/feast-postgres)
  * [x] [Spark (community plugin)](https://github.com/Adyen/feast-spark-offline-store)
  * [x] Kafka source (with [push support into the online store](https://docs.feast.dev/reference/alpha-stream-ingestion))
  * [ ] HTTP source
* **Offline Stores**
  * [x] [Snowflake](https://docs.feast.dev/reference/offline-stores/snowflake)
  * [x] [Redshift](https://docs.feast.dev/reference/offline-stores/redshift)
  * [x] [BigQuery](https://docs.feast.dev/reference/offline-stores/bigquery)
  * [x] [Synapse (community plugin)](https://github.com/Azure/feast-azure)
  * [x] [Hive (community plugin)](https://github.com/baineng/feast-hive)
  * [x] [Postgres (community plugin)](https://github.com/nossrannug/feast-postgres)
  * [x] [Trino (community plugin)](https://github.com/Shopify/feast-trino)
  * [x] [Spark (community plugin)](https://github.com/Adyen/feast-spark-offline-store)
  * [x] [In-memory / Pandas](https://docs.feast.dev/reference/offline-stores/file)
  * [x] [Custom offline store support](https://docs.feast.dev/how-to-guides/adding-a-new-offline-store)
* **Online Stores**
  * [x] [DynamoDB](https://docs.feast.dev/reference/online-stores/dynamodb)
  * [x] [Redis](https://docs.feast.dev/reference/online-stores/redis)
  * [x] [Datastore](https://docs.feast.dev/reference/online-stores/datastore)
  * [x] [SQLite](https://docs.feast.dev/reference/online-stores/sqlite)
  * [x] [Azure Cache for Redis (community plugin)](https://github.com/Azure/feast-azure)
  * [x] [Postgres (community plugin)](https://github.com/nossrannug/feast-postgres)
  * [x] [Custom online store support](https://docs.feast.dev/how-to-guides/adding-support-for-a-new-online-store)
  * [ ] Bigtable
  * [ ] Cassandra
* **Streaming**
  * [x] [Custom streaming ingestion job support](https://docs.feast.dev/how-to-guides/creating-a-custom-provider)
  * [x] [Push based streaming data ingestion](reference/alpha-stream-ingestion.md)
  * [ ] Streaming ingestion on AWS
  * [ ] Streaming ingestion on GCP
* **Feature Engineering**
  * [x] On-demand Transformations (Alpha release. See [RFC](https://docs.google.com/document/d/1lgfIw0Drc65LpaxbUu49RCeJgMew547meSJttnUqz7c/edit#))
  * [ ] Batch transformation (SQL. In progress. See [RFC](https://docs.google.com/document/d/1964OkzuBljifDvkV-0fakp2uaijnVzdwWNGdz7Vz50A/edit))
  * [ ] Streaming transformation
* **Deployments**
  * [x] AWS Lambda (Alpha release. See [RFC](https://docs.google.com/document/d/1eZWKWzfBif66LDN32IajpaG-j82LSHCCOzY6R7Ax7MI/edit))
  * [x] Kubernetes (See [guide](https://docs.feast.dev/how-to-guides/running-feast-in-production#4.3.-java-based-feature-server-deployed-on-kubernetes))
  * [ ] Cloud Run
  * [ ] KNative
* **Feature Serving**
  * [x] Python Client
  * [x] REST Feature Server (Python) (Alpha release. See [RFC](https://docs.google.com/document/d/1iXvFhAsJ5jgAhPOpTdB3j-Wj1S9x3Ev\_Wr6ZpnLzER4/edit))
  * [x] gRPC Feature Server (Java) (See [#1497](https://github.com/feast-dev/feast/issues/1497))
  * [x] Push API
  * [ ] Java Client
  * [ ] Go Client
  * [ ] Delete API
  * [ ] Feature Logging (for training)
* **Data Quality Management (See [RFC](https://docs.google.com/document/d/110F72d4NTv80p35wDSONxhhPBqWRwbZXG4f9mNEMd98/edit))**
  * [x] Data profiling and validation (Great Expectations)
  * [ ] Metric production
  * [ ] Training-serving skew detection
  * [ ] Drift detection
* **Feature Discovery and Governance**
  * [x] Python SDK for browsing feature registry
  * [x] CLI for browsing feature registry
  * [x] Model-centric feature tracking (feature services)
  * [x] Amundsen integration (see [Feast extractor](https://github.com/amundsen-io/amundsen/blob/main/databuilder/databuilder/extractor/feast_extractor.py))
  * [ ] REST API for browsing feature registry
  * [ ] Feast Web UI
  * [ ] Feature versioning
