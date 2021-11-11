# Roadmap

The list below contains the functionality that contributors are planning to develop for Feast

* Items below that are in development (or planned for development) will be indicated in parentheses.
* We welcome contribution to all items in the roadmap!
* Want to influence our roadmap and prioritization? Submit your feedback to [this form](https://docs.google.com/forms/d/e/1FAIpQLSfa1nRQ0sKz-JEFnMMCi4Jseag\_yDssO\_3nV9qMfxfrkil-wA/viewform).
* Want to speak to a Feast contributor? We are more than happy to jump on a call. Please schedule a time using [Calendly](https://calendly.com/d/x2ry-g5bb/meet-with-feast-team).
* **Data Sources**
  * [x] [Redshift source](https://docs.feast.dev/reference/data-sources/redshift)
  * [x] [BigQuery source](https://docs.feast.dev/reference/data-sources/bigquery)
  * [x] [Parquet file source](https://docs.feast.dev/reference/data-sources/file)
  * [x] [Synapse source (community plugin)](https://github.com/Azure/feast-azure)
  * [x] [Hive (community plugin)](https://github.com/baineng/feast-hive)
  * [x] [Postgres (community plugin)](https://github.com/nossrannug/feast-postgres)
  * [x] Kafka source (with [push support into the online store](reference/alpha-stream-ingestion.md))
  * [ ] Snowflake source (Planned for Q4 2021)
  * [ ] HTTP source
* **Offline Stores**
  * [x] [Redshift](https://docs.feast.dev/reference/offline-stores/redshift)
  * [x] [BigQuery](https://docs.feast.dev/reference/offline-stores/bigquery)
  * [x] [Synapse (community plugin)](https://github.com/Azure/feast-azure)
  * [x] [Hive (community plugin)](https://github.com/baineng/feast-hive)
  * [x] [Postgres (community plugin)](https://github.com/nossrannug/feast-postgres)
  * [x] [In-memory / Pandas](https://docs.feast.dev/reference/offline-stores/file)
  * [x] [Custom offline store support](https://docs.feast.dev/how-to-guides/adding-a-new-offline-store)
  * [ ] Snowflake (Planned for Q4 2021)
  * [ ] Trino (Planned for Q4 2021)
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
  * [ ] Batch transformation (SQL)
  * [ ] Streaming transformation
* **Deployments**
  * [x] AWS Lambda (Alpha release. See [RFC](https://docs.google.com/document/d/1eZWKWzfBif66LDN32IajpaG-j82LSHCCOzY6R7Ax7MI/edit))
  * [ ] Cloud Run
  * [ ] Kubernetes
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
* **Data Quality Management**
  * [ ] Data profiling and validation (Great Expectations) (Planned for Q4 2021)
  * [ ] Metric production
  * [ ] Training-serving skew detection
  * [ ] Drift detection
  * [ ] Alerting
* **Feature Discovery and Governance**
  * [x] Python SDK for browsing feature registry
  * [x] CLI for browsing feature registry
  * [x] Model-centric feature tracking (feature services)
  * [ ] REST API for browsing feature registry
  * [ ] Feast Web UI (Planned for Q4 2021)
  * [ ] Feature versioning
  * [ ] Amundsen integration
