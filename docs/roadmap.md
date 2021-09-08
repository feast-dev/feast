# Roadmap

The list below contains Feast functionality that contributors are planning to develop
* Items below may indicate if it is planned for development or whether development is in progress. 
* We welcome contribution to all items in the roadmap, especially those that are not currently planned or in development.
* Want to influence our roadmap? Submit your feedback to [this form](https://docs.google.com/forms/d/e/1FAIpQLSfa1nRQ0sKz-JEFnMMCi4Jseag_yDssO_3nV9qMfxfrkil-wA/viewform) in order to influence the prioritization of our roadmap.
* Want to speak to a Feast contributor? We are more than happy to jump on a quick call. Please schedule a time using [Calendly](https://calendly.com/d/x2ry-g5bb/meet-with-feast-team).

- **Data Sources**
    - [x]  [Redshift source](https://docs.feast.dev/reference/data-sources/redshift)
    - [x]  [BigQuery source](https://docs.feast.dev/reference/data-sources/bigquery)
    - [x]  [Parquet file source](https://docs.feast.dev/reference/data-sources/file)
    - [ ]  Kafka source (Planned for Q4 2021)
    - [ ]  HTTP source
    - [ ]  Snowflake source
    - [ ]  Synapse source


- **Offline Stores**
    - [x]  [Redshift](https://docs.feast.dev/reference/offline-stores/redshift)
    - [x]  [BigQuery](https://docs.feast.dev/reference/offline-stores/bigquery)
    - [x]  [In-memory / Pandas](https://docs.feast.dev/reference/offline-stores/file)
    - [x]  [Custom offline store support](https://docs.feast.dev/how-to-guides/adding-a-new-offline-store)
    - [x]  [Hive (community maintained)](https://github.com/baineng/feast-hive)
    - [ ]  Snowflake 
    - [ ]  Synapse


- **Online Stores**
    - [x]  [DynamoDB](https://docs.feast.dev/reference/online-stores/dynamodb)
    - [x]  [Redis](https://docs.feast.dev/reference/online-stores/redis)
    - [x]  [Datastore](https://docs.feast.dev/reference/online-stores/datastore)
    - [x]  [SQLite](https://docs.feast.dev/reference/online-stores/sqlite)
    - [x]  [Custom online store support](https://docs.feast.dev/how-to-guides/adding-support-for-a-new-online-store)
    - [ ]  Postgres
    - [ ]  Bigtable
    - [ ]  Cassandra


- **Streaming**
    - [ ]  [Custom streaming ingestion job support](https://docs.feast.dev/how-to-guides/creating-a-custom-provider)
    - [ ]  Streaming ingestion on AWS (Planned for Q4 2021)
    - [ ]  Streaming ingestion on GCP


- **Feature Engineering**
    - [ ]  On-demand Transformations (Development in progress. See [RFC](https://docs.google.com/document/d/1lgfIw0Drc65LpaxbUu49RCeJgMew547meSJttnUqz7c/edit#))
    - [ ]  Batch transformation (SQL)
    - [ ]  Streaming transformation


- **Deployments**
    - [ ]  AWS Lambda (Development in progress. See [RFC](https://docs.google.com/document/d/1eZWKWzfBif66LDN32IajpaG-j82LSHCCOzY6R7Ax7MI/edit))
    - [ ]  Cloud Run
    - [ ]  Kubernetes
    - [ ]  KNative


- **Feature Serving**
    - [x]  Python Client
    - [ ]  REST Feature Server (Python) (Development in progress. See [RFC](https://docs.google.com/document/d/1iXvFhAsJ5jgAhPOpTdB3j-Wj1S9x3Ev_Wr6ZpnLzER4/edit))   
    - [ ]  gRPC Feature Server (Java) (See [#1497](https://github.com/feast-dev/feast/issues/1497))
    - [ ]  Java Client
    - [ ]  Go Client    
    - [ ]  Push API
    - [ ]  Delete API
    - [ ]  Feature Logging (for training)


- **Data Quality Management**
    - [ ]  Data profiling and validation (Great Expectations) (Planned for Q4 2021)
    - [ ]  Metric production
    - [ ]  Training-serving skew detection
    - [ ]  Drift detection
    - [ ]  Alerting


- **Feature Discovery and Governance**
    - [x]  Python SDK for browsing feature registry
    - [x]  CLI for browsing feature registry
    - [x]  Model-centric feature tracking (feature services)
    - [ ]  REST API for browsing feature registry
    - [ ]  Feast Web UI (Planned for Q4 2021)
    - [ ]  Feature versioning
    - [ ]  Amundsen integration