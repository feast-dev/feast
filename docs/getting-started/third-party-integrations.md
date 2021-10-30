# Third party integrations

We integrate with a wide set of tools and technologies so you can make Feast work in your existing stack. Many of these integrations are maintained as plugins to the main Feast repo.&#x20;

{% hint style="info" %}
Don't see your offline store or online store of choice here? Check our our guides to make a custom one!

* [Adding a new offline store](broken-reference)
* [Adding a new online store](broken-reference)
{% endhint %}

## Integrations

### **Data Sources**

* [x] [Redshift source](https://docs.feast.dev/reference/data-sources/redshift)
* [x] [BigQuery source](https://docs.feast.dev/reference/data-sources/bigquery)
* [x] [Parquet file source](https://docs.feast.dev/reference/data-sources/file)
* [x] [Synapse source (community plugin)](https://github.com/Azure/feast-azure)
* [x] [Hive (community plugin)](https://github.com/baineng/feast-hive)
* [x] [Postgres (community plugin)](https://github.com/nossrannug/feast-postgres)
* [ ] Kafka source (Planned for Q4 2021)
* [ ] Snowflake source (Planned for Q4 2021)
* [ ] HTTP source

### Offline Stores

* [x] [Redshift](https://docs.feast.dev/reference/offline-stores/redshift)
* [x] [BigQuery](https://docs.feast.dev/reference/offline-stores/bigquery)
* [x] [Synapse (community plugin)](https://github.com/Azure/feast-azure)
* [x] [Hive (community plugin)](https://github.com/baineng/feast-hive)
* [x] [Postgres (community plugin)](https://github.com/nossrannug/feast-postgres)
* [x] [In-memory / Pandas](https://docs.feast.dev/reference/offline-stores/file)
* [x] [Custom offline store support](https://docs.feast.dev/how-to-guides/adding-a-new-offline-store)
* [ ] Snowflake (Planned for Q4 2021)
* [ ] Trino (Planned for Q4 2021)

### Online Stores

* [x] [DynamoDB](https://docs.feast.dev/reference/online-stores/dynamodb)
* [x] [Redis](https://docs.feast.dev/reference/online-stores/redis)
* [x] [Datastore](https://docs.feast.dev/reference/online-stores/datastore)
* [x] [SQLite](https://docs.feast.dev/reference/online-stores/sqlite)
* [x] [Azure Cache for Redis (community plugin)](https://github.com/Azure/feast-azure)
* [x] [Postgres (community plugin)](https://github.com/nossrannug/feast-postgres)
* [x] [Custom online store support](https://docs.feast.dev/how-to-guides/adding-support-for-a-new-online-store)
* [ ] Bigtable
* [ ] Cassandra

### **Deployments**

* [x] AWS Lambda (Alpha release. See [guide](../reference/alpha-aws-lambda-feature-server.md) and [RFC](https://docs.google.com/document/d/1eZWKWzfBif66LDN32IajpaG-j82LSHCCOzY6R7Ax7MI/edit))
* [ ] Cloud Run
* [ ] Kubernetes
* [ ] KNative
