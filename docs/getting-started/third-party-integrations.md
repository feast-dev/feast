# Third party integrations

We integrate with a wide set of tools and technologies so you can make Feast work in your existing stack. Many of these integrations are maintained as plugins to the main Feast repo.

{% hint style="info" %}
Don't see your offline store or online store of choice here? Check out our guides to make a custom one!

* [Adding a new offline store](../how-to-guides/adding-a-new-offline-store.md)
* [Adding a new online store](../how-to-guides/adding-support-for-a-new-online-store.md)
{% endhint %}

## Integrations

### **Data Sources**

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

### Offline Stores

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
* [x] Kubernetes (See [guide](https://docs.feast.dev/how-to-guides/running-feast-in-production#4.3.-java-based-feature-server-deployed-on-kubernetes))
* [ ] Cloud Run
* [ ] KNative


## Standards

In order for a plugin integration to be highlighted on this page, it must meet the following requirements:

1. The plugin must have tests. Ideally it would use the Feast universal tests (see this [guide](../how-to-guides/adding-or-reusing-tests.md) for an example), but custom tests are fine.
2. The plugin must have some basic documentation on how it should be used.
3. The author must work with a maintainer to pass a basic code review (e.g. to ensure that the implementation roughly matches the core Feast implementations).&#x20;

In order for a plugin integration to be merged into the main Feast repo, it must meet the following requirements:

1. The PR must pass all integration tests. The universal tests (tests specifically designed for custom integrations) must be updated to test the integration.
2. There is documentation and a tutorial on how to use the integration.
3. The author (or someone else) agrees to take ownership of all the files, and maintain those files going forward.
4. If the plugin is being contributed by an organization, and not an individual, the organization should provide the infrastructure (or credits) for integration tests.
