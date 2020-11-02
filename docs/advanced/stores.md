# Stores

In Feast, a store describes a database that is populated with feature data served to models.

Feast supports two classes of stores:

* Historical stores
* Online stores

To populate these stores, Feast Core creates a long running ingestion job that streams data from all feature sources to all stores that subscribe to those feature sets.

![](../.gitbook/assets/image%20%282%29.png)

### Historical Stores

Historical stores subscribe to feature sets and maintain a complete history of feature data for these sets.

Feast currently only supports [Google BigQuery](https://cloud.google.com/bigquery) as a feature store, but we have [developed a storage API ](https://github.com/gojek/feast/issues/482)that makes adding a new store possible.

Each historical store models its data differently. In a relational store \(like BigQuery\), each feature set maps directly to a table. Each feature and entity within a feature set maps directly to a column within a table.

Data from historical stores can be used to train models. Retrieving data from a historical store requires connecting to a Feast Online Serving deployment, and requesting historical features. Visit [feature retrieval](../user-guide/feature-retrieval.md) for more details.

{% hint style="danger" %}
Data persists in log format in historical stores like BigQuery. Repeated ingestions duplicates the data persisted in the store. Feast automatically deduplicates data during retrieval, but currently does **not** remove any duplicate data from the stores themselves.
{% endhint %}

### Online Stores

Feast currently supports Redis as an online store. Online stores maintain only the latest values for a specific feature because they are meant for very high throughput-writes from ingestion jobs, and very low latency access to features during online serving.

Visit the [feature retrieval](../user-guide/feature-retrieval.md) section for more details on retrieving data from online storage.

### Subscriptions

Stores are populated by ingestion jobs \(Apache Beam\) that use subscriptions to retrieve feature data from sources. These subscriptions are typically defined by the administrators of the Feast deployment. Depending on your use case, you can subscribe to all features, or only a subset of features.

