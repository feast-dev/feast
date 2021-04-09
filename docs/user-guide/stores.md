# Stores

In Feast, a store describes a database that is populated with feature data in order to be served to models.

Feast supports two classes of stores

* Historical stores
* Online stores

In order to populate these stores, Feast Core creates a long running ingestion job that streams in data from all feature sources to all stores that subscribe to those feature sets.

![](../.gitbook/assets/image%20%282%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29%20%282%29.png)

## Historical Stores

Historical stores maintain a complete history of feature data for the feature sets they are subscribed to. 

Feast currently only supports [Google BigQuery](https://cloud.google.com/bigquery) as a feature store, but we have [developed a storage API ](https://github.com/gojek/feast/issues/482)that makes adding a new store possible.

Each historical store models its data differently, but in the case of a relational store \(like BigQuery\), each feature set maps directly to a table. Each feature and entity within a feature set maps directly to a column within a table.

Data from historical stores can be used to train a model. In order to retrieve data from a historical store it is necessary to connect to a Feast Serving deployment and request historical features. Please see feature retrieval for more details.

{% hint style="danger" %}
Data is persisted in historical stores like BigQuery in log format. Repeated ingestions will duplicate the data is persisted in the store. Feast will automatically deduplicate data during retrieval, but it doesn't currently remove data from the stores themselves.
{% endhint %}

## Online Stores

Online stores maintain only the latest values for a specific feature. Feast currently supports Redis as an online store. Online stores are meant for very high throughput writes from ingestion jobs and very low latency access to features during online serving.

Please continue to the [feature retrieval](feature-retrieval.md) section for more details on retrieving data from online storage.

## Subscriptions

Stores are populated by ingestion jobs \(Apache Beam\) that retrieve feature data from sources based on subscriptions. These subscriptions are typically defined by the administrators of the Feast deployment. In most cases a store would simply subscribe to all features, but in some cases it may subscribe to a subset in order to improve performance or efficiency.



