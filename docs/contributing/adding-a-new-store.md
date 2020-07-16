# Adding a New Store

The following guide will explain the process of adding a new store through the introduction of a storage connector.

## 1. Storage API

Feast has an external module where storage interfaces are defined: [Storage API](https://github.com/gojek/feast/tree/master/storage/api/src/main/java/feast/storage/api)

Feast interacts with a store at three points .

1. **During initialization:** Store configuration is loaded into memory by Feast Serving and synchronized with Feast Core
2. **During ingestion of feature data.** [writer interfaces](https://github.com/gojek/feast/tree/master/storage/api/src/main/java/feast/storage/api/writer) are used by the Apache Beam ingestion jobs in order to populate stores \(historical or online\).
3. **During retrieval of feature data:** [Retrieval interfaces](https://github.com/gojek/feast/tree/master/storage/api/src/main/java/feast/storage/api/retriever) are used by Feast Serving in order to read data from stores in order to create training datasets or to serve online data.

All three of these components should be implemented in order to have a complete storage connector.

## 2. Adding a Storage Connector

### 2.1 Initialization and configuration

Stores are configured in Feast Serving. Feast Serving publishes its store configuration to Feast Core, after which Feast Core can start ingestion/population jobs to populate it.

Store configuration is always in the form of a map&lt;String, String&gt;. The keys and configuration for stores are defined in [protos](https://github.com/gojek/feast/blob/master/protos/feast/core/Store.proto). This must be added in order to define a new store

Then the store must be configured to be loaded through Feast Serving. The above configuration is loaded through [FeastProperties.java](https://github.com/gojek/feast/blob/a1937c374a4e39b7a75d828e7b7c3b87a64d9d6e/serving/src/main/java/feast/serving/config/FeastProperties.java#L175). 

Once configuration is loaded, the store will then be instantiated.

* Feast Core: The [StoreUtil.java](https://github.com/gojek/feast/blob/master/ingestion/src/main/java/feast/ingestion/utils/StoreUtil.java#L85) instantiates new stores for the purposes of feature ingestion.
* Feast Serving: The [ServingServiceConfig](https://github.com/gojek/feast/blob/a1937c374a4e39b7a75d828e7b7c3b87a64d9d6e/serving/src/main/java/feast/serving/config/ServingServiceConfig.java#L56) instantiates new stores for the purposes of retrieval

{% hint style="info" %}
In the future we plan to provide a plugin interface for adding stores.
{% endhint %}

### 2.2 Feature Ingestion \(Writer\)

Feast creates and manages ingestion/population jobs that stream in data from upstream data sources. Currently Feast only supports Kafka as a data source, meaning these jobs are all long running. Batch ingestion \(from users\) results in data being pushed to Kafka topics after which they are picked up by these "population" jobs and written to stores.

In order for ingestion to succeed, the destination store must be writable. This means that Feast must be able to create the appropriate tables/schemas in the store and also write data from the population job into the store.

Currently Feast Core starts and manages these population jobs that ingest data into stores \(although we are planning to move this responsibility to the serving layer\). Feast Core starts an [Apache Beam](https://beam.apache.org/) job which synchronously runs migrations on the destination store and subsequently starts consuming [FeatureRows](https://github.com/gojek/feast/blob/master/protos/feast/types/FeatureRow.proto) from Kafka and writing it into stores using a [writer](https://github.com/gojek/feast/tree/master/storage/api/src/main/java/feast/storage/api/writer).

Below is a "happy path" of a batch ingestion process which includes a blocking step at the Python SDK.

![](https://user-images.githubusercontent.com/6728866/74807906-91e73c00-5324-11ea-8ba5-2b43c7c5282b.png)



The complete ingestion flow is executed by a [FeatureSink](https://github.com/gojek/feast/blob/master/storage/api/src/main/java/feast/storage/api/writer/FeatureSink.java). Two methods should be implemented

* [prepareWrite\(\)](https://github.com/gojek/feast/blob/a1937c374a4e39b7a75d828e7b7c3b87a64d9d6e/storage/api/src/main/java/feast/storage/api/writer/FeatureSink.java#L45): Sets up storage backend for writing/ingestion. This method will be called once during pipeline initialisation. Typically this is used to apply schemas.
* [writer\(\)](https://github.com/gojek/feast/blob/a1937c374a4e39b7a75d828e7b7c3b87a64d9d6e/storage/api/src/main/java/feast/storage/api/writer/FeatureSink.java#L53): Retrieves an Apache Beam PTransform that is used to write data to this store.

### 2.2 Feature Serving \(Retriever\)

Feast Serving can serve both historical/batch features and online features. Depending on the store that is being added, you should implement either a historical/batch store or an online storage.

#### 2.2.1 Historical Serving

The historical serving interface is defined through the [HistoricalRetriever](https://github.com/gojek/feast/blob/master/storage/api/src/main/java/feast/storage/api/retriever/HistoricalRetriever.java) interface. Historical retrieval is an asynchronous process. The client submits a request for a dataset to be produced, and polls until it is ready.

![High-level flow for batch retrieval](https://user-images.githubusercontent.com/6728866/74797157-702a8c80-5305-11ea-8901-bf6f4eb075f9.png)

The current implementation of batch retrieval starts and ends with a file \(dataset\) in a Google Cloud Storage bucket. The user ingests an entity dataset. This dataset is loaded into a store \(BigQuery0, joined to features in a point-in-time correct way, then exported again to the bucket.

Additionally, we have also implemented a [batch retrieval method ](https://github.com/gojek/feast/blob/a1937c374a4e39b7a75d828e7b7c3b87a64d9d6e/sdk/python/feast/client.py#L509)in the Python SDK. Depending on the means through which this new store will export data, this client may have to change. At the very least it would change if Google Cloud Storage isn't used as the staging bucket.

The means through which you implement the export/import of data into the store will depend on your store. 

#### 2.2.2 Online Serving

In the case of online serving it is necessary to implement an [OnlineRetriever](https://github.com/gojek/feast/blob/master/storage/api/src/main/java/feast/storage/api/retriever/OnlineRetriever.java). This online retriever will read rows directly and synchronously from an online database. The exact encoding strategy you use to store your data in the store would be defined in the FeatureSink. The OnlineRetriever is expected to read and decode those rows.

## 3. Storage Connectors Examples

Feast currently provides support for the following storage types

Historical storage

* [BigQuery](https://github.com/gojek/feast/tree/master/storage/connectors/bigquery)

Online storage

* [Redis & Redis Cluster](https://github.com/gojek/feast/tree/master/storage/connectors/redis) 

