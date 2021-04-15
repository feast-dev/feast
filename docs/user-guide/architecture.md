# Architecture

## Architecture

![Feast high-level flow](../.gitbook/assets/blank-diagram-4%20%284%29%20%284%29%20%284%29%20%284%29%20%284%29%20%284%29%20%284%29%20%284%29%20%282%29%20%281%29%20%281%29%20%281%29%20%281%29%20%284%29.svg)

### **Feast Core**

Feast Core is the central management service of a Feast deployment. It's role is to:

* Allows users to create [entities](entities.md) and [features](features.md) through the creation and management of [feature sets](feature-sets.md). 
* Starts and manages [ingestion jobs](data-ingestion.md). These jobs populate [stores](stores.md) from [sources](sources.md) based on the feature sets that are defined and the subscription\(s\) that a [store](stores.md) has. 

{% hint style="info" %}
Job management may move out of Feast Core to Feast Serving in the future.
{% endhint %}

### **Feast Ingestion**

Before a user ingests data into Feast, they should register one or more feature sets. These [feature sets](feature-sets.md) tell Feast where to find their data, how to ingest it, and also describe the characteristics of the data for validation purposes. Once a feature set is registered, Feast will start an Apache Beam job in order to populate a store with data from a source.

In order for stores to be populated with data, users must publish the data to a [source](sources.md). Currently Feast only supports Apache Kafka as a source. Feast users \(or pipelines\) ingest batch data through the [Feast SDK](../introduction/getting-started/feast-sdk.md) using its `ingest()` method. The SDK publishes the data straight to Kafka.

Streaming systems can also ingest data into Feast. This is done by publishing to the correct Kafka topic in the expected format. Feast expects data to be in [FeatureRow.proto](https://api.docs.feast.dev/grpc/feast.types.pb.html#FeatureRow) format. The topic and brokers can be found on the feature set schema using the Python SDK.

### **Stores**

Stores are nothing more than databases used to store feature data. Stores are documented in a separate section

{% page-ref page="stores.md" %}

### **Feast Serving**

 `Feast Serving` is the data access layer through which end users and production systems retrieve feature data. Each `Serving` instance is backed by a [store](stores.md).

Since Feast supports multiple store types \(online, historical\) it is common to have two instances of Feast Serving deployed, one for online serving and one for historical serving. However, Feast allows for any number of `Feast Serving` deployments, meaning it is possible to deploy a `Feast Serving` deployment  per production system, with its own stores and population jobs.

`Serving` deployments can subscribe to a subset of feature data. Meaning they do not have to consume all features known to a `Feast Core` deployment.

Feature retrieval \(and feature references\) are documented in more detail in subsequent sections

{% page-ref page="feature-retrieval.md" %}





