# Architecture

![Feast high-level flow](../.gitbook/assets/blank-diagram-4.svg)

### **Feast Core**

Feast Core is the central management service of a Feast deployment. It's role is to:

* Allow users to create [entities](entities.md).
* Allow users to create features through the creation of [feature tables](feature-tables.md).
* Act as a source of truth and central registry of feature tables.

### **Feast Ingestion**

Before a user ingests data into Feast, they should register one or more entity, followed by feature tables. These [feature tables](feature-tables.md) tell Feast where to find their data, how to ingest it, and also describe the characteristics of the data for validation purposes. Once a feature table is registered, users can now start a Spark job to populate a store with data from the defined source in the feature table specification.

In order for stores to be populated with data, users must publish the data to a [source](sources.md). Currently, Feast supports a few batch and stream sources. Feast users \(or pipelines\) ingest batch data through the [Feast SDK](../getting-started/connecting-to-feast-1/connecting-to-feast.md) using its `ingest()` method. The SDK publishes the data into the batch source specified for the feature table's batch source.

Streaming systems can also ingest data into Feast. This is done by publishing to the correct stream source from the feature table specification in the expected format. The topic and brokers can be found on the feature table's stream source if specified during registration.

### **Stores**

Stores are nothing more than databases used to store feature data. Feast loads data into stores through an ingestion process, after which the data can be served through the Feast Serving API. Stores are documented in the following section.

{% page-ref page="../user-guide/stores.md" %}

### **Feast Serving**

`Feast Serving` is the data access layer through which end users and production systems retrieve feature data. Each `Serving` instance is backed by a [store](../user-guide/stores.md).

Since Feast supports multiple store types \(online, historical\) it is common to have two instances of Feast Serving deployed, one for online serving and one for historical serving. However, Feast allows for any number of `Feast Serving` deployments, meaning it is possible to deploy a `Feast Serving` deployment  per production system, with its own stores and population jobs.

`Feast Serving` deployments will subscribe to all feature data, consuming all features known to a `Feast Core` deployment.

Feature retrieval \(and feature references\) are documented in more detail in subsequent sections.

{% page-ref page="../user-guide/feature-retrieval.md" %}

