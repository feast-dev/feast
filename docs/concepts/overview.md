# Overview

## Using Feast

Feast acts as the interface between ML models and data. Feast enables your team to

1. Create feature specifications to manage features and load in data that should be managed
2. Retrieve historical features for training models
3. Retrieve online features for serving models

{% hint style="info" %}
Feast currently does not apply feature transformations to data.
{% endhint %}

### 1. Creating and managing features

Feature creators model the data within their organization into Feast through the creation of [feature tables](feature-tables.md).

Feature tables are specifications that contain both schema and data source information. They allow Feast to know how to interpret your data, and optionally where to find it. Feature tables allow users to define domain [entities](entities.md) along with the features that are available on these entities. Feature tables also allow users to define schemas that describe the properties of the data, which in turn can be used for validation purposes.

Once a feature table has been registered, Feast will create the relevant schemas to store feature data within its feature [stores](../user-guide/stores.md). These stores are then populated by [ingestion jobs](../user-guide/data-ingestion.md) that ingest data from data [sources](sources.md), making it possible for Feast to provide access to features for training and serving. It is also possible for users to [ingest](../user-guide/data-ingestion.md) data into Feast instead of using an external source.

Read more about [feature tables](feature-tables.md).

### 2. Retrieving historical features during training

Both online and historical retrieval are executed through an API call to `Feast Serving` using [feature references](../user-guide/feature-retrieval.md). In the case of historical serving it is necessary to provide Feast with the entities and timestamps that feature data will be joined to. Feast eagerly produces a point-in-time correct dataset based on the features that have been requested. These features can come from any number of feature sets.

Stores supported: [BigQuery](https://cloud.google.com/bigquery)

### 3. Retrieving online features during serving

Feast also allows users to call `Feast Serving` for online feature data. Feast only stores the latest values during online serving for each feature, as opposed to historical serving where all historical values are stored. Online serving allows for very low latency requests to feature data at very high throughput.

Stores supported: [Redis](https://redis.io/), [Redis Cluster](https://redis.io/topics/cluster-tutorial)

## Concept Hierarchy

![](../.gitbook/assets/concept_hierarchy.png)

Feast resources are arranged in the above hierarchy, with projects grouping one or more [entities](entities.md), which in turn groups [feature tables](feature-tables.md), which consists of [data sources](sources.md) and multiple features.

The logical grouping of these resources are important for namespacing as well as retrieval. During retrieval time, it is necessary to reference individual features through feature references. These references uniquely identify a feature within a Feast deployment.

## Concepts

[Entities](entities.md) are objects in an organization that model a specific construct. Examples of these include customers, transactions, and drivers.

[Sources](sources.md) are either internal or external data sources where feature data can be found.

[Feature Tables](feature-tables.md) are schemas that define logical groupings of features, data sources, and other related metadata.

[Stores](../user-guide/stores.md) are databases that maintain feature data that gets served to models during training or inference.

[Ingestion](../user-guide/data-ingestion.md) is the process of loading data into Feast.

