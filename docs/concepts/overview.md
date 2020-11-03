# Overview

## Using Feast

Feast is the bridge between your ML models and data. Feast enables your team to:

1. Create feature specifications to manage features, and load data that you want managed
2. Retrieve historical features for training models
3. Retrieve online features for serving models

{% hint style="info" %}
Feast currently does not apply feature transformations to data.
{% endhint %}

### Creating and managing features

Feature creators model the data within their organization into Feast through the creation of [feature tables](feature-tables.md).

Feature tables are both a schema and a means of identifying data sources for features. They allow Feast to know how to interpret your data, and optionally where to find it. Feature tables allow you to define domain [entities](entities.md) along with the features that are available on these entities. Feature tables also allow you to define schemas that describe properties of the respective data, which in turn can be used for validation purposes.

After you register a feature table, Feast creates the relevant schemas to store feature data within its feature [stores](). These stores are then populated by [ingestion jobs](../user-guide/data-ingestion.md) that ingest data from data [sources](sources.md). The now data-rich stores enable Feast to provide access to features for training and serving. Alternatively, you can [ingest](../user-guide/data-ingestion.md) data into Feast instead of using an external source.

Visit [feature tables](feature-tables.md) to learn more about them.

### Retrieving historical features during training

Historical retrieval uses [feature references](../user-guide/feature-retrieval.md) through the[ Feast SDK](https://api.docs.feast.dev/python/) to retrieve historical features. For historical serving, Feast requires that you provide the entities and timestamps for the corresponding feature data. Feast produces a point-in-time correct dataset using the requested features. These features can be requested from an unlimited number of feature sets.

{% hint style="info" %}
For historical serving, Feast stores all historical values.
{% endhint %}

Stores supported: [BigQuery](https://cloud.google.com/bigquery)

### Retrieving online features during serving

Online retrieval uses feature references through the [Feast Online Serving API](https://api.docs.feast.dev/grpc/feast.serving.pb.html) to retrieve online features. Online serving allows for very low latency requests to feature data at very high throughput.

{% hint style="info" %}
During online serving, Feast stores **only** the latest values for each feature.
{% endhint %}

Stores supported: [Redis](https://redis.io/), [Redis Cluster](https://redis.io/topics/cluster-tutorial)

## Concept Hierarchy

![](../.gitbook/assets/concept_hierarchy.png)

Feast resources are arranged in the above hierarchy, with projects grouping one or more [entities](entities.md), which in turn groups [feature tables](feature-tables.md). These feature tables consist of [data sources](sources.md) and multiple features.

The logical grouping of these resources is important for namespacing and retrieval. Retrieval requires referencing individual features through feature references. These references uniquely identify a feature within a Feast deployment.

### Concepts

[Entities](entities.md) are objects in an organization that model a specific construct. Examples of these include customers, transactions, and drivers.

[Sources](sources.md) are either internal or external data sources where feature data can be found.

[Feature Tables](feature-tables.md) are schemas that define logical groupings of features, data sources, and other related metadata.

[Stores]() are databases that maintain feature data that gets served to models during training or inference.

[Ingestion](../user-guide/data-ingestion.md) is the process of loading data into Feast.

