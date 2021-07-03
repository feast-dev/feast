# Overview

### Using Feast

Feast development happens through three key workflows:

1. [Define and load feature data into Feast](define-and-ingest-features.md)
2. [Retrieve historical features for training models](getting-training-features.md)
3. [Retrieve online features for serving models](getting-online-features.md)

### Defining feature tables and ingesting data into Feast

Feature creators model the data within their organization into Feast through the definition of [feature tables](../concepts/feature-tables.md) that contain [data sources](../concepts/sources.md). Feature tables are both a schema and a means of identifying data sources for features, and allow Feast to know how to interpret your data, and where to find it.

After registering a feature table with Feast, users can trigger an ingestion from their data source into Feast. This loads feature values from an upstream data source into Feast stores through ingestion jobs. 

Visit [feature tables](../concepts/feature-tables.md#overview) to learn more about them.

{% page-ref page="define-and-ingest-features.md" %}

### Retrieving historical features for training

In order to generate a training dataset it is necessary to provide both an [entity dataframe ]()and feature references through the[ Feast SDK](https://api.docs.feast.dev/python/) to retrieve historical features. For historical serving, Feast requires that you provide the entities and timestamps for the corresponding feature data. Feast produces a point-in-time correct dataset using the requested features. These features can be requested from an unlimited number of feature sets.

{% page-ref page="getting-training-features.md" %}

### Retrieving online features for online serving

Online retrieval uses feature references through the [Feast Online Serving API](https://api.docs.feast.dev/grpc/feast.serving.pb.html) to retrieve online features. Online serving allows for very low latency requests to feature data at very high throughput.

{% page-ref page="getting-online-features.md" %}

