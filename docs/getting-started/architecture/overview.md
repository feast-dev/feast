# Overview

![Feast Architecture Diagram](<../../assets/feast_marchitecture.png>)

Feast's architecture is designed to be flexible and scalable. It is composed of several components that work together to provide a feature store that can be used to serve features for training and inference.

* Feast uses a [Push Model](push-vs-pull-model.md) to ingest data from different sources and store feature values in the 
online store. 
This allows Feast to serve features in real-time with low latency.

* Feast supports [feature transformation](feature-transformation.md) for On Demand and Streaming data sources and
  will support Batch transformations in the future. For Streaming and Batch data sources, Feast requires a separate 
[Feature Transformation Engine](feature-transformation.md#feature-transformation-engines) (in the batch case, this is 
typically your Offline Store). We are exploring adding a default streaming engine to Feast.

* Domain expertise is recommended when integrating a data source with Feast understand the [tradeoffs from different
  write patterns](write-patterns.md) to your application

* We recommend [using Python](language.md) for your Feature Store microservice. As mentioned in the document, precomputing features is the recommended optimal path to ensure low latency performance. Reducing feature serving to a lightweight database lookup is the ideal pattern, which means the marginal overhead of Python should be tolerable. Because of this we believe the pros of Python outweigh the costs, as reimplementing feature logic is undesirable. Java and Go Clients are also available for online feature retrieval.

* [Role-Based Access Control (RBAC)](rbac.md) is a security mechanism that restricts access to resources based on the roles of individual users within an organization. In the context of the Feast, RBAC ensures that only authorized users or groups can access or modify specific resources, thereby maintaining data security and operational integrity.


