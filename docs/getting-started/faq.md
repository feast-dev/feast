# FAQ

{% hint style="info" %}
**Don't see your question?**

We encourage you to ask questions on [Slack](https://slack.feast.dev) or [GitHub](https://github.com/feast-dev/feast). Even better, once you get an answer, add the answer to this FAQ via a [pull request](../project/development-guide.md)!
{% endhint %}

## Getting started

### Do you have any examples of how Feast should be used?

The [quickstart](quickstart.md) is the easiest way to learn about Feast. For more detailed tutorials, please check out the [tutorials](../tutorials/tutorials-overview/) page.

## Concepts

### Do feature views have to include entities?

No, there are [feature views without entities](concepts/feature-view.md#feature-views-without-entities).

### How does Feast handle model or feature versioning?

Feast expects that each version of a model corresponds to a different feature service.

Feature views once they are used by a feature service are intended to be immutable and not deleted (until a feature service is removed). In the future, `feast plan` and `feast apply` will throw errors if it sees this kind of behavior.

### What is the difference between data sources and the offline store?

The data source itself defines the underlying data warehouse table in which the features are stored. The offline store interface defines the APIs required to make an arbitrary compute layer work for Feast (e.g. pulling features given a set of feature views from their sources, exporting the data set results to different formats). Please see [data sources](concepts/data-ingestion.md) and [offline store](architecture-and-components/offline-store.md) for more details.

### Is it possible to have offline and online stores from different providers?

Yes, this is possible. For example, you can use BigQuery as an offline store and Redis as an online store.

## Functionality

### How do I run `get_historical_features` without providing an entity dataframe?

Feast does not provide a way to do this right now. This is an area we're actively interested in contributions for. See [GitHub issue](https://github.com/feast-dev/feast/issues/1611)

### Does Feast provide security or access control?

Feast currently does not support any access control other than the access control required for the Provider's environment (for example, GCP and AWS permissions).

It is a good idea though to lock down the registry file so only the CI/CD pipeline can modify it. That way data scientists and other users cannot accidentally modify the registry and lose other team's data.

### Does Feast support streaming sources?

Yes. In earlier versions of Feast, we used Feast Spark to manage ingestion from stream sources. In the current version of Feast, we support [push based ingestion](../reference/data-sources/push.md). Feast also defines a [stream processor](../tutorials/building-streaming-features.md) that allows a deeper integration with stream sources.

### Does Feast support feature transformation?

There are several kinds of transformations:

* On demand transformations (See [docs](../reference/alpha-on-demand-feature-view.md))
  * These transformations are Pandas transformations run on batch data when you call `get_historical_features` and at online serving time when you call \`get\_online\_features.
  * Note that if you use push sources to ingest streaming features, these transformations will execute on the fly as well
* Batch transformations (WIP, see [RFC](https://docs.google.com/document/d/1964OkzuBljifDvkV-0fakp2uaijnVzdwWNGdz7Vz50A/edit))
  * These will include SQL + PySpark based transformations on batch data sources.
* Streaming transformations (RFC in progress)

### Does Feast have a Web UI?

Yes. See [documentation](../reference/alpha-web-ui.md).

### Does Feast support composite keys?

A feature view can be defined with multiple entities. Since each entity has a unique join\_key, using multiple entities will achieve the effect of a composite key.

### How does Feast compare with Tecton?

Please see a detailed comparison of Feast vs. Tecton [here](https://www.tecton.ai/feast/). For another comparison, please see [here](https://mlops.community/learn/feature-store/).

### What are the performance/latency characteristics of Feast?

Feast is designed to work at scale and support low latency online serving. See our [benchmark blog post](https://feast.dev/blog/feast-benchmarks/) for details.

### Does Feast support embeddings and list features?

Yes. Specifically:

* Simple lists / dense embeddings:
  * BigQuery supports list types natively
  * Redshift does not support list types, so you'll need to serialize these features into strings (e.g. json or protocol buffers)
  * Feast's implementation of online stores serializes features into Feast protocol buffers and supports list types (see [reference](https://github.com/feast-dev/feast/blob/master/docs/specs/online\_store\_format.md#appendix-a-value-proto-format))
* Sparse embeddings (e.g. one hot encodings)
  * One way to do this efficiently is to have a protobuf or string representation of [https://www.tensorflow.org/guide/sparse\_tensor](https://www.tensorflow.org/guide/sparse\_tensor)

### Does Feast support X storage engine?

The list of supported offline and online stores can be found [here](../reference/offline-stores/) and [here](../reference/online-stores/), respectively. The [roadmap](../roadmap.md) indicates the stores for which we are planning to add support. Finally, our Provider abstraction is built to be extensible, so you can plug in your own implementations of offline and online stores. Please see more details about customizing Feast [here](../how-to-guides/customizing-feast/).

### Does Feast support using different clouds for offline vs online stores?

Yes. Using a GCP or AWS provider in `feature_store.yaml` primarily sets default offline / online stores and configures where the remote registry file can live (Using the AWS provider also allows for deployment to AWS Lambda). You can override the offline and online stores to be in different clouds if you wish.

### What is the difference between a data source and an offline store?

The data source and the offline store are closely tied, but separate concepts. 
The offline store controls how feast talks to a data store for historical feature retrieval, and the data source points to specific table (or query) within a data store. Offline stores are infrastructure-level connectors to data stores like Snowflake.

Additional differences:

- Data sources may be specific to a project (e.g. feed ranking), but offline stores are agnostic and used across projects.
- A feast project may define several data sources that power different feature views, but a feast project has a single offline store.
- Feast users typically need to define data sources when using feast, but only need to use/configure existing offline stores without creating new ones.

### How can I add a custom online store?

Please follow the instructions [here](../how-to-guides/customizing-feast/adding-support-for-a-new-online-store.md).

### Can the same storage engine be used for both the offline and online store?

Yes. For example, the Postgres connector can be used as both an offline and online store (as well as the registry).

### Does Feast support S3 as a data source?

Yes. There are two ways to use S3 in Feast:

* Using Redshift as a data source via Spectrum ([AWS tutorial](https://docs.aws.amazon.com/redshift/latest/dg/tutorial-nested-data-create-table.html)), and then continuing with the [Running Feast with Snowflake/GCP/AWS](../how-to-guides/feast-snowflake-gcp-aws/) guide. See a [presentation](https://youtu.be/pMFbRJ7AnBk?t=9463) we did on this at our apply() meetup.
* Using the `s3_endpoint_override` in a `FileSource` data source. This endpoint is more suitable for quick proof of concepts that won't necessarily scale for production use cases.

### Is Feast planning on supporting X functionality?

Please see the [roadmap](../roadmap.md).

## Project

### How do I contribute to Feast?

For more details on contributing to the Feast community, see [here](../community.md) and this [here](../project/contributing.md).

## Feast 0.9 (legacy)

### What is the difference between Feast 0.9 and Feast 0.10+?

Feast 0.10+ is much lighter weight and more extensible than Feast 0.9. It is designed to be simple to install and use. Please see this [document](https://docs.google.com/document/d/1AOsr\_baczuARjCpmZgVd8mCqTF4AZ49OEyU4Cn-uTT0) for more details.

### How do I migrate from Feast 0.9 to Feast 0.10+?

Please see this [document](https://docs.google.com/document/d/1AOsr\_baczuARjCpmZgVd8mCqTF4AZ49OEyU4Cn-uTT0). If you have any questions or suggestions, feel free to leave a comment on the document!

### What are the plans for Feast Core, Feast Serving, and Feast Spark?

Feast Core and Feast Serving were both part of Feast Java. We plan to support Feast Serving. We will not support Feast Core; instead we will support our object store based registry. We will not support Feast Spark. For more details on what we plan on supporting, please see the [roadmap](../roadmap.md).
