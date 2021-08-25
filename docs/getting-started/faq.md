# FAQ

## Concepts

### What is the difference between feature tables and feature views?

Feature tables from Feast 0.9 have been renamed to feature views in Feast 0.10+. For more details, please see the discussion [here](https://github.com/feast-dev/feast/issues/1583).

## Functionality

### Does Feast provide security or access control?

Feast currently does not support any access control other than the access control required for the Provider's environment \(for example, GCP and AWS permissions\).

### Does Feast support composite keys?

A feature view can be defined with multiple entities. Since each entity has a unique join\_key, using multiple entities will achieve the effect of a composite key.

### How does Feast compare with Tecton?

Please see a detailed comparison of Feast vs. Tecton [here](https://www.tecton.ai/feast/). For another comparison, please see [here](https://mlops.community/learn/feature-store/).

### Is Feast planning on supporting X functionality?

Please see the [roadmap](../roadmap.md).

## Storage

### Does Feast support X storage engine?

The list of supported offline and online stores can be found [here](../reference/offline-stores/) and [here](../reference/online-stores/), respectively. The [roadmap](../roadmap.md) indicates the stores for which we are planning to add support. Finally, our Provider abstraction is built to be extensible, so you can plug in your own implementations of offline and online stores. Please see more details about custom providers [here](../how-to-guides/creating-a-custom-provider.md).

### How can I add a custom storage engine?

Please follow the instructions [here](../how-to-guides/adding-support-for-a-new-online-store.md).

## Compute

### How can I use Spark with Feast?

Feast does not support Spark natively. However, you can create a [custom provider](../how-to-guides/creating-a-custom-provider.md) that will support Spark, which can help with more scalable materialization and ingestion.

## Project

### What is the difference between Feast 0.9 and Feast 0.10+?

Feast 0.10+ is much lighter weight and more extensible than Feast 0.9. It is designed to be simple to install and use. Please see this [document](https://docs.google.com/document/d/1AOsr_baczuARjCpmZgVd8mCqTF4AZ49OEyU4Cn-uTT0) for more details.

### How do I migrate from Feast 0.9 to Feast 0.10+?

Please see this [document](https://docs.google.com/document/d/1AOsr_baczuARjCpmZgVd8mCqTF4AZ49OEyU4Cn-uTT0). If you have any questions or suggestions, feel free to leave a comment on the document!

### How do I contribute to Feast?

For more details on contributing to the Feast community, see [here](../community.md) and this [here](../project/contributing.md).

### What are the plans for Feast Core, Feast Serving, and Feast Spark?

Feast Core and Feast Serving were both part of Feast Java. We plan to support Feast Serving. We will not support Feast Core; instead we will support our object store based registry. We will not support Feast Spark. For more details on what we plan on supporting, please see the [roadmap](../roadmap.md).

## Examples

### Do you have any examples of how Feast should be used?

The [quickstart](quickstart.md) is the easiest way to learn about Feast. For more detailed tutorials, please check out the [tutorials](../tutorials/tutorials-overview.md) page.

