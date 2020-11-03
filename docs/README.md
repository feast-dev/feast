# Introduction

### What is Feast?

Feast \(**Fea**ture **St**ore\) is a tool for managing and serving machine learning features.

Feast aims to:

* Provide a unified means of managing feature data, whether you are a single user or a large enterprise.
* Provide scalable and performant access to feature data when training and serving models.
* Provide consistent and point-in-time correct access to feature data.
* Enable discovery, documentation, and insights into your features.

![](.gitbook/assets/feast-docs-overview-diagram-2.svg)

Feast decouples feature engineering from feature usage. Features that are added to Feast become available immediately for training and serving. Models can retrieve the same features used in training from a low latency online store in production.

This means that new ML projects start with a process of feature selection from a catalog instead of having to do feature engineering from scratch.

### How can I get started?

{% hint style="info" %}
The best way to learn Feast is to use it. Jump over to our [Quickstart](quickstart.md) guide to have one of our examples running in no time at all!
{% endhint %}

 Explore the following resources to get started with Feast:

* The [Getting Started](getting-started/) section provides guides on [Installing Feast](getting-started/install-feast/) and [Connecting to Feast](getting-started/connect-to-feast/).
* The [Concepts](./) section describes all important Feast API concepts.
* The [User guide](user-guide/data-ingestion.md) section provides guidance on completing Feast workflows.
* The [Examples](https://github.com/feast-dev/feast/tree/master/examples) section contains Jupyter notebooks that you can run on your Feast deployment.
* The [Advanced]() section contains information about both advanced and operational aspects of Feast.
* The [Reference](reference/api/) section contains detailed API and design documents for advanced users.
* The [Contributing](contributing/contributing.md) section contains resources for anyone who wants to contribute to Feast.

