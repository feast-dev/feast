# Offline Store

An offline store is a storage and compute system where historic feature data can be stored or accessed for building training datasets or for sourcing data for materialization into the online store.

Offline stores are used primarily for two reasons

1. Building training datasets
2. Querying data sources for feature data in order to load these features into your online store

Feast does not actively manage your offline store. Instead, you are asked to select an offline store \(like `BigQuery` or the `File` offline store\) and then to introduce batch sources from these stores using [data sources](data-model-and-concepts.md#data-source) inside feature views.

Feast will use your offline store to query these sources. It is not possible to query all data sources from all offline stores, and only a single offline store can be used at a time. For example, it is not possible to query a BigQuery table from a `File` offline store, nor is it possible for a `BigQuery` offline store to query files in your local file system.

Please see [feature\_store.yaml](../reference/feature-repository/feature-store-yaml.md#overview) for configuring your offline store.

