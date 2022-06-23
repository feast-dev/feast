# Offline store

Feast uses offline stores as storage and compute systems. Offline stores store historic time-series feature values. Feast does not generate these features, but instead uses the offline store as the interface for querying existing features in your organization.

Offline stores are used primarily for two reasons

1. Building training datasets from time-series features.
2. Materializing \(loading\) features from the offline store into an online store in order to serve those features at low latency for prediction.

Offline stores are configured through the [feature\_store.yaml](../../reference/offline-stores/). When building training datasets or materializing features into an online store, Feast will use the configured offline store along with the data sources you have defined as part of feature views to execute the necessary data operations.

It is not possible to query all data sources from all offline stores, and only a single offline store can be used at a time. For example, it is not possible to query a BigQuery table from a `File` offline store, nor is it possible for a `BigQuery` offline store to query files from your local file system.

Please see the [Offline Stores](../../reference/offline-stores/) reference for more details on configuring offline stores.

Please see the [Push Documentation](reference/data-sources/push.md) for reference on how to push features directly to the offline store in your feature store.

