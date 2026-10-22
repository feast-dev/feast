# Offline store

An offline store is an interface for working with historical time-series feature values that are stored in [data sources](../../getting-started/concepts/data-ingestion.md).
The `OfflineStore` interface has several different implementations, such as the `BigQueryOfflineStore`, each of which is backed by a different storage and compute engine.
For more details on which offline stores are supported, please see [Offline Stores](../../reference/offline-stores/).

Offline stores are primarily used for two reasons:
1. Building training datasets from time-series features.
2. Materializing \(loading\) features into an online store to serve those features at low-latency in a production setting.

Offline stores are configured through the [feature\_store.yaml](../../reference/feature-repository/feature-store-yaml.md).
When building training datasets or materializing features into an online store, Feast will use the configured offline store with your configured data sources to execute the necessary data operations.

Only a single offline store can be used at a time.
Moreover, offline stores are not compatible with all data sources; for example, the `BigQuery` offline store cannot be used to query a file-based data source.

Please see [Push Source](../../reference/data-sources/push.md) for more details on how to push features directly to the offline store in your feature store.
