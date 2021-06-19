# Provider

A provider is an implementation of a feature store using specific feature store components targeting a specific environment**.** More specifically, a provider is the target environment to which you have configured your feature store to deploy and run.

Providers are built to orchestrate various components \(offline store, online store, infrastructure, compute\) inside an environment. For example, the `gcp` provider supports [BigQuery](https://cloud.google.com/bigquery) as an offline store and [Datastore](https://cloud.google.com/datastore) as an online store, ensuring that these components can work together seamlessly.

Providers also come with default configurations which makes it easier for users to start a feature store in a specific environment.

Please see [feature\_store.yaml](../reference/feature-repository/feature-store-yaml.md#overview) for configuring providers.

