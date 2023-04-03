# Provider

A provider is an implementation of a feature store using specific feature store components (e.g. offline store, online store) targeting a specific environment (e.g. GCP stack).

Providers orchestrate various components (offline store, online store, infrastructure, compute) inside an environment. For example, the `gcp` provider supports [BigQuery](https://cloud.google.com/bigquery) as an offline store and [Datastore](https://cloud.google.com/datastore) as an online store, ensuring that these components can work together seamlessly. Feast has three built-in providers (`local`, `gcp`, and `aws`) with default configurations that make it easy for users to start a feature store in a specific environment. These default configurations can be overridden easily. For instance, you can use the `gcp` provider but use Redis as the online store instead of Datastore.

If the built-in providers are not sufficient, you can create your own custom provider. Please see [this guide](../../how-to-guides/customizing-feast/creating-a-custom-provider.md) for more details.

Please see [feature\_store.yaml](../../reference/feature-repository/feature-store-yaml.md#overview) for configuring providers.
