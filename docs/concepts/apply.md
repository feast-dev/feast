# Apply

The `apply` command is used to persist your feature definitions in the feature registry and to configure or provision the necessary infrastructure for your feature store to operate

## What happens during `feast apply`?

### 1. Scan the Feature Repository

Feast will scan Python files in your feature repository, and find all Feast object definitions, such as feature views, entities, and data sources.

### 2. Update metadata

If all definitions look valid, Feast will sync the metadata about Feast objects to the registry. The registry is a tiny database storing most of the same information you have in the feature repository. This step is necessary because the production feature serving infrastructure won't be able to access Python files in the feature repository at run time, but it will be able to efficiently and securely read the feature definitions from the registry.

### 3. Create cloud infrastructure

Feast CLI will create all necessary infrastructure for feature serving and materialization to work. What exactly gets created depends on which provider is configured to be used in `feature_store.yaml` in the feature repository.

For example, for the `local` provider, it is as easy as creating a sqlite database on disk as a key-value store to serve feature data from. The `local` provider is most usable for local testing, not production use.

A full featured configuration is to use the `gcp` provider and Cloud Datastore to store feature data. When you run `feast apply`, Feast will initialize Datastore based on your Feast objects \(like feature views\) in order to materialize and serve features.

{% hint style="warning" %}
Since `feast apply` \(when configured to use non-Local provider\) will create cloud infrastructure in your AWS or GCP account, it may incur some costs on your cloud bill.
{% endhint %}

