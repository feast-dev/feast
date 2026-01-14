# Registry

The Feast feature registry is a central catalog of all feature definitions and their related metadata. Feast uses the registry to store all applied Feast objects (e.g. Feature views, entities, etc). It allows data scientists to search, discover, and collaborate on new features. The registry exposes methods to apply, list, retrieve and delete these objects, and is an abstraction with multiple implementations.

Feast comes with built-in file-based and sql-based registry implementations. By default, Feast uses a file-based registry, which stores the protobuf representation of the registry as a serialized file in the local file system. For more details on which registries are supported, please see [Registries](../../reference/registries/).

## Updating the registry

We recommend users store their Feast feature definitions in a version controlled repository, which then via CI/CD
automatically stays synced with the registry. Users will often also want multiple registries to correspond to
different environments (e.g. dev vs staging vs prod), with staging and production registries with locked down write
access since they can impact real user traffic. See [Running Feast in Production](../../how-to-guides/running-feast-in-production.md#1.-automatically-deploying-changes-to-your-feature-definitions) for details on how to set this up.

## Deleting objects from the registry

{% hint style="warning" %}
Simply removing a feature definition from your code and running `feast apply` or `FeatureStore.apply()` **does not** delete the object from the registry. You must explicitly delete objects using the dedicated delete methods.
{% endhint %}

To delete objects from the registry, use the explicit delete methods provided by the `FeatureStore` class:

### Deleting feature views
```python
from feast import FeatureStore

store = FeatureStore(repo_path=".")
store.delete_feature_view("my_feature_view")
```

### Deleting feature services
```python
store.delete_feature_service("my_feature_service")
```

### Deleting entities, data sources, and other objects

For entities, data sources, and other registry objects, you can use the registry methods directly:

```python
# Delete an entity
store._registry.delete_entity("my_entity", project=store.project)

# Delete a data source
store._registry.delete_data_source("my_data_source", project=store.project)

# Delete a saved dataset
store._registry.delete_saved_dataset("my_saved_dataset", project=store.project)

# Delete a validation reference
store._registry.delete_validation_reference("my_validation_reference", project=store.project)
```

{% hint style="info" %}
When using `feast apply` via the CLI, you can also use the `objects_to_delete` parameter with `partial=False` to delete objects as part of the apply operation. However, this is less common and typically used in automated deployment scenarios.
{% endhint %}

## Accessing the registry from clients

Users can specify the registry through a `feature_store.yaml` config file, or programmatically. We often see teams
preferring the programmatic approach because it makes notebook driven development very easy:

### Option 1: programmatically specifying the registry

```python
repo_config = RepoConfig(
    registry=RegistryConfig(path="gs://feast-test-gcs-bucket/registry.pb"),
    project="feast_demo_gcp",
    provider="gcp",
    offline_store="file",  # Could also be the OfflineStoreConfig e.g. FileOfflineStoreConfig
    online_store="null",  # Could also be the OnlineStoreConfig e.g. RedisOnlineStoreConfig
)
store = FeatureStore(config=repo_config)
```

### Option 2: specifying the registry in the project's `feature_store.yaml` file

```yaml
project: feast_demo_aws
provider: aws
registry: s3://feast-test-s3-bucket/registry.pb
online_store: null
offline_store:
  type: file
```

Instantiating a `FeatureStore` object can then point to this:

```python
store = FeatureStore(repo_path=".")
```

{% hint style="info" %}
The file-based feature registry is a [Protobuf representation](https://github.com/feast-dev/feast/blob/master/protos/feast/core/Registry.proto) of Feast metadata. This Protobuf file can be read programmatically from other programming languages, but no compatibility guarantees are made on the internal structure of the registry.
{% endhint %}