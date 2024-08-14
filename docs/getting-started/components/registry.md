# Registry

Feast uses a registry to store all applied Feast objects (e.g. Feature views, entities, etc). It allows data scientists to search, discover, and collaborate on new features. The registry exposes methods to apply, list, retrieve and delete these objects, and is an abstraction with multiple implementations.

Feast comes with built-in file-based and sql-based registry implementations. By default, Feast uses a file-based registry, which stores the protobuf representation of the registry as a serialized file in the local file system. For more details on which registries are supported, please see [Registries](../../reference/registries/).

## Updating the registry

We recommend users store their Feast feature definitions in a version controlled repository, which then via CI/CD
automatically stays synced with the registry. Users will often also want multiple registries to correspond to
different environments (e.g. dev vs staging vs prod), with staging and production registries with locked down write
access since they can impact real user traffic. See [Running Feast in Production](../../how-to-guides/running-feast-in-production.md#1.-automatically-deploying-changes-to-your-feature-definitions) for details on how to set this up.

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

<!-- Each Feast deployment has a single feature registry. Feast only supports file-based registries today, but supports four different backends.

* `Local`: Used as a local backend for storing the registry during development
* `S3`: Used as a centralized backend for storing the registry on AWS
* `GCS`: Used as a centralized backend for storing the registry on GCP
* `[Alpha] Azure`: Used as centralized backend for storing the registry on Azure Blob storage. -->


<!-- The feature registry is updated during different operations when using Feast. More specifically, objects within the registry \(entities, feature views, feature services\) are updated when running `apply` from the Feast CLI, but metadata about objects can also be updated during operations like materialization.

Users interact with a feature registry through the Feast SDK. Listing all feature views:

```python
fs = FeatureStore("my_feature_repo/")
print(fs.list_feature_views())
```

Or retrieving a specific feature view:

```python
fs = FeatureStore("my_feature_repo/")
fv = fs.get_feature_view(“my_fv1”)
```

{% hint style="info" %}
The feature registry is a [Protobuf representation](https://github.com/feast-dev/feast/blob/master/protos/feast/core/Registry.proto) of Feast metadata. This Protobuf file can be read programmatically from other programming languages, but no compatibility guarantees are made on the internal structure of the registry.
{% endhint %}
 -->
