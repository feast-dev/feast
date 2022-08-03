# Registry

Feast uses a registry to store all applied Feast objects (e.g. Feature views, entities, etc). The registry exposes methods to apply, list, retrieve and delete these objects, and is an abstraction with multiple implementations.

### Options for registry implementations

By default, the registry Feast uses a file-based registry implementation, which stores the protobuf representation of the registry as a serialized file. This registry file can be stored in a local file system, or in cloud storage (in, say, S3 or GCS).

However, there's inherent limitations with a file-based registry, since changing a single field in the registry requires re-writing the whole registry file. With multiple concurrent writers, this presents a risk of data loss, or bottlenecks writes to the registry since all changes have to be serialized (e.g. when running materialization for multiple feature views or time ranges concurrently).

Alternatively, a [SQL Registry](../../tutorials/using-scalable-registry.md) can be used for a more scalable registry.

### Updating the registry

We recommend users store their Feast feature definitions in a version controlled repository, which then via CI/CD automatically stays synced with the registry. Users will often also want multiple registries to correspond to different environments (e.g. dev vs staging vs prod), with staging and production registries with locked down write access since they can impact real user traffic. See [Running Feast in Production](../../how-to-guides/running-feast-in-production.md#1.-automatically-deploying-changes-to-your-feature-definitions) for details on how to set this up.

### Accessing the registry from clients

Users can specify the registry through a `feature_store.yaml` config file, or programmatically. We often see teams preferring the programmatic approach because it makes notebook driven development very easy:

#### Option 1: programmatically specifying the registry

```python
repo_config = RepoConfig(
    registry=RegistryConfig(path="gs://feast-test-gcs-bucket/registry.pb"),
    project="feast_demo_gcp",
    provider="gcp",thon
    offline_store="file",  # Could also be the OfflineStoreConfig e.g. FileOfflineStoreConfig
    online_store="null",  # Could also be the OnlineStoreConfig e.g. RedisOnlineStoreConfig
)
store = FeatureStore(config=repo_config)
```

#### Option 2: specifying the registry in the project's `feature_store.yaml` file

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
