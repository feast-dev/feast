# Registry

The Feast feature registry is a central catalog of all the feature definitions and their related metadata. It allows data scientists to search, discover, and collaborate on new features.

Each Feast deployment has a single feature registry. Feast only supports file-based registries today, but supports three different backends

* `Local`: Used as a local backend for storing the registry during development
* `S3`: Used as a centralized backend for storing the registry on AWS
* `GCS`: Used as a centralized backend for storing the registry on GCP

The feature registry is updated during different operations when using Feast. More specifically, objects within the registry \(entities, feature views, feature services\) are updated when running `apply` from the Feast CLI, but metadata about objects can also be updated during operations like materialization.

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

