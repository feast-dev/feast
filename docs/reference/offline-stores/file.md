# File

### Description

The File offline store provides support for reading [FileSources](https://github.com/feast-dev/feast/blob/c50a36ec1ad5b8d81c6f773c23204db7c7a7d218/sdk/python/feast/data_source.py#L523).

* Only Parquet files are currently supported.
* All data is downloaded and joined using Python and may not scale to production workloads.

### Example

{% code title="feature\_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
offline_store:
  type: file
```
{% endcode %}

Configuration options are available [here](https://rtd.feast.dev/en/latest/#feast.repo_config.FileOfflineStoreConfig).

