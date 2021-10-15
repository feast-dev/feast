# SQLite

## Description

The [SQLite](https://www.sqlite.org/index.html) online store provides support for materializing feature values into an SQLite database for serving online features.

* All feature values are stored in an on-disk SQLite database
* Only the latest feature values are persisted

## Example

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: sqlite
  path: data/online_store.db
```
{% endcode %}

Configuration options are available [here](https://rtd.feast.dev/en/latest/#feast.repo_config.SqliteOnlineStoreConfig).
