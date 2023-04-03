# SQLite online store

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

The full set of configuration options is available in [SqliteOnlineStoreConfig](https://rtd.feast.dev/en/latest/#feast.infra.online_stores.sqlite.SqliteOnlineStoreConfig).

## Functionality Matrix

The set of functionality supported by online stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the Sqlite online store.

| | Sqlite |
| :-------------------------------------------------------- | :-- |
| write feature values to the online store                  | yes |
| read feature values from the online store                 | yes |
| update infrastructure (e.g. tables) in the online store   | yes |
| teardown infrastructure (e.g. tables) in the online store | yes |
| generate a plan of infrastructure changes                 | yes |
| support for on-demand transforms                          | yes |
| readable by Python SDK                                    | yes |
| readable by Java                                          | no  |
| readable by Go                                            | yes |
| support for entityless feature views                      | yes |
| support for concurrent writing to the same key            | no  |
| support for ttl (time to live) at retrieval               | no  |
| support for deleting expired data                         | no  |
| collocated by feature view                                | yes |
| collocated by feature service                             | no  |
| collocated by entity key                                  | no  |

To compare this set of functionality against other online stores, please see the full [functionality matrix](overview.md#functionality-matrix).
