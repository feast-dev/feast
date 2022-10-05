# Bigtable online store

## Description

The [Bigtable](https://cloud.google.com/bigtable) online store provides support for
materializing feature values into Cloud Bigtable. The data model used to store feature
values in Bigtable is described in more detail
[here](../../specs/online_store_format.md#google-bigtable-online-store-format).

## Getting started

In order to use this online store, you'll need to run `pip install 'feast[gcp]'`. You
can then get started with the command `feast init REPO_NAME -t gcp`.

## Example

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: gcp
online_store:
  type: bigtable
  project_id: my_gcp_project
  instance: my_bigtable_instance
```
{% endcode %}

The full set of configuration options is available in
[BigtableOnlineStoreConfig](https://rtd.feast.dev/en/latest/#feast.infra.online_stores.bigtable.BigtableOnlineStoreConfig).

## Functionality Matrix

The set of functionality supported by online stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the Bigtable online store.

|                                                           | Bigtable |
|-----------------------------------------------------------|----------|
| write feature values to the online store                  | yes      |
| read feature values from the online store                 | yes      |
| update infrastructure (e.g. tables) in the online store   | yes      |
| teardown infrastructure (e.g. tables) in the online store | yes      |
| generate a plan of infrastructure changes                 | no       |
| support for on-demand transforms                          | yes      |
| readable by Python SDK                                    | yes      |
| readable by Java                                          | no       |
| readable by Go                                            | no       |
| support for entityless feature views                      | yes      |
| support for concurrent writing to the same key            | yes      |
| support for ttl (time to live) at retrieval               | no       |
| support for deleting expired data                         | no       |
| collocated by feature view                                | yes      |
| collocated by feature service                             | no       |
| collocated by entity key                                  | yes      |

To compare this set of functionality against other online stores, please see the full [functionality matrix](overview.md#functionality-matrix).
