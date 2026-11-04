# HBase online store

## Description

The [HBase](https://hbase.apache.org/) online store provides support for materializing feature values into an Apache HBase database for serving online features in real-time.

* Each feature view is mapped to an HBase table
* Connects to HBase via the Thrift server using [happybase](https://happybase.readthedocs.io/)

## Getting started
In order to use this online store, you'll need to run `pip install 'feast[hbase]'`.

## Example

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
    type: hbase
    host: localhost
    port: "9090"
    connection_pool_size: 4    # optional
    protocol: binary           # optional
    transport: buffered        # optional
```
{% endcode %}

The full set of configuration options is available in [HbaseOnlineStoreConfig](https://rtd.feast.dev/en/master/#feast.infra.online_stores.hbase_online_store.hbase.HbaseOnlineStoreConfig).

## Functionality Matrix

The set of functionality supported by online stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the HBase online store.

|                                                           | HBase |
| :-------------------------------------------------------- | :---- |
| write feature values to the online store                  | yes   |
| read feature values from the online store                 | yes   |
| update infrastructure (e.g. tables) in the online store   | yes   |
| teardown infrastructure (e.g. tables) in the online store | yes   |
| generate a plan of infrastructure changes                 | no    |
| support for on-demand transforms                          | yes   |
| readable by Python SDK                                    | yes   |
| readable by Java                                          | no    |
| readable by Go                                            | no    |
| support for entityless feature views                      | yes   |
| support for concurrent writing to the same key            | no    |
| support for ttl (time to live) at retrieval               | no    |
| support for deleting expired data                         | no    |
| collocated by feature view                                | yes   |
| collocated by feature service                             | no    |
| collocated by entity key                                  | no    |

To compare this set of functionality against other online stores, please see the full [functionality matrix](overview.md#functionality-matrix).
