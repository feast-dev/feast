# MySQL online store

## Description

The MySQL online store provides support for materializing feature values into a MySQL database for serving online features.

* Only the latest feature values are persisted

## Getting started
In order to use this online store, you'll need to run `pip install 'feast[mysql]'`. You can get started by then running `feast init` and then setting the `feature_store.yaml` as described below.

## Example

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
    type: mysql
    host: DB_HOST
    port: DB_PORT
    database: DB_NAME
    user: DB_USERNAME
    password: DB_PASSWORD
```
{% endcode %}

The full set of configuration options is available in [MySQLOnlineStoreConfig](https://rtd.feast.dev/en/master/#feast.infra.online_stores.mysql_online_store.MySQLOnlineStoreConfig).

## Functionality Matrix

The set of functionality supported by online stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the Mys online store.

|                                                           | Mys  |
| :-------------------------------------------------------- | :--- |
| write feature values to the online store                  | yes  |
| read feature values from the online store                 | yes  |
| update infrastructure (e.g. tables) in the online store   | yes  |
| teardown infrastructure (e.g. tables) in the online store | yes  |
| generate a plan of infrastructure changes                 | no   |
| support for on-demand transforms                          | yes  |
| readable by Python SDK                                    | yes  |
| readable by Java                                          | no   |
| readable by Go                                            | no   |
| support for entityless feature views                      | yes  |
| support for concurrent writing to the same key            | no   |
| support for ttl (time to live) at retrieval               | no   |
| support for deleting expired data                         | no   |
| collocated by feature view                                | yes  |
| collocated by feature service                             | no   |
| collocated by entity key                                  | no   |

To compare this set of functionality against other online stores, please see the full [functionality matrix](overview.md#functionality-matrix).
