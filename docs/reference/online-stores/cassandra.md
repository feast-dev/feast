# Cassandra + Astra DB online store (contrib)

## Description

The [[Cassandra](https://cassandra.apache.org/_/index.html) / [Astra DB](https://www.datastax.com/products/datastax-astra?utm_source=feast)] online store provides support for materializing feature values into an Apache Cassandra / Astra DB database for online features.

* The whole project is contained within a Cassandra keyspace
* Each feature view is mapped one-to-one to a specific Cassandra table
* This implementation inherits all strengths of Cassandra such as high availability, fault-tolerance, and data distribution

## Getting started
In order to use this online store, you'll need to run `pip install 'feast[cassandra]'`. You can then get started with the command `feast init REPO_NAME -t cassandra`.

### Example (Cassandra)

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
    type: cassandra
    hosts:
        - 192.168.1.1
        - 192.168.1.2
        - 192.168.1.3
    keyspace: KeyspaceName
    port: 9042                                                              # optional
    username: user                                                          # optional
    password: secret                                                        # optional
    protocol_version: 5                                                     # optional
    load_balancing:                                                         # optional
        local_dc: 'datacenter1'                                             # optional
        load_balancing_policy: 'TokenAwarePolicy(DCAwareRoundRobinPolicy)'  # optional
    read_concurrency: 100                                                   # optional
    write_concurrency: 100                                                  # optional
```
{% endcode %}

### Example (Astra DB)

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
    type: cassandra
    secure_bundle_path: /path/to/secure/bundle.zip
    keyspace: KeyspaceName
    username: Client_ID
    password: Client_Secret
    protocol_version: 4                                                     # optional
    load_balancing:                                                         # optional
        local_dc: 'eu-central-1'                                            # optional
        load_balancing_policy: 'TokenAwarePolicy(DCAwareRoundRobinPolicy)'  # optional
    read_concurrency: 100                                                   # optional
    write_concurrency: 100                                                  # optional
```
{% endcode %}

The full set of configuration options is available in [CassandraOnlineStoreConfig](https://rtd.feast.dev/en/master/#feast.infra.online_stores.contrib.cassandra_online_store.cassandra_online_store.CassandraOnlineStoreConfig).
For a full explanation of configuration options please look at file
`sdk/python/feast/infra/online_stores/contrib/cassandra_online_store/README.md`.

Storage specifications can be found at `docs/specs/online_store_format.md`.

## Functionality Matrix

The set of functionality supported by online stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the Cassandra online store.

|                                                           | Cassandra |
| :-------------------------------------------------------- | :-------- |
| write feature values to the online store                  | yes       |
| read feature values from the online store                 | yes       |
| update infrastructure (e.g. tables) in the online store   | yes       |
| teardown infrastructure (e.g. tables) in the online store | yes       |
| generate a plan of infrastructure changes                 | yes       |
| support for on-demand transforms                          | yes       |
| readable by Python SDK                                    | yes       |
| readable by Java                                          | no        |
| readable by Go                                            | no        |
| support for entityless feature views                      | yes       |
| support for concurrent writing to the same key            | no        |
| support for ttl (time to live) at retrieval               | no        |
| support for deleting expired data                         | no        |
| collocated by feature view                                | yes       |
| collocated by feature service                             | no        |
| collocated by entity key                                  | no        |

To compare this set of functionality against other online stores, please see the full [functionality matrix](overview.md#functionality-matrix).
