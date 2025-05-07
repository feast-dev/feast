# ScyllaDB Cloud online store

## Description

ScyllaDB is a low-latency and high-performance Cassandra-compatible (uses CQL) database. You can use the existing Cassandra connector to use ScyllaDB as an online store in Feast.

The [ScyllaDB](https://www.scylladb.com/) online store provides support for materializing feature values into a ScyllaDB or [ScyllaDB Cloud](https://www.scylladb.com/product/scylla-cloud/) cluster for serving online features real-time.

## Getting started

Install Feast with Cassandra support:
```bash
pip install "feast[cassandra]"
```

Create a new Feast project:
```bash
feast init REPO_NAME -t cassandra
```

### Example (ScyllaDB)

{% code title="feature_store.yaml" %}
```yaml
project: scylla_feature_repo
registry: data/registry.db
provider: local
online_store:
    type: cassandra
    hosts:
        - 172.17.0.2
    keyspace: feast
    username: scylla
    password: password
```
{% endcode %}

### Example (ScyllaDB Cloud)

{% code title="feature_store.yaml" %}
```yaml
project: scylla_feature_repo
registry: data/registry.db
provider: local
online_store:
    type: cassandra
    hosts:
        - node-0.aws_us_east_1.xxxxxxxx.clusters.scylla.cloud
        - node-1.aws_us_east_1.xxxxxxxx.clusters.scylla.cloud
        - node-2.aws_us_east_1.xxxxxxxx.clusters.scylla.cloud
    keyspace: feast
    username: scylla
    password: password
```
{% endcode %}


The full set of configuration options is available in [CassandraOnlineStoreConfig](https://rtd.feast.dev/en/master/#feast.infra.online_stores.cassandra_online_store.cassandra_online_store.CassandraOnlineStoreConfig).
For a full explanation of configuration options please look at file
`sdk/python/feast/infra/online_stores/contrib/cassandra_online_store/README.md`.

Storage specifications can be found at `docs/specs/online_store_format.md`.

## Functionality Matrix

The set of functionality supported by online stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the Cassandra plugin.

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

## Resources

* [Sample application with ScyllaDB](https://feature-store.scylladb.com/stable/)
* [ScyllaDB website](https://www.scylladb.com/)
* [ScyllaDB Cloud documentation](https://cloud.docs.scylladb.com/stable/)
