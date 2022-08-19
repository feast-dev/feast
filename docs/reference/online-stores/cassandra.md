# Cassandra + Astra DB online store (contrib)

## Description

The [Cassandra / Astra DB] online store provides support for materializing feature values into an Apache Cassandra / Astra DB database for online features.

* The whole project is contained within a Cassandra keyspace
* Each feature view is mapped one-to-one to a specific Cassandra table
* This implementation inherits all strengths of Cassandra such as high availability, fault-tolerance, and data distribution

An easy way to get started is the command `feast init REPO_NAME -t cassandra`.

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

```
{% endcode %}

For a full explanation of configuration options please look at file
`sdk/python/feast/infra/online_stores/contrib/cassandra_online_store/README.md`.

Storage specifications can be found at `docs/specs/online_store_format.md`.