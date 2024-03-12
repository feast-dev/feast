# Redis online store

## Description

The [Redis](https://redis.io) online store provides support for materializing feature values into Redis.

* Both Redis and Redis Cluster are supported.
* The data model used to store feature values in Redis is described in more detail [here](../../specs/online\_store\_format.md).

## Getting started
In order to use this online store, you'll need to install the redis extra (along with the dependency needed for the offline store of choice). E.g.
-  `pip install 'feast[gcp, redis]'`
-  `pip install 'feast[snowflake, redis]'`
-  `pip install 'feast[aws, redis]'`
-  `pip install 'feast[azure, redis]'`

You can get started by using any of the other templates (e.g. `feast init -t gcp` or `feast init -t snowflake` or `feast init -t aws`), and then swapping in Redis as the online store as seen below in the examples.

## Examples

Connecting to a single Redis instance:

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: redis
  connection_string: "localhost:6379"
```
{% endcode %}

Connecting to a Redis Cluster with SSL enabled and password authentication:

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: redis
  redis_type: redis_cluster
  connection_string: "redis1:6379,redis2:6379,ssl=true,password=my_password"
```
{% endcode %}

Connecting to a Redis Sentinel with SSL enabled and password authentication:

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: redis
  redis_type: redis_sentinel
  sentinel_master: mymaster
  connection_string: "redis1:26379,ssl=true,password=my_password"
```
{% endcode %}

Additionally, the redis online store also supports automatically deleting data via a TTL mechanism.
The TTL is applied at the entity level, so feature values from any associated feature views for an entity are removed together. 
This TTL can be set in the `feature_store.yaml`, using the `key_ttl_seconds` field in the online store. For example:

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: redis
  key_ttl_seconds: 604800
  connection_string: "localhost:6379"
```
{% endcode %}


The full set of configuration options is available in [RedisOnlineStoreConfig](https://rtd.feast.dev/en/latest/#feast.infra.online_stores.redis.RedisOnlineStoreConfig).

## Functionality Matrix

The set of functionality supported by online stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the Redis online store.

|                                                           | Redis |
| :-------------------------------------------------------- | :---- |
| write feature values to the online store                  | yes   |
| read feature values from the online store                 | yes   |
| update infrastructure (e.g. tables) in the online store   | yes   |
| teardown infrastructure (e.g. tables) in the online store | yes   |
| generate a plan of infrastructure changes                 | no    |
| support for on-demand transforms                          | yes   |
| readable by Python SDK                                    | yes   |
| readable by Java                                          | yes   |
| readable by Go                                            | yes   |
| support for entityless feature views                      | yes   |
| support for concurrent writing to the same key            | yes   |
| support for ttl (time to live) at retrieval               | yes   |
| support for deleting expired data                         | yes   |
| collocated by feature view                                | no    |
| collocated by feature service                             | no    |
| collocated by entity key                                  | yes   |

To compare this set of functionality against other online stores, please see the full [functionality matrix](overview.md#functionality-matrix).
