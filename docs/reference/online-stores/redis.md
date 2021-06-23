# Redis

### Description

The [Redis](https://redis.io/) online store provides support for materializing feature values into Redis.

* Both Redis and Redis Cluster are supported
* The data model used to store feature values in Redis is described in more detail [here](https://github.com/feast-dev/feast/blob/master/docs/specs/online_store_format.md).

### Examples

Connecting to a single Redis instance

{% code title="feature\_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: redis
  connection_string: "localhost:6379"
```
{% endcode %}

Connecting to a Redis Cluster with SSL enabled and password authentication

{% code title="feature\_store.yaml" %}
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

Configuration options are available [here](https://rtd.feast.dev/en/master/#feast.repo_config.RedisOnlineStoreConfig).

