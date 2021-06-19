# Redis

### Description

The [Redis](https://redis.io/) online store provides support for materializing feature values into Redis.

* Both Redis and Redis Cluster are supported
* The data model used to store feature values in Redis is described in more detail [here](https://github.com/feast-dev/feast/blob/master/docs/specs/online_store_format.md).

### Example

{% code title="feature\_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: redis
  connection_string: localhost:6379
```
{% endcode %}

Configuration options are available [here](https://rtd.feast.dev/en/master/#feast.repo_config.RedisOnlineStoreConfig).

