# Redis online store

## Description

The [Redis](https://redis.io) online store provides support for materializing feature values into Redis.

* Both Redis and Redis Cluster are supported.
* The data model used to store feature values in Redis is described in more detail [here](../../specs/online\_store\_format.md).

**Data model:** All feature views that share the same entity key are stored in a single Redis hash. The hash key is derived from the serialized entity key and the project name. Each feature's value is a hash field keyed by a murmur3 hash of `"feature_view_name:feature_name"`, and a separate `_ts:<feature_view_name>` field stores the event timestamp per feature view.

This collocated-by-entity design enables an important performance optimization: `get_online_features()` requests that span multiple feature views for the same entity can issue all `HMGET` commands in a **single Redis pipeline execution**, regardless of how many feature views are requested. See [Performance characteristics](#performance-characteristics) below.

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

## TTL configuration

The Redis online store supports two complementary TTL mechanisms:

### Key-level TTL (`key_ttl_seconds`)

Sets a Redis `EXPIRE` on the entire entity hash key. When the TTL elapses, Redis automatically deletes all feature values for that entity across **all** feature views that share the same key. Use this to bound memory usage and automatically evict stale entity data.

```yaml
online_store:
  type: redis
  key_ttl_seconds: 604800   # 7 days
  connection_string: "localhost:6379"
```

{% hint style="warning" %}
Because all feature views for the same entity share one Redis hash key, `key_ttl_seconds` uses the **entity** as the expiry unit, not the feature view. Writing any feature view for an entity resets the TTL for the whole hash. This means a frequently written feature view can keep a stale, infrequently written feature view alive beyond its intended TTL.
{% endhint %}

{% hint style="info" %}
`FeatureView.ttl` defines the **offline retrieval window** (how far back in time point-in-time joins look in the offline store). It does **not** filter online store reads. To control online data expiry, use `key_ttl_seconds`.
{% endhint %}

## Performance characteristics

### Batched multi-feature-view reads

Unlike most online stores, the Redis implementation overrides `get_online_features()` to issue a **single pipeline execution** for all feature views in the request. Because all feature views for the same entity live in the same Redis hash, all `HMGET` commands across every feature view are batched into one `pipeline.execute()` call.

| Feature views in request | Redis round trips (before) | Redis round trips (after) |
| :---: | :---: | :---: |
| 1 | 1 | 1 |
| 5 | 5 | 1 |
| 10 | 10 | 1 |
| 20 | 20 | 1 |

Benchmark results against Redis 8.6.2 (localhost, 50 entities, 3 features/FV, 300 rounds):

| Feature views | Master (per-FV pipeline) | Improved (batched pipeline) | Speedup |
| :---: | ---: | ---: | :---: |
| 1 | 1.57 ms | 1.32 ms | 1.19× |
| 5 | 7.27 ms | 5.63 ms | 1.29× |
| 10 | 15.64 ms | 10.65 ms | 1.47× |
| 20 | 36.33 ms | 21.21 ms | **1.71×** |

The speedup grows with the number of feature views and is most pronounced in production environments with non-trivial network RTT to Redis.

### Write path: `skip_dedup` for bulk loads

By default, `online_write_batch()` checks existing timestamps before writing (to avoid overwriting newer data with older data). This requires two pipeline round trips per batch: one to read existing timestamps, one to write new values.

For initial bulk loads or append-only pipelines where out-of-order writes are not a concern, set `skip_dedup: true` to write in a **single pipeline round trip**:

```yaml
online_store:
  type: redis
  connection_string: "localhost:6379"
  skip_dedup: true
```

{% hint style="warning" %}
With `skip_dedup: true`, writes always overwrite existing data regardless of timestamp order. Under concurrent writers, an older record can overwrite a newer one. Use only for controlled bulk loads or pipelines that guarantee ordered delivery.
{% endhint %}

### Async write support

The Redis online store implements `online_write_batch_async()` using the async Redis client. This enables non-blocking batch writes in async serving frameworks. `skip_dedup` is also respected in the async path.

## Configuration reference

| Parameter | Default | Description |
| --- | --- | --- |
| `type` | `redis` | Online store type selector |
| `redis_type` | `redis` | Connection type: `redis`, `redis_cluster`, or `redis_sentinel` |
| `connection_string` | `localhost:6379` | Host:port and optional parameters. For cluster: `redis1:6379,redis2:6379,ssl=true,password=...` |
| `sentinel_master` | `mymaster` | Sentinel master name (only used when `redis_type: redis_sentinel`) |
| `key_ttl_seconds` | `null` | Redis `EXPIRE` TTL in seconds applied to the entity hash key after each write. Expires all feature views for that entity together. |
| `full_scan_for_deletion` | `true` | When `true`, deleting or renaming a feature view scans Redis to remove its hash fields. Set `false` to skip deletion scans (faster `feast apply`, but leaves orphaned data). |
| `skip_dedup` | `false` | When `true`, skips the existing-timestamp read before each write, halving write round trips. Suitable for initial bulk loads; may cause older values to overwrite newer ones under concurrent writers. |

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
| async batch writes                                        | yes   |
| batched multi-feature-view reads (single pipeline)        | yes   |

To compare this set of functionality against other online stores, please see the full [functionality matrix](overview.md#functionality-matrix).
