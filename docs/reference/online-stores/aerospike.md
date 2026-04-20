# Aerospike online store (Preview)

## Description

The [Aerospike](https://aerospike.com/) online store provides support for materializing feature values into an Aerospike cluster for serving online features.

{% hint style="warning" %}
The Aerospike online store is currently in **preview**. Some functionality may be unstable, and breaking changes may occur in future releases.
{% endhint %}

## Features

* Supports both synchronous and asynchronous read/write paths (`online_read` / `online_read_async`, `online_write_batch` / `online_write_batch_async`). Async methods wrap the blocking client in `run_in_executor`, keeping the event loop responsive in feature-server workloads.
* Partial, server-side upserts via Aerospike Map CDT operations — writing one feature view never clobbers another feature view stored on the same entity.
* Record-level TTL controlled by a single `ttl_seconds` config option (honours the namespace default, a "never expire" sentinel, or an explicit number of seconds).
* Authentication and TLS options for Aerospike Enterprise Edition passed straight through to the Aerospike Python client.
* `client_kwargs` escape hatch for any advanced client-config field not surfaced on `AerospikeOnlineStoreConfig`.
* Baseline: Aerospike Server **≥ 6.0** (uses batch-write / batch-operate APIs). The store has been developed against CE 8.x.

## Getting started

Install the Aerospike extra (alongside the dependency for the offline store of choice):

```bash
pip install 'feast[aerospike]'
```

You can start from any of the standard templates (e.g. `feast init -t local` or `feast init -t aws`) and then swap in Aerospike as the online store as shown below.

## Examples

### Basic configuration — local Aerospike CE

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: aerospike
  hosts:
    - ["127.0.0.1", 3000]
  namespace: feast
```
{% endcode %}

### Multi-node cluster

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: aerospike
  hosts:
    - ["aerospike-1.internal", 3000]
    - ["aerospike-2.internal", 3000]
    - ["aerospike-3.internal", 3000]
  namespace: feast
  ttl_seconds: 86400            # 24h record-level TTL
  read_timeout_ms: 50
  write_timeout_ms: 200
  total_timeout_ms: 500
  max_retries: 2
```
{% endcode %}

### Aerospike Enterprise with authentication

> Requires Aerospike Enterprise Edition. The Community Edition server has no built-in user/security model and will reject these config keys.

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: aerospike
  hosts:
    - ["aerospike.internal", 3000]
  namespace: feast
  user: feast_user
  password: ${AEROSPIKE_PASSWORD}   # pragma: allowlist secret
  auth_mode: internal               # internal | external | pki
```
{% endcode %}

### Aerospike Enterprise with TLS

> Requires Aerospike Enterprise Edition. The Community Edition server does not implement TLS, so `tls` config is effective only against EE clusters.

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: aerospike
  hosts:
    - ["aerospike-1.internal", 4333, "aerospike-tls"]
  namespace: feast
  tls:
    enable: true
    cafile: /etc/aerospike/certs/ca.pem
    certfile: /etc/aerospike/certs/client.pem
    keyfile: /etc/aerospike/certs/client.key
```
{% endcode %}

The full set of configuration options is available in [`AerospikeOnlineStoreConfig`](https://rtd.feast.dev/en/latest/#feast.infra.online_stores.aerospike_online_store.aerospike.AerospikeOnlineStoreConfig).

## Data Model

The Aerospike online store uses a **single set per project** with entity-key collocation. Features from multiple feature views for the same entity are stored together on a single Aerospike record, analogous to the MongoDB online store's "one document per entity" layout.

| Aerospike concept | Feast mapping                                                                 |
| :---------------- | :---------------------------------------------------------------------------- |
| Namespace         | `online_store.namespace` (must be pre-configured on the cluster)              |
| Set               | `online_store.set_name_template` → `"{project}_{collection_suffix}"` by default |
| Key               | `serialize_entity_key(entity_key)` (raw bytes)                                |
| Bin `features`    | Map CDT keyed by feature-view name, each value a map of `feature → native`   |
| Bin `event_ts`    | Map CDT keyed by feature-view name, each value an int64 epoch-ms timestamp    |
| Bin `created_ts`  | Top-level int64 epoch-ms timestamp (last `feast materialize`)                |

### Example record

For a single entity carrying features from two feature views (`driver_stats` and `pricing`):

```text
key:  (ns="feast", set="my_feature_repo_latest", user_key=<serialized_entity_key bytes>)
bins:
  features:
    driver_stats:
      rating:        4.91
      trips_last_7d: 132
    pricing:
      surge_multiplier: 1.2
  event_ts:
    driver_stats: 1737374400000     # 2025-01-20T12:00:00Z
    pricing:      1737447000000     # 2025-01-21T08:30:00Z
  created_ts:      1737460805000    # 2025-01-21T12:00:05Z
```

### Key design decisions

* **Record per entity, bin per concept.** `features` and `event_ts` are Aerospike Map CDT bins, not dynamic bins, which keeps the store within the 15-byte Aerospike bin-name limit regardless of how many feature views a project has.
* **Partial upserts via Map CDT ops.** Writes use `batch_write` with `map_put_items("features", {<fv>: {...}})` and `map_put("event_ts", <fv>, <epoch_ms>)`. Concurrent writes to different feature views on the same entity never clobber each other — each write mutates only its own map keys.
* **Entity-key bytes as the Aerospike user key.** Feast's `serialize_entity_key` output is used directly as the user key, so lookups are O(1) and no secondary index is needed.
* **Timestamps as int64 epoch milliseconds.** Aerospike has no native datetime type; tz-naive timestamps are treated as UTC per the `OnlineStore` contract.

### TTL and expiry

`ttl_seconds` is written as record-level metadata on every `online_write_batch` call:

| `ttl_seconds` | Aerospike TTL                     | Effect                                                      |
| :------------ | :-------------------------------- | :---------------------------------------------------------- |
| not set / `null` | `TTL_NAMESPACE_DEFAULT`        | Record inherits the namespace's configured `default-ttl`. |
| `0`           | `TTL_NEVER_EXPIRE`               | Record is kept until explicitly deleted.                    |
| `>0`          | that many seconds                 | Record is evicted by the server's `nsup` thread.            |

There is no per-feature-view TTL override in this version — the setting is applied uniformly for every write made by the online store.

### Indexes

No secondary indexes are created. All access goes through the primary key, which is the serialized entity key.

## Async support

Async read/write are provided by running the Aerospike Python client's blocking calls on the default thread-pool executor (`loop.run_in_executor`). The underlying C client releases the GIL during network I/O, so `await store.online_read_async(...)` keeps the event loop responsive. A native asyncio Aerospike client is not currently used.

Both sync and async methods are fully supported:

* `online_read` / `online_read_async`
* `online_write_batch` / `online_write_batch_async`
* `initialize` / `close` — `initialize(config)` eagerly opens the connection so feature servers pay the TCP/handshake cost at startup; `close()` releases the cached client.

## Functionality Matrix

The set of functionality supported by online stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the Aerospike online store.

|                                                           | Aerospike |
| :-------------------------------------------------------- | :-------- |
| write feature values to the online store                  | yes       |
| read feature values from the online store                 | yes       |
| update infrastructure (e.g. tables) in the online store   | yes       |
| teardown infrastructure (e.g. tables) in the online store | yes       |
| generate a plan of infrastructure changes                 | no        |
| support for on-demand transforms                          | yes       |
| readable by Python SDK                                    | yes       |
| readable by Java                                          | no        |
| readable by Go                                            | no        |
| support for entityless feature views                      | yes       |
| support for concurrent writing to the same key            | yes       |
| support for ttl (time to live) at retrieval               | yes       |
| support for deleting expired data                         | yes       |
| collocated by feature view                                | no        |
| collocated by feature service                             | no        |
| collocated by entity key                                  | yes       |

To compare this set of functionality against other online stores, please see the full [functionality matrix](overview.md#functionality-matrix).
