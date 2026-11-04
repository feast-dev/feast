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
* Per-feature-view **namespace overrides** and **set overrides** — pin individual feature views to RAM-only or SSD-backed namespaces, or isolate one view in its own set, without splitting projects.
* **Prewriting hook** — a configurable, import-string-resolved callable applied to every write batch for cross-cutting concerns like PII masking, application-side encryption, or value coercion.
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
  ttl_seconds: 86400             # 24h record-level TTL
  read_timeout_ms: 150            # hard deadline for a single-record get
  write_timeout_ms: 300           # hard deadline for a single-record put/operate
  batch_total_timeout_ms: 500     # hard deadline for online_read / online_write_batch
  batch_max_records: 1000         # chunk size for batch_write / batch_operate
  socket_timeout_ms: 50           # per-attempt deadline so max_retries can fire
  max_retries: 2
```
{% endcode %}

> **Timeout semantics.** The Aerospike client distinguishes per-attempt
> (`socket_timeout`) from total (`total_timeout`) deadlines. `*_timeout_ms` map
> to `total_timeout` — the overall budget for a call including retries. Set
> `socket_timeout_ms` as well so each individual attempt has its own (shorter)
> deadline; without it, `max_retries` effectively never fires because the
> first attempt is allowed to consume the entire total deadline.

> **Batch chunking.** `online_read` and `online_write_batch` split large
> requests into chunks of at most `batch_max_records` (default `1000`).
> Aerospike enforces a per-node batch limit via the server `batch-max-requests`
> setting (historically `5000`). Lower `batch_max_records` if your cluster cap
> is tighter; raise it only when the server limit and client timeouts allow.

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

### Per-feature-view namespace and set overrides

Two `Dict[str, str]` config fields — `namespace_overrides` and `set_overrides` — let you place individual feature views on a different Aerospike namespace or set without splitting your project across stores. Anything not listed in either map falls back to the store-level default (`namespace` / `set_name_template`).

Common reasons to reach for these:

* A **hot, latency-sensitive view** belongs on a RAM-only namespace; a **wide, cold view** belongs on an SSD-backed namespace. Same project, different storage tiers.
* You want `feast apply` deletions or `truncate` on one feature view to be O(1) without scanning records of the others — give that view its own set.

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: aerospike
  hosts:
    - ["aerospike.internal", 3000]
  namespace: feast                # default namespace
  set_name_template: "{project}_{collection_suffix}"
  namespace_overrides:
    driver_realtime_stats: feast_ram   # in-memory namespace
    driver_history_lookup: feast_ssd   # device-backed namespace
  set_overrides:
    isolated_view: my_feature_repo_isolated
```
{% endcode %}

> **Tradeoffs.**
>
> * Every namespace listed in `namespace_overrides` MUST already exist on the cluster — Aerospike cannot create namespaces at runtime, and a missing namespace surfaces as an opaque `AEROSPIKE_ERR_PARAM` on the first read or write.
> * Putting feature views on different sets means a multi-feature-view read for the same entity becomes one Aerospike round trip per set, not one round trip total. Only opt in when the operational isolation is worth that cost. Reads that touch a single feature view are unaffected.
> * Admin operations honour the overrides automatically: `update()` (called by `feast apply`) groups dropped feature views by their resolved `(namespace, set)` and issues one background scan per group; `teardown()` truncates every unique `(namespace, set)` pair the project may have written to (including the store-level default).

### Prewriting hooks

`prewriting_hook` is the import path of a callable that is invoked once per `online_write_batch` call, receives the rows about to be written, and returns the rows that actually go on the wire. Use it for cross-cutting write-side concerns that you don't want sprinkled through every materialization job — PII masking, application-side encryption, dual-write fan-out, value coercion, etc.

Hooks are referenced by import string (rather than as a Python `Callable` value) so the config survives YAML/JSON serialisation and remote-feature-server transport. The resolved callable is cached on the store instance, so import cost is paid once per store lifetime.

**Hook signature:**

```python
def hook(
    config: RepoConfig,
    table: FeatureView,
    data: list[
        tuple[
            EntityKeyProto,
            dict[str, ValueProto],
            datetime,
            datetime | None,
        ]
    ],
) -> list[
    tuple[
        EntityKeyProto,
        dict[str, ValueProto],
        datetime,
        datetime | None,
    ]
]:
    ...
```

The hook MUST return a row list with the same schema as its input. Returning `[]` short-circuits the write — same path as an empty input, no wire call is issued. Hooks that raise will fail the whole batch; there is no per-row fallback.

**1. Drop a hook function in your project.** Any module on the `PYTHONPATH` of every process that writes through Feast will do (the materialization workers, the registry CLI host, and the feature server, if you run one).

{% code title="my_feature_repo/hooks.py" %}
```python
"""Prewriting hooks for the Aerospike online store."""
from __future__ import annotations

import hashlib
import os
from datetime import datetime
from typing import Optional

from feast import FeatureView
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RepoConfig

# Names of features that must never reach the online store as plaintext.
# Matched by exact feature name; tweak to your project's conventions.
_SENSITIVE_FEATURES = {"email", "phone_number", "ssn"}


def hash_pii_string_features(
    config: RepoConfig,
    table: FeatureView,
    data: list[
        tuple[
            EntityKeyProto,
            dict[str, ValueProto],
            datetime,
            Optional[datetime],
        ]
    ],
) -> list[
    tuple[
        EntityKeyProto,
        dict[str, ValueProto],
        datetime,
        Optional[datetime],
    ]
]:
    """Replace any sensitive string feature with a salted SHA-256 hex digest.

    The hash is deterministic (same input → same digest) so downstream lookups
    that hash the candidate value the same way still hit. ``FEAST_PII_SALT``
    must be set on every process that materialises features; an unset salt
    raises rather than silently falling back to plaintext.
    """
    salt = os.environ.get("FEAST_PII_SALT")
    if salt is None:
        raise RuntimeError(
            "FEAST_PII_SALT is not set; refusing to write feature batches "
            "without a configured PII salt."
        )
    salt_bytes = salt.encode("utf-8")

    def _digest(plaintext: str) -> str:
        h = hashlib.sha256()
        h.update(salt_bytes)
        h.update(plaintext.encode("utf-8"))
        return h.hexdigest()

    transformed: list[
        tuple[
            EntityKeyProto,
            dict[str, ValueProto],
            datetime,
            Optional[datetime],
        ]
    ] = []
    for entity_key, values, event_ts, created_ts in data:
        new_values = dict(values)
        for feature_name in _SENSITIVE_FEATURES.intersection(new_values):
            v = new_values[feature_name]
            if v.HasField("string_val") and v.string_val:
                new_values[feature_name] = ValueProto(string_val=_digest(v.string_val))
        transformed.append((entity_key, new_values, event_ts, created_ts))
    return transformed
```
{% endcode %}

**2. Reference the hook from `feature_store.yaml`:**

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
  prewriting_hook: my_feature_repo.hooks.hash_pii_string_features
```
{% endcode %}

> **Operational notes.**
>
> * The hook is **only invoked on the write path**; reads pass through the store untouched. If your hook is one-way (e.g. hashing) you have to apply the same transformation to the candidate value at read time yourself.
> * Hooks run inside the same process as the writer — they're not RPCs and not sandboxed. They can read environment variables, open files, call out to KMS, etc. Treat them as part of your trusted code base.
> * A misconfigured `prewriting_hook` (bad import path, missing function, non-callable target) raises `ValueError` / `TypeError` on the *first* `online_write_batch` call, not on store construction. Add a smoke test that writes one row at deploy time so misconfigurations surface before a real batch.

The full set of configuration options is available in [`AerospikeOnlineStoreConfig`](https://rtd.feast.dev/en/latest/#feast.infra.online_stores.aerospike_online_store.aerospike.AerospikeOnlineStoreConfig).

## Data Model

The Aerospike online store uses a **single set per project** with entity-key collocation. Features from multiple feature views for the same entity are stored together on a single Aerospike record, analogous to the MongoDB online store's "one document per entity" layout.

| Aerospike concept | Feast mapping                                                                 |
| :---------------- | :---------------------------------------------------------------------------- |
| Namespace         | `online_store.namespace` (must be pre-configured on the cluster); per-feature-view override via `online_store.namespace_overrides` |
| Set               | `online_store.set_name_template` → `"{project}_{collection_suffix}"` by default; per-feature-view override via `online_store.set_overrides` |
| Key               | `serialize_entity_key(entity_key)` as `bytearray` user key                      |
| Bin `features`    | Map CDT keyed by feature-view name, each value a map of `feature → native`   |
| Bin `event_ts`    | Map CDT keyed by feature-view name, each value an int64 epoch-ms timestamp    |
| Bin `created_ts`  | Top-level int64 epoch-ms timestamp (last `feast materialize`)                |

### Example record

For a single entity carrying features from two feature views (`driver_stats` and `pricing`):

```text
key:  (ns="feast", set="my_feature_repo_latest", user_key=<serialize_entity_key as bytearray>)
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
* **Entity-key bytes as the Aerospike user key.** Feast's `serialize_entity_key` output is passed as a `bytearray` user key (not `bytes` — the Python client hashes only the first byte of `bytes` keys, which would collapse distinct entities).
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
