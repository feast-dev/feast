# Online Server Performance Tuning

This guide covers end-to-end performance tuning for the Feast Python online feature server, with a focus on Kubernetes deployments using the [Feast Operator](./feast-on-kubernetes.md). It brings together worker configuration, registry caching, online store selection, horizontal scaling, observability, and network optimization into a single operational playbook.

## Request lifecycle

Understanding where time is spent during a `get_online_features()` call helps you focus tuning efforts on the right layer:

1. **Registry lookup** — resolve feature references to metadata (cached locally).
2. **Online store read** — fetch feature values from the backing database (network I/O).
3. **On-demand transformation** — execute any ODFVs attached to the request (CPU).
4. **Response serialization** — encode the result as JSON or protobuf.

In most production deployments, **step 2 (online store read)** dominates latency. Registry refresh (step 1) can cause periodic latency spikes if not configured for background refresh. Step 3 matters only when ODFVs are present.

---

## Feature view and feature service structuring

How you organize features into feature views directly affects online serving latency, because **each distinct feature view in a request produces a separate online store read**. This is a structural decision that no amount of runtime tuning can fix after the fact.

### Fewer feature views = fewer store round-trips

When the server processes a `get_online_features()` call, it groups the requested features by their source feature view and issues one `online_read()` per group. For sync stores, these reads are **sequential**; for async stores (DynamoDB, MongoDB), they run **concurrently** via `asyncio.gather()`, but each is still a separate network round-trip.

| Request shape | Store reads | Impact |
| ------------- | ----------- | ------ |
| 10 features from **1** feature view | 1 read | Minimal latency |
| 10 features from **5** feature views | 5 reads | 5× the network round-trips (sequential) or 5 concurrent reads competing for connections (async) |
| 10 features from **10** feature views | 10 reads | Latency dominated by store reads, not transformation |

**Guideline:** For features that share the same entity key and are frequently requested together, consolidate them into a **single feature view**. This reduces the number of store round-trips per request. Split feature views only when features have different entities, different materialization schedules, or different data source update frequencies.

{% hint style="info" %}
**Redis exception:** The Redis online store overrides `get_online_features()` to batch all `HMGET` commands across every feature view into a **single pipeline execution**. Because all feature views for the same entity share one Redis hash key, the number of Redis round trips is always **1**, regardless of how many feature views the request touches. This means the "fewer feature views" guideline is less critical for Redis than for other stores — but consolidating feature views still reduces serialization and protobuf overhead at the application layer.
{% endhint %}

### Feature services are free

A [Feature Service](../getting-started/concepts/feature-retrieval.md) is a named collection of feature references — it's a convenience grouping, not a separate storage or execution unit. Using a feature service adds only a registry lookup (cached) compared to listing features individually. There is no performance penalty for using feature services, and they are the recommended way to define stable, versioned feature sets for production models.

### ODFV overhead is additive

Regular feature views incur only **store read** cost. On-demand feature views add **CPU-bound transformation** cost on top:

```
Total request latency ≈ store_read_time + sum(odfv_transformation_times)
```

Each ODFV in the request runs its transformation function after all store reads complete. The overhead depends on:

- **Number of ODFVs** in the request — each one adds its transformation time.
- **Transformation complexity** — a simple arithmetic operation is microseconds; ML model inference can be milliseconds.
- **Transformation mode** — `mode="python"` is 3–10× faster than `mode="pandas"` for online serving (see [ODFV optimization](#on-demand-feature-view-odfv-optimization)).
- **Entity count** — transformations run across all entity rows in the request. More entities = more work.

### Hidden store reads from ODFV source dependencies

ODFVs declare source feature views. When you request features from an ODFV, the server must also read **all source feature views** that the ODFV depends on, even if you didn't explicitly request features from those views. This can add unexpected store reads:

```python
@on_demand_feature_view(
    sources=[driver_stats_fv, vehicle_stats_fv, request_source],
    schema=[Field(name="combined_score", dtype=Float64)],
    mode="python",
)
def combined_score(inputs: dict[str, Any]) -> dict[str, Any]:
    return {"combined_score": [d + v for d, v in zip(inputs["driver_rating"], inputs["vehicle_rating"])]}
```

Requesting just `combined_score` triggers reads from **both** `driver_stats_fv` and `vehicle_stats_fv`. If those are already needed by other features in the same request, there's no extra cost (the server deduplicates). But if they're only needed by the ODFV, these are hidden reads you should account for.

### Structuring recommendations

| Practice | Why |
| -------- | --- |
| Group co-accessed features into the same feature view | Reduces store round-trips per request |
| Use feature services to define stable production feature sets | No performance cost; improves governance and versioning |
| Minimize the number of ODFVs per request | Each ODFV adds CPU-bound transformation latency |
| Prefer `write_to_online_store=True` for ODFVs that don't need request-time data | Moves compute from serving to materialization path |
| Audit ODFV source dependencies | Avoid pulling in unnecessary store reads via unused source feature views |
| Use `track_metrics=True` on ODFVs during profiling | Identifies which transforms are the bottleneck |

---

## Worker and connection tuning

The Python feature server uses Gunicorn with async workers. Tuning workers, connections, and timeouts directly impacts throughput and tail latency.

### Feast Operator CR configuration

```yaml
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: my-feast
spec:
  feastProject: my_project
  services:
    onlineStore:
      server:
        workerConfigs:
          workers: 4
          workerConnections: 2000
          maxRequests: 5000
          maxRequestsJitter: 500
          keepAliveTimeout: 30
          registryTTLSeconds: 300
        resources:
          requests:
            cpu: "2"
            memory: 2Gi
          limits:
            cpu: "4"
            memory: 4Gi
```

### CLI equivalent

```bash
feast serve \
  --workers 4 \
  --worker-connections 2000 \
  --max-requests 5000 \
  --max-requests-jitter 500 \
  --keep-alive-timeout 30 \
  --registry_ttl_sec 300
```

### Tuning recommendations

| Parameter | Default | Recommended starting point | Notes |
| --------- | ------- | -------------------------- | ----- |
| `workers` | 1 | `2 × CPU cores + 1` (use `-1` for auto) | Each worker is a separate process. More workers = more parallel requests, but also more memory. |
| `workerConnections` | 1000 | 1000–2000 | Max simultaneous connections **per worker**. Increase for high-concurrency workloads behind a load balancer. |
| `maxRequests` | 1000 | 5000–10000 | Recycles a worker after N requests to guard against memory leaks. Higher values reduce recycle overhead. |
| `maxRequestsJitter` | 50 | 200–500 | Randomizes recycle timing to avoid a thundering herd of simultaneous worker restarts. |
| `keepAliveTimeout` | 30 | 30–60 | Seconds to keep an idle connection open. Match this to your load balancer's idle timeout. |
| `registryTTLSeconds` | 60 | 300 | How often the registry cache is refreshed. See [Registry cache tuning](#registry-cache-tuning) below. |

{% hint style="info" %}
**Sizing rule of thumb:** Start with `workers = 2 × vCPU + 1` and allocate ~256–512 MB memory per worker. Monitor RSS per process and adjust. If p99 latency is high under load, add more workers or scale horizontally rather than increasing connections.
{% endhint %}

---

## Registry cache tuning

The default registry configuration uses **synchronous** cache refresh. When the TTL expires, the **next** `get_online_features()` call blocks while the full registry is re-downloaded. For large registries this can add tens of milliseconds to a single request.

### The problem

With the default `cache_mode: sync` (or unset), the refresh cycle looks like:

```
Request N   →  cache hit (fast)
  ...TTL expires...
Request N+1 →  cache miss → synchronous download → response delayed
Request N+2 →  cache hit (fast)
```

### The fix: background thread refresh

Configure the registry to refresh in a background thread so that no serving request ever blocks on a download:

{% code title="feature_store.yaml (inside the Operator secret)" %}
```yaml
registry:
  registry_type: sql
  path: postgresql://<DB_USERNAME>:<DB_PASSWORD>@<DB_HOST>:5432/feast
  cache_mode: thread
  cache_ttl_seconds: 300
```
{% endcode %}

With `cache_mode: thread`:

- The cache is populated **eagerly** at startup.
- A background thread refreshes every `cache_ttl_seconds`.
- Serving requests always read from the **in-memory** cache and are never blocked.

When deploying with the Feast Operator using a **file-based** registry, set these fields directly on the CR:

```yaml
spec:
  services:
    registry:
      local:
        persistence:
          file:
            cache_mode: thread
            cache_ttl_seconds: 300
```

{% hint style="warning" %}
For DB/SQL-backed registries deployed via the Operator, `cache_mode` and `cache_ttl_seconds` are set inside the secret's YAML payload rather than on the CR struct. Ensure your secret includes these keys.
{% endhint %}

### The trade-off: freshness vs. performance

Registry caching is a **freshness vs. performance** trade-off. The registry contains metadata — feature view definitions, entity schemas, data source configs — not the feature values themselves. When someone runs `feast apply` to add or modify a feature view, the change is invisible to the serving pods until the cache refreshes.

**Lower TTL** (e.g., 10–30s):
- Schema changes propagate faster to serving pods.
- But each refresh re-downloads and deserializes the entire registry, consuming CPU and network bandwidth.
- With `cache_mode: sync`, a lower TTL means **more frequent latency spikes** — the unlucky request that triggers the refresh is blocked for the full download duration.
- With `cache_mode: thread`, no request is directly blocked, but the background thread competes with Gunicorn workers for CPU during deserialization. On CPU-constrained pods, frequent refreshes can **indirectly increase p99 serving latency** by starving workers of CPU cycles.

**Higher TTL** (e.g., 300–600s):
- Fewer refreshes mean less CPU contention, resulting in **more stable and predictable serving latency**.
- But schema changes take longer to propagate. A new feature view added via `feast apply` won't be queryable until the cache refreshes (up to `cache_ttl_seconds` delay).
- This is usually acceptable in production where schema changes are infrequent and deployed through CI/CD pipelines, not ad-hoc.

**Memory impact:** The full registry is deserialized into memory on each worker process. For registries with hundreds of feature views, this can be tens of megabytes per worker. With 4 workers × 5 replicas, that's 20 copies of the registry in memory. A higher TTL doesn't reduce memory usage (the cache is always held), but it reduces the CPU cost of periodic deserialization — which in turn keeps CPU available for serving requests with lower latency.

{% hint style="info" %}
The `registryTTLSeconds` field on the Operator CR (or `--registry_ttl_sec` CLI flag) controls how often the **online server** refreshes its cache. The `cache_ttl_seconds` in `feature_store.yaml` controls how often the **SDK client** refreshes. In an Operator deployment, the CR field is what matters for server performance.
{% endhint %}

### Recommendations

| Scenario | `cache_mode` | `cache_ttl_seconds` |
| -------- | ------------ | ------------------- |
| Development / iteration | `sync` (default) | 5–10 |
| Production (low-latency) | `thread` | 300 |
| Production (frequent schema changes) | `thread` | 60 |

---

## Online store selection

The online store is the single largest factor in `get_online_features()` latency. Choose based on your latency budget, throughput needs, and existing infrastructure.

| Store | Typical p50 latency | Async read | Best for | Key trade-off |
| ----- | ------------------- | ---------- | -------- | ------------- |
| **Redis / Dragonfly** | < 1 ms | No (threadpool) | Ultra-low latency, high throughput; all FV reads batched into 1 pipeline | Requires in-memory capacity for your dataset |
| **DynamoDB** | 2–5 ms | Yes | Serverless, auto-scaling on AWS | Pay-per-request cost; batch API limits (100 items) |
| **PostgreSQL** | 3–10 ms | No (threadpool) | Teams with existing Postgres infra | Connection pooling needed at scale |
| **MongoDB** | 2–5 ms | Yes | Flexible schema, async-native | Requires index tuning for large datasets |
| **Bigtable** | 3–8 ms | No (threadpool) | Large-scale GCP workloads | Row-key design affects read performance |
| **Cassandra / ScyllaDB** | 2–5 ms | No (threadpool) | Multi-region, write-heavy | Tunable consistency; requires DC-aware routing |
| **Remote** | Varies | No (threadpool) | Centralized feature server architecture | Adds an HTTP hop; tune connection pool |
| **SQLite** | N/A | No | Local development only | No concurrent access; not suitable for production |

{% hint style="info" %}
**General rule:** If your p99 latency budget is under 10 ms, use Redis. If you need serverless scaling on AWS, use DynamoDB. For everything else, PostgreSQL with connection pooling is a strong default.
{% endhint %}

### Async vs sync online reads

The feature server can read from the online store using either an **async** or **sync** code path. The choice is made automatically based on the store's `async_supported` property — no user configuration is needed.

**How it works:**

- When a `get_online_features()` request touches multiple feature views, the server must read from each one.
- **Async path** (`async_supported.online.read = True`): All feature view reads run concurrently via `asyncio.gather()` in a single event loop — no thread overhead, minimal context switching.
- **Sync path** (default fallback): The sync `online_read()` is wrapped in `run_in_threadpool()`, which dispatches to a thread pool. This works correctly but adds thread scheduling overhead, especially when reading from many feature views.

**Current async support by store:**

| Store | Async read | Async write | Notes |
| ----- | ---------- | ----------- | ----- |
| **DynamoDB** | Yes | Yes | Uses `aiobotocore` for non-blocking I/O |
| **MongoDB** | Yes | Yes | Uses `motor` (async MongoDB driver) |
| **PostgreSQL** | Implemented | No | Has `online_read_async` but does not yet advertise via `async_supported`; uses sync/threadpool path |
| **Redis** | Implemented | **Yes** | `online_read_async` and `online_write_batch_async` both implemented; uses sync/threadpool path for `get_online_features` (overridden with batched single pipeline) |
| All others | No | No | Fall back to sync with `run_in_threadpool()` |

**When async matters most:**

- Requests that fan out across **many feature views** benefit the most — async gathers all reads concurrently without thread pool contention.
- For requests hitting a **single feature view**, the difference is negligible.
- If you are choosing between two stores with comparable latency, prefer the one with full async support for better throughput under concurrent load.

### DynamoDB tuning

```yaml
online_store:
  type: dynamodb
  region: us-west-2
  batch_size: 100
  max_read_workers: 10
  consistent_reads: false
  max_pool_connections: 100
  keepalive_timeout: 30.0
  connect_timeout: 3
  read_timeout: 5
  total_max_retry_attempts: 2
  retry_mode: adaptive
```

Key knobs:

- **`batch_size`**: DynamoDB's `BatchGetItem` accepts up to 100 items per request. For 500 entities, this means 5 batches. Keep at 100 unless hitting the 16 MB response limit.
- **`max_read_workers`**: Controls parallelism for batch reads. With 10 workers, those 5 batches run concurrently (~10 ms) instead of sequentially (~50 ms).
- **`consistent_reads: false`**: Eventually consistent reads are faster and cheaper. Use `true` only if you need read-after-write consistency.
- **`max_pool_connections`**: Increase for high-throughput deployments to improve HTTP connection reuse to the DynamoDB endpoint.
- **`keepalive_timeout`**: Longer keep-alive reduces TLS handshake overhead on reused connections.
- **`connect_timeout` / `read_timeout`**: Lower values fail fast, improving p99. Set aggressively if your retry strategy covers transient failures.
- **`total_max_retry_attempts: 2`**: Reduces p99 by capping retry delay. Combine with `retry_mode: adaptive` for backpressure-aware retries.

{% hint style="info" %}
**VPC Endpoints:** Configure a [VPC endpoint for DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/vpc-endpoints-dynamodb.html) to keep traffic on AWS's private network. This reduces latency by eliminating the public internet hop and can also reduce data transfer costs.
{% endhint %}

### PostgreSQL tuning

```yaml
online_store:
  type: postgres
  host: <DB_HOST>
  port: 5432
  database: feast
  db_schema: public
  user: <DB_USERNAME>
  password: <DB_PASSWORD>
  conn_type: pool
  min_conn: 4
  max_conn: 20
  keepalives_idle: 30
  sslmode: require
```

- **`conn_type: pool`**: Enables connection pooling via `psycopg` pool. Without this, every request opens and closes a TCP connection.
- **`min_conn` / `max_conn`**: Set `min_conn` to your expected steady-state concurrency and `max_conn` to your burst limit. Keep `max_conn` below the PostgreSQL `max_connections` divided by your replica count.
- **`keepalives_idle`**: TCP keep-alive prevents idle connections from being dropped by firewalls or load balancers.

### Redis tuning

```yaml
online_store:
  type: redis
  connection_string: "redis-cluster.internal:6379,ssl=true"
  redis_type: redis_cluster
  key_ttl_seconds: 604800
  skip_dedup: false          # set true for initial bulk loads to halve write round trips
```

- Use `redis_cluster` for horizontal partitioning across shards.
- Set `key_ttl_seconds` to auto-expire stale feature data, keeping memory usage bounded. This is a **key-level** TTL — it expires the entire entity hash (all feature views for that entity) together.
- Ensure the Redis instance is in the **same availability zone** as the feature server pods to minimize network round-trips.

#### Batched multi-feature-view reads

The Redis online store overrides `get_online_features()` to issue all `HMGET` commands — across every feature view in the request — in a **single pipeline execution**. This reduces Redis round trips from `N` (one per feature view) to `1` regardless of request size.

| Feature views | Round trips (other stores) | Round trips (Redis) |
| :---: | :---: | :---: |
| 1 | 1 | 1 |
| 5 | 5 | **1** |
| 10 | 10 | **1** |
| 20 | 20 | **1** |

This means the latency cost of adding feature views to a Redis-backed request is primarily **serialization and protobuf overhead** at the application layer, not Redis network latency.

#### Write throughput: `skip_dedup`

For initial bulk loads or append-only materialization pipelines:

```yaml
online_store:
  type: redis
  connection_string: "localhost:6379"
  skip_dedup: true
```

With `skip_dedup: true`, `online_write_batch()` skips the existing-timestamp read pipeline and writes all values directly in a single pass, halving write round trips. Use only when you can guarantee write ordering — under concurrent writers, an older record can overwrite a newer one.

### Cassandra / ScyllaDB tuning

Cassandra and ScyllaDB share the same Feast connector. The key read-path knobs are concurrency and data-center-aware routing:

```yaml
online_store:
  type: cassandra
  hosts:
  - cassandra-0.internal
  - cassandra-1.internal
  keyspace: feast
  read_concurrency: 100
  write_concurrency: 100
  request_timeout: 10
  load_balancing:
    load_balancing_policy: DCAwareRoundRobinPolicy
    local_dc: datacenter1
```

- **`read_concurrency`** (default: 100): Maximum concurrent async read operations. Increase for high fan-out workloads; decrease if the cluster shows read-timeout pressure.
- **`write_concurrency`** (default: 100): Maximum concurrent async write operations during materialization.
- **`request_timeout`** (default: driver default): Per-request timeout in seconds. Lower values fail fast and improve p99 at the cost of higher error rates under load.
- **`load_balancing.local_dc`**: Set to your local data center name so the driver routes reads to the nearest replicas, avoiding cross-DC latency.

### Bigtable tuning

Bigtable's client library uses an internal connection pool that caps concurrency:

```yaml
online_store:
  type: bigtable
  project_id: my-gcp-project
  instance: feast-instance
```

- The client library's **connection pool size** is hardcoded at 10 internally. For workloads that need higher concurrent reads, scale horizontally (more feature server replicas) rather than trying to push more concurrency through a single pod.
- **`max_versions`** (default: 2): Number of cell versions retained. Lower values reduce storage and read amplification.
- Co-locate the feature server in the **same GCP region** as your Bigtable instance.

### MongoDB tuning

MongoDB supports async reads natively. Use `client_kwargs` to pass performance options directly to the PyMongo / Motor driver:

```yaml
online_store:
  type: mongodb
  connection_string: "mongodb://mongo.internal:27017"
  database_name: feast
  client_kwargs:
    maxPoolSize: 100
    minPoolSize: 10
    maxIdleTimeMS: 30000
    connectTimeoutMS: 3000
    socketTimeoutMS: 5000
```

- **`maxPoolSize` / `minPoolSize`**: Controls the driver connection pool. Default `maxPoolSize` is 100 in PyMongo, but explicitly setting it ensures predictability across versions.
- **`connectTimeoutMS` / `socketTimeoutMS`**: Tighter timeouts improve p99 by failing fast on slow connections.
- MongoDB is one of the stores with **full async support** (read and write), so it benefits from concurrent feature view reads via `asyncio.gather()`.

### Remote online store tuning

The Remote online store connects to a Feast feature server over HTTP. Connection pooling is critical:

```yaml
online_store:
  type: remote
  path: https://feast-server.internal:6566
  connection_pool_size: 50
  connection_idle_timeout: 300
  connection_retries: 3
```

- **`connection_pool_size`** (default: 50): Number of persistent HTTP connections. Increase for high-throughput deployments to reduce connection setup overhead.
- **`connection_idle_timeout`** (default: 300s): Seconds before idle connections are closed. Set to 0 to disable idle cleanup (useful when traffic is bursty).
- **`connection_retries`** (default: 3): Number of retries on transient HTTP failures. Lower for tighter p99; higher for better reliability.

---

## Batch size tuning

Different online stores have different optimal batch sizes for `get_online_features()` calls. The batch size controls how many entity keys are fetched per round-trip to the store.

| Store | Default batch size | Max batch size | Recommendation |
| ----- | ------------------ | -------------- | -------------- |
| DynamoDB | 100 | 100 (API limit) | Keep at 100; tune `max_read_workers` for parallelism |
| Redis | N/A (pipelined) | N/A | All entity keys **and** all feature views are batched into a single pipeline; no batch tuning needed |
| PostgreSQL | N/A (single query) | N/A | Single `SELECT ... WHERE key IN (...)` query; tune connection pool instead |

Profile your workload by measuring `feast_feature_server_online_store_read_duration_seconds` (see [Metrics setup](#metrics-setup-prometheus--opentelemetry)) across different entity counts to find the sweet spot.

---

## On-demand feature view (ODFV) optimization

If your feature pipeline uses ODFVs, the transformation mode significantly affects serving latency.

### Prefer native Python mode

Native Python transformations avoid pandas overhead and are substantially faster for online serving:

```python
@on_demand_feature_view(
    sources=[driver_stats_fv, request_source],
    schema=[Field(name="trip_rate", dtype=Float64)],
    mode="python",
)
def trip_rate_python(inputs: dict[str, Any]) -> dict[str, Any]:
    return {
        "trip_rate": [
            trips / max(hours, 1)
            for trips, hours in zip(inputs["total_trips"], inputs["active_hours"])
        ]
    }
```

| Mode | Relative latency | When to use |
| ---- | ---------------- | ----------- |
| `mode="python"` | **1x** (baseline) | Production serving — minimal overhead |
| `mode="pandas"` | **3–10x** | Offline batch retrieval where pandas ergonomics matter |

### Use singleton mode for single-row transforms

When your Python ODFV processes one entity at a time (common in online serving), use `singleton=True` to avoid the overhead of wrapping and unwrapping single values in lists:

```python
@on_demand_feature_view(
    sources=[driver_stats_fv, request_source],
    schema=[Field(name="trip_rate", dtype=Float64)],
    mode="python",
    singleton=True,
)
def trip_rate_singleton(inputs: dict[str, Any]) -> dict[str, Any]:
    return {"trip_rate": inputs["total_trips"] / max(inputs["active_hours"], 1)}
```

With `singleton=True`, input values are plain scalars (not lists), and the function returns scalars. This avoids list comprehension and zip overhead for single-entity requests. Singleton mode requires `mode="python"`.

### Write-time transformations

For ODFVs that don't depend on request-time data, set `write_to_online_store=True` to compute features during materialization instead of at serving time:

```python
@on_demand_feature_view(
    sources=[driver_stats_fv],
    schema=[Field(name="is_high_mileage", dtype=Bool)],
    mode="python",
    write_to_online_store=True,
)
def high_mileage(inputs: dict[str, Any]) -> dict[str, Any]:
    return {"is_high_mileage": [m > 100000 for m in inputs["total_miles"]]}
```

This moves CPU cost from the serving path to the materialization path, reducing online latency.

### Consistency vs. latency: choosing between read-time and write-time transforms

Read-time ODFVs (the default, `write_to_online_store=False`) are **request-blocking** — they execute synchronously during `get_online_features()`. This provides a **strong consistency guarantee**: the output is always computed from the latest source features in the online store and can incorporate request-time data (e.g., the user's current location or the current timestamp).

Write-time ODFVs (`write_to_online_store=True`) are pre-computed during materialization and stored. They add zero latency to the serving path, but the result can be **stale** — it reflects the source feature values at the time of the last materialization run, not the current request.

Use this decision framework:

| Condition | Recommendation |
| --------- | -------------- |
| ODFV uses **request-time data** (e.g., current timestamp, user input) | Must use read-time (default) — request data is not available at materialization |
| ODFV depends on features that change **frequently** and freshness matters | Keep read-time for strong consistency |
| ODFV is a **pure derivation** of slowly changing features (e.g., age buckets, tier labels) | Safe to move to write-time for lower latency |
| ODFV is **computationally expensive** (ML inference, complex aggregations) | Prefer write-time if staleness is acceptable; otherwise use read-time with [static artifacts](#pre-load-heavy-resources-with-static-artifacts) |

{% hint style="warning" %}
Moving an ODFV to `write_to_online_store=True` is a **performance-consistency trade-off**, not a free optimization. Verify that your use case tolerates stale results bounded by the materialization interval before switching.
{% endhint %}

### Pre-load heavy resources with static artifacts

If your ODFV uses ML models, lookup tables, or other expensive resources, load them **once** at server startup using [static artifacts loading](../reference/alpha-static-artifacts.md) instead of loading them per request:

```python
# static_artifacts.py — loaded once at feature server startup
from fastapi import FastAPI

def load_artifacts(app: FastAPI):
    from transformers import pipeline
    app.state.model = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")

    import example_repo
    example_repo._model = app.state.model
```

```python
# example_repo.py — use pre-loaded artifact in ODFV
_model = None

@on_demand_feature_view(
    sources=[text_fv],
    schema=[Field(name="sentiment_score", dtype=Float64)],
    mode="python",
)
def sentiment(inputs: dict[str, Any]) -> dict[str, Any]:
    global _model
    scores = [_model(t)[0]["score"] for t in inputs["text"]]
    return {"sentiment_score": scores}
```

Without static artifacts, the model would be loaded on every request (or every worker restart), adding seconds of latency. With pre-loading, the ODFV pays only the inference cost.

**Using static artifacts with the Feast Operator:**

The feature server looks for `static_artifacts.py` in the feature repository directory at startup. Whether this works in an operator deployment depends on how the repo is created:

- **`feastProjectDir.git`** — The operator clones your git repository via an init container. Include `static_artifacts.py` in the repo alongside your feature definitions and it will be available at startup. This is the recommended approach for production.

```yaml
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: my-feast
spec:
  feastProject: my_project
  feastProjectDir:
    git:
      url: https://github.com/my-org/feast-repo.git
      ref: main
```

- **`feastProjectDir.init`** (default) — The operator runs `feast init` to create a template repo. The generated template does not include `static_artifacts.py`. To use static artifacts in this mode, you would need to build a custom feature server image that bundles the file, or mount it via a ConfigMap/volume.

{% hint style="info" %}
For production deployments that use ODFVs with heavy resources (ML models, large lookup tables), use `feastProjectDir.git` so your `static_artifacts.py` is version-controlled and deployed automatically alongside your feature definitions.
{% endhint %}

### Transformation code best practices

Small choices inside transformation functions add up at scale:

- **Avoid I/O in the transform function.** Network calls, file reads, or database queries inside an ODFV block the serving thread. Pre-load data via static artifacts or use `write_to_online_store` to compute offline.
- **Avoid creating DataFrames.** Even in Python mode, constructing a pandas DataFrame inside the function defeats the purpose of avoiding pandas overhead.
- **Minimize allocations.** Reuse structures where possible. List comprehensions are faster than building lists with `.append()` in a loop.
- **Profile with `track_metrics=True`.** Enable per-ODFV timing to identify slow transforms, but disable it in production once you've tuned — the timer itself adds minor overhead.

```python
@on_demand_feature_view(
    sources=[driver_stats_fv],
    schema=[Field(name="score", dtype=Float64)],
    mode="python",
    track_metrics=True,  # enable during profiling, disable once tuned
)
def compute_score(inputs: dict[str, Any]) -> dict[str, Any]:
    return {"score": [x * 0.5 + y * 0.3 for x, y in zip(inputs["a"], inputs["b"])]}
```

The `feast_feature_server_transformation_duration_seconds` metric (labeled by `odfv_name` and `mode`) shows exactly how much time each ODFV adds to the serving path.

---

## Horizontal scaling

When a single pod cannot meet your throughput or latency requirements, scale horizontally using the Feast Operator.

Horizontal scaling requires **DB-backed persistence** for all services. File-based stores (SQLite, DuckDB, `registry.db`) do not support concurrent access from multiple pods. The Feast Operator supports both static replicas (`spec.replicas`) and HPA autoscaling (`services.scaling.autoscaling`). See [Scaling Feast](./scaling-feast.md) for full configuration examples.

### Expected scaling efficiency

| Replicas | Expected throughput | Notes |
| -------- | ------------------- | ----- |
| 1 | Baseline | Single-pod throughput depends on worker count and online store latency |
| 2–3 | ~1.8–2.8x | Near-linear scaling; overhead from load balancer routing |
| 5–10 | ~4.5–9x | Online store connection pressure becomes the bottleneck; tune pool sizes |
| 10+ | Diminishing returns | Online store or network bandwidth may saturate; monitor store-side metrics |

{% hint style="info" %}
Scaling the feature server is only effective if the online store can handle the aggregate connection and throughput load. Monitor the store's CPU, memory, and connection counts alongside feature server metrics.
{% endhint %}

### Coordinating database connections with scaling

When you scale horizontally, every replica and every Gunicorn worker within a replica opens its own database connection pool to the online store. The **total connection count** multiplies quickly:

```
Total connections = replicas × workers_per_pod × max_conn_per_worker
```

**Example:** 5 replicas × 4 workers × 20 `max_conn` = **400 connections** to the database. If your PostgreSQL instance has the default `max_connections: 100`, this will fail immediately.

This applies to every connection-oriented online store:

| Store | Connection setting | Default | What to check on the store side |
| ----- | ------------------ | ------- | ------------------------------- |
| **PostgreSQL** | `max_conn` (per worker pool) | 10 | `max_connections` on the server (typically 100–500) |
| **DynamoDB** | `max_pool_connections` (HTTP pool) | 10 | No hard limit, but AWS SDK has per-process pool caps; monitor throttling |
| **Redis** | Connection per worker | 1 | `maxclients` on the Redis server (default: 10,000) |
| **MongoDB** | `maxPoolSize` (in `client_kwargs`) | 100 | Server's `net.maxIncomingConnections` |
| **Cassandra** | Driver manages pool per node | Auto | `native_transport_max_threads` on each Cassandra node |
| **Remote** | `connection_pool_size` (HTTP pool) | 50 | The target feature server's worker capacity |

**How to size it:**

1. **Start from the store's limit.** Determine the maximum connections your database can handle (e.g., PostgreSQL `max_connections`, or RDS instance class limit).
2. **Reserve headroom** for other clients (materialization jobs, `feast apply`, monitoring). A common rule is to reserve 20–30% of the store's capacity.
3. **Divide by total workers.** Set `max_conn` (or equivalent) per worker so the total stays within the budget:

```
max_conn_per_worker = (store_max_connections × 0.7) / (replicas × workers_per_pod)
```

**Example calculation for PostgreSQL:**

| Parameter | Value |
| --------- | ----- |
| PostgreSQL `max_connections` | 200 |
| Reserved for other clients (30%) | 60 |
| Available for feature server | 140 |
| Replicas | 5 |
| Workers per pod | 4 |
| **`max_conn` per worker** | **140 / (5 × 4) = 7** |

```yaml
# feature_store.yaml (in the Operator secret)
online_store:
  type: postgres
  conn_type: pool
  min_conn: 2
  max_conn: 7
  ...
```

{% hint style="warning" %}
If you use HPA autoscaling, size `max_conn` based on `maxReplicas` (not `minReplicas`) to avoid connection exhaustion during scale-out. Under-provisioning here causes cascading failures: the store rejects connections, requests fail, load increases, HPA scales up further, and even more connections are rejected.
{% endhint %}

**For DynamoDB and other HTTP-based stores**, the concern is less about hard connection limits and more about SDK-level pool exhaustion and throttling. Monitor `ThrottledRequests` (DynamoDB) or HTTP `429` responses and increase pool sizes or store capacity accordingly.

### High availability

When scaling is enabled, the operator automatically injects:

- **Soft pod anti-affinity** — prefers spreading pods across nodes.
- **Zone topology spread** — distributes pods across availability zones (best-effort).

You can also configure a **PodDisruptionBudget** to limit voluntary disruptions:

```yaml
spec:
  services:
    podDisruptionBudgets:
      maxUnavailable: 1
```

See [Scaling Feast](./scaling-feast.md) for the full horizontal scaling reference, including KEDA integration.

---

## Metrics setup: Prometheus + OpenTelemetry

Observability is essential for tuning. Without metrics, you're guessing.

### Prometheus (built-in)

Enable the built-in Prometheus metrics endpoint in the Feast Operator CR:

```yaml
spec:
  services:
    onlineStore:
      server:
        metrics: true
```

This exposes a `/metrics` endpoint on port 8000. If the Prometheus Operator's `ServiceMonitor` CRD is installed, the Feast operator automatically creates a ServiceMonitor for scraping.

#### Key metrics for performance tuning

| Metric | What to watch |
| ------ | ------------- |
| `feast_feature_server_request_latency_seconds` | p50, p95, p99 latency by endpoint. The primary SLI. |
| `feast_feature_server_online_store_read_duration_seconds` | Time spent in the online store. If this is close to total latency, tune the store, not the server. |
| `feast_feature_server_transformation_duration_seconds` | ODFV execution time (requires `track_metrics=True` on the ODFV). |
| `feast_feature_server_cpu_usage` | Per-worker CPU. Sustained > 80% means you need more workers or replicas. |
| `feast_feature_server_memory_usage` | Per-worker memory. Growing over time may indicate a memory leak — use `maxRequests` to recycle. |
| `feast_feature_server_request_total` | Request throughput and error rates by endpoint and status. |
| `feast_feature_freshness_seconds` | Seconds since last materialization per feature view. Alerts you to stale data. |

#### Example Prometheus alert rules

```yaml
groups:
- name: feast-online-server
  rules:
  - alert: FeastHighP99Latency
    expr: histogram_quantile(0.99, rate(feast_feature_server_request_latency_seconds_bucket[5m])) > 0.1
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "Feast online server p99 latency exceeds 100ms"

  - alert: FeastStaleFeatures
    expr: feast_feature_freshness_seconds > 3600
    for: 10m
    labels:
      severity: critical
    annotations:
      summary: "Feature view {{ $labels.feature_view }} has not been materialized in over 1 hour"
```

### OpenTelemetry

For deeper tracing and integration with your existing observability stack, add OpenTelemetry instrumentation.

**1. Install the OpenTelemetry Operator** (requires `cert-manager`):

```bash
kubectl apply -f https://github.com/open-telemetry/opentelemetry-operator/releases/latest/download/opentelemetry-operator.yaml
```

**2. Deploy an OpenTelemetry Collector:**

```yaml
apiVersion: opentelemetry.io/v1beta1
kind: OpenTelemetryCollector
metadata:
  name: feast-otel
spec:
  mode: sidecar
  config:
    receivers:
      otlp:
        protocols:
          grpc: {}
          http: {}
    processors:
      batch: {}
    exporters:
      prometheus:
        endpoint: "0.0.0.0:8889"
    service:
      pipelines:
        metrics:
          receivers: [otlp]
          processors: [batch]
          exporters: [prometheus]
```

**3. Add instrumentation** to the FeatureStore pods via the Operator's `env` field:

```yaml
spec:
  services:
    onlineStore:
      server:
        env:
        - name: OTEL_EXPORTER_OTLP_ENDPOINT
          value: "http://localhost:4317"
        - name: OTEL_SERVICE_NAME
          value: "feast-online-server"
```

See the [OpenTelemetry Integration](../getting-started/components/open-telemetry.md) guide for the complete setup including Instrumentation CRDs and ServiceMonitors.

---

## Network latency optimization

Network round-trip time to the online store is the **dominant factor** in `get_online_features()` latency. No amount of server-side tuning can compensate for a slow network path.

### Co-locate feature server and online store

- Deploy the feature server pods in the **same AWS region and availability zone** as your online store (DynamoDB, ElastiCache, RDS, etc.).
- On Kubernetes or similar platforms, ensure the cluster and the managed database service share the same VPC and subnet.

### Use private network endpoints

Public internet round-trips add 5–50 ms of latency compared to private network paths. Use VPC endpoints or private link:

| Cloud | Service | Private endpoint |
| ----- | ------- | ---------------- |
| AWS | DynamoDB | [Gateway VPC Endpoint](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/vpc-endpoints-dynamodb.html) |
| AWS | ElastiCache (Redis) | Deploy in same VPC; no public endpoint needed |
| AWS | RDS (PostgreSQL) | Enable [RDS Private Link](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/vpc-interface-endpoints.html) or same-VPC access |
| GCP | Bigtable / Cloud SQL | [Private Service Connect](https://cloud.google.com/vpc/docs/private-service-connect) |
| Azure | Cosmos DB / Azure Cache | [Private Endpoints](https://learn.microsoft.com/en-us/azure/private-link/private-endpoint-overview) |

### DNS resolution caching

Frequent DNS lookups to the store endpoint add latency. In Kubernetes, ensure CoreDNS caching is enabled (default) and consider using IP-based endpoints for critical-path connections.

---

## Client access patterns: REST API vs. SDK with remote store

When consuming features from a Feast online server deployed via the Operator, there are three client access patterns with different performance profiles:

### Pattern comparison

| Pattern | How it works | Network hops | Client-side overhead | Best for |
| ------- | ------------ | ------------ | -------------------- | -------- |
| **Direct REST API** | HTTP POST to `/get-online-features` from any language | Client → feature server → online store | Minimal (JSON only) | Non-Python clients, lowest client-side latency, microservices in any language |
| **Feast SDK with remote store** | `store.get_online_features()` over HTTP via `online_store.type: remote` | Client → feature server → online store | SDK registry resolution + proto/JSON conversion | Python clients that want the SDK API with centralized serving |
| **Feast SDK direct** | `store.get_online_features()` directly to the store (no feature server) | Client → online store | SDK registry resolution + store driver | Python clients with direct network access to the store |

### Performance characteristics

**Direct REST API** has the lowest client-side overhead. The client sends a JSON payload and receives a JSON response — no SDK, no registry loading, no protobuf conversion. The feature server handles all processing (registry lookup, store read, ODFVs). This is the best option when:

- Your inference service is written in Java, Go, C++, or another non-Python language.
- You want to minimize client-side CPU and memory usage.
- You're already behind a load balancer and want the simplest possible integration.

```bash
curl -X POST "http://feast-server:6566/get-online-features" \
  -H "Content-Type: application/json" \
  -d '{
    "features": ["driver_stats:conv_rate", "driver_stats:acc_rate"],
    "entities": {"driver_id": [1001, 1002]}
  }'
```

**Feast SDK with remote store** (`online_store.type: remote`) adds client-side overhead: the SDK loads the registry locally to resolve feature references, converts entities to protobuf, serializes to JSON for HTTP transport, and deserializes the response back to protobuf. This means:

- The client needs access to the registry (or a remote registry server).
- Each request pays for proto → JSON → proto round-trip conversion.
- But you get the full SDK API: feature services, type checking, Python-native `OnlineResponse` objects, and built-in connection pooling.

```python
store = FeatureStore(repo_path=".")
# Internally: registry lookup → proto conversion → HTTP POST → JSON parse → proto conversion
features = store.get_online_features(
    features=["driver_stats:conv_rate", "driver_stats:acc_rate"],
    entity_rows=[{"driver_id": 1001}, {"driver_id": 1002}],
)
```

**Feast SDK direct** (`online_store.type: postgres/redis/dynamodb/...`) skips the feature server entirely and reads from the online store directly. This eliminates the HTTP hop and JSON serialization, giving the **lowest end-to-end latency** for Python clients. However, it requires:

- Direct network access from the client to the online store (often not available in production Kubernetes setups).
- Online store credentials on the client side.
- Each client manages its own connection pool to the store, which complicates connection budgeting at scale.

### Recommendation for Operator deployments

In a typical Feast Operator deployment on Kubernetes:

- **Use the direct REST API** for production inference services, especially non-Python ones. It has the least overhead and works with any language.
- **Use the SDK with remote store** for Python services that benefit from the SDK ergonomics (feature services, type safety, `OnlineResponse` helpers).
- **Avoid SDK direct** in most Operator deployments — the feature server already manages store connections, worker pools, and registry caching. Bypassing it means every client pod opens its own connections to the database, duplicating the connection budget problem.

{% hint style="info" %}
Regardless of which pattern you use, the **server-side** performance is identical — the feature server processes `/get-online-features` the same way. The performance difference is entirely on the **client side**: how much work the client does before and after the HTTP call.
{% endhint %}

---

## Putting it all together

Here is a complete Feast Operator CR for a production deployment with all the tuning recommendations applied:

```yaml
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: production-feast
spec:
  feastProject: my_project
  services:
    scaling:
      autoscaling:
        minReplicas: 2
        maxReplicas: 10
        metrics:
        - type: Resource
          resource:
            name: cpu
            target:
              type: Utilization
              averageUtilization: 70
    podDisruptionBudgets:
      maxUnavailable: 1
    onlineStore:
      server:
        metrics: true
        workerConfigs:
          workers: 4
          workerConnections: 2000
          maxRequests: 5000
          maxRequestsJitter: 500
          keepAliveTimeout: 30
          registryTTLSeconds: 300
        resources:
          requests:
            cpu: "2"
            memory: 2Gi
          limits:
            cpu: "4"
            memory: 4Gi
      persistence:
        store:
          type: postgres
          secretRef:
            name: feast-online-store
    registry:
      local:
        persistence:
          store:
            type: sql
            secretRef:
              name: feast-registry
```

With the online store secret configured for connection pooling and background registry refresh:

{% code title="feast-online-store secret (feature_store.yaml content)" %}
```yaml
online_store:
  type: postgres
  host: <DB_HOST>
  port: 5432
  database: feast
  db_schema: public
  user: <DB_USERNAME>
  password: <DB_PASSWORD>
  conn_type: pool
  min_conn: 2
  max_conn: 7
  keepalives_idle: 30
  sslmode: require
```
{% endcode %}

{% hint style="info" %}
The `max_conn: 7` above is sized for the HPA configuration in this example (maxReplicas=10, 4 workers per pod). See [Coordinating database connections with scaling](#coordinating-database-connections-with-scaling) for the calculation. Adjust based on your store's connection limit and replica count.
{% endhint %}

{% code title="feast-registry secret (feature_store.yaml content)" %}
```yaml
registry:
  registry_type: sql
  path: postgresql://<DB_USERNAME>:<DB_PASSWORD>@<DB_HOST>:5432/feast
  cache_mode: thread
  cache_ttl_seconds: 300
```
{% endcode %}

---

## Materialization write performance

Materialization (`feast materialize` / `feast materialize-incremental`) reads features from the offline store and writes them to the online store. For large feature views, two common bottlenecks arise: **memory exhaustion** during proto conversion and **write throughput** to the online store.

### Memory: `online_write_batch_size`

By default, Feast converts the entire Arrow table returned by the offline store into Python protobuf objects in a single pass before writing to the online store. For datasets with millions of rows this can consume tens of gigabytes of memory on the materialization worker.

Set `online_write_batch_size` in `feature_store.yaml` to break the write into manageable chunks:

```yaml
materialization:
  online_write_batch_size: 10000   # rows per write batch
```

Each chunk is independently converted and written, keeping peak memory proportional to the batch size rather than the full dataset. This is supported by the **local, Spark, and Ray** compute engines.

| Dataset size | Without batching | With `online_write_batch_size: 10000` |
| --- | --- | --- |
| 1 M rows (100 bytes/row) | ~100 MB peak | ~1 MB peak |
| 10 M rows | ~1 GB peak | ~1 MB peak |
| 100 M rows | OOM / swap | ~1 MB peak |

**Choosing a value:**

- **Larger batches** (50 000+): fewer write calls to the online store, lower overhead per row — good when worker memory allows.
- **Smaller batches** (1 000–5 000): lower peak memory — necessary for memory-constrained workers or very wide feature views (many features per row).
- For **Redis**: pipeline overhead per batch is negligible; a batch size of 10 000–50 000 is a good starting point.
- For **DynamoDB**: each batch maps to one or more `BatchWriteItem` calls (max 25 items per call); a larger `online_write_batch_size` amortizes the per-call overhead but doesn't change the 25-item DynamoDB limit.

See the [feature-store-yaml reference](../reference/feature-repository/feature-store-yaml.md#online_write_batch_size) for the complete option documentation.

### Throughput: parallel feature view materialization

Each feature view in a `feast materialize` call is materialized sequentially by the local engine. To materialize multiple feature views in parallel, use a job orchestrator (Airflow, Kubernetes Jobs) and materialize one feature view per job:

```bash
# Airflow / cron: one task per feature view
feast materialize-incremental $(date -u +"%Y-%m-%dT%H:%M:%S") \
  --views driver_stats
```

Alternatively, use the **Spark** or **Ray** compute engines which distribute the work across a cluster.

### Redis: combine with `skip_dedup` for bulk reloads

When performing a full historical reload into Redis (not an incremental update), combine `online_write_batch_size` with `skip_dedup` for maximum throughput:

```yaml
materialization:
  online_write_batch_size: 50000   # large chunks — memory is bounded

online_store:
  type: redis
  connection_string: "redis-cluster.internal:6379"
  skip_dedup: true                 # skip per-row timestamp check — halves write round trips
```

`skip_dedup: true` eliminates the timestamp-read pipeline before each write (see [Redis tuning](#redis-tuning)), while `online_write_batch_size` prevents the write worker from converting the entire dataset into memory at once.

{% hint style="warning" %}
Reset `skip_dedup` to `false` (or remove it) after the bulk reload. Under normal incremental materialization, deduplication prevents older feature values from overwriting newer ones.
{% endhint %}

---

## Further reading

- [Scaling Feast](./scaling-feast.md) — Horizontal scaling, HPA, KEDA, and HA in detail
- [Python Feature Server](../reference/feature-servers/python-feature-server.md) — CLI flags, metrics reference, and API endpoints
- [OpenTelemetry Integration](../getting-started/components/open-telemetry.md) — Full OTEL setup with Prometheus Operator
- [DynamoDB Online Store](../reference/online-stores/dynamodb.md) — Store-specific configuration and performance tuning
- [PostgreSQL Online Store](../reference/online-stores/postgres.md) — Connection pooling and SSL configuration
- [Redis Online Store](../reference/online-stores/redis.md) — Cluster mode, Sentinel, TTL configuration, and batched reads
- [On Demand Feature Views](../reference/beta-on-demand-feature-view.md) — Transformation modes and write-time transforms
- [feature_store.yaml reference](../reference/feature-repository/feature-store-yaml.md) — Full configuration reference including `materialization` options
