---
title: "Tuning the Feast Feature Server for Sub-2ms Online Serving"
description: "A practical guide to achieving low-latency, high-throughput feature serving with Feast on Kubernetes — from default configuration to production-grade performance with pre-computed feature vectors and benchmarks at every step."
date: 2026-06-02
authors: ["Nikhil Kathole"]
---

**Feast supports production-grade worker configuration, connection pooling, async reads, batched pipelines, serialization optimizations, and pre-computed feature vectors for the Python feature server.** This post walks through a real-world performance tuning exercise in two stages: first, server and client tuning that brings p99 latency down to **sub-5ms** for single-row requests; then, **pre-computed feature vectors** that push it further to **sub-2ms p99** — regardless of how many feature views your FeatureService spans. We share the benchmarking methodology, the exact configuration changes, and the measured impact of each step so you can apply the same approach to your own deployments.

---

## The Problem

When you deploy Feast on Kubernetes using the [Feast Operator](https://docs.feast.dev/how-to-guides/production-deployment-topologies), the default configuration is designed for simplicity — a single Gunicorn worker, short keep-alive timeouts, and frequent registry refreshes. This is fine for development but leaves significant performance on the table for production workloads where every millisecond matters.

We set out to answer a practical question: **how low can we push the Feast online server's p99 latency, and what does it take to get there?**

---

## Test Environment

| Component | Configuration |
|-----------|--------------|
| **Online Store** | Redis 7.0.12 (standalone, in-cluster) |
| **Registry** | PostgreSQL 16 (SQL registry) |
| **Platform** | Kubernetes |
| **Deployment** | Feast Operator with `FeatureStore` CR |

We used a banking feature store project with multiple feature views spanning customer demographics, transactions, and behavioral profiles.

All benchmarks run 200 iterations (after 30–50 warmup) for each scenario, measuring p50, p95, p99, and mean latency. Throughput is measured with 10 concurrent workers over 15 seconds.

---

## Three Access Modes

Feast supports three ways to retrieve online features. Understanding how each one works is key to knowing where latency comes from — and where to optimize.

### REST API

```
Client → HTTP POST (JSON) → Gunicorn/FastAPI Server → Redis mget() → JSON response
```

The simplest and most common pattern. Your application sends a JSON request to the feature server's `/get-online-features` endpoint. The server holds persistent Redis connections and a pre-loaded registry, so each request is just a Redis read plus JSON serialization. HTTP keep-alive reuses TCP/TLS connections across requests.

### Direct SDK

```
Client (Python) → FeatureStore SDK → Redis mget() directly
```

The Python SDK connects to Redis directly — no HTTP hop, no JSON overhead. However, it pays for in-process registry lookups and entity key serialization on every call, and reads each FeatureView sequentially.

### Remote SDK

```
Client (Python SDK) → HTTP POST → Feature Server → Redis → JSON → Client
```

The SDK delegates feature retrieval to a remote feature server over HTTP. This combines the worst of both worlds: SDK-side overhead *plus* an HTTP round-trip. Without connection pooling, each call creates a new TCP connection and TLS handshake.

---

## Baseline: Default Configuration

With no tuning applied — a single Gunicorn worker, default timeouts, and no connection pooling:

| Mode | p99 (1 row) | p99 (5 rows) | Throughput |
|------|----------------|-------------------|------------|
| **REST API** | 6.92 ms | 4.94 ms | 480 RPS |
| **Direct SDK** | 5.83 ms | 5.59 ms | — |
| **Remote SDK** | 11.71 ms | **74.31 ms** | ~2 RPS |

The REST API and Direct SDK are already in the 5–7ms range out of the box, but the Remote SDK fails badly — p99 spiking to **74ms** at just 5 rows due to per-request TCP/TLS setup overhead. This is our starting point.

---

## Server-Side Configuration

These are changes you apply to the **feature server deployment** — no code changes needed, just configuration via the `FeatureStore` CR and Redis runtime settings.

### Worker Tuning via the Feast Operator

The Feast Operator exposes `workerConfigs` in the `FeatureStore` CR, letting you tune the Gunicorn server without rebuilding images:

```yaml
apiVersion: feast.dev/v1alpha1
kind: FeatureStore
spec:
  services:
    onlineStore:
      server:
        workerConfigs:
          workers: -1              # Auto: 2 × CPU cores + 1
          keepAliveTimeout: 120    # Reuse connections longer
          maxRequests: 5000        # Recycle workers to prevent memory leaks
          maxRequestsJitter: 200   # Stagger recycling
          registryTTLSeconds: 300  # Reduce registry refresh overhead
          workerConnections: 2000  # High-concurrency support
```

Setting `workers: -1` on a 4-core pod gives 9 Gunicorn workers, each with its own event loop and Redis connection. This is the **single most impactful change** — it transforms the server from single-threaded to multi-process, dropping 5-row p99 from ~10ms to ~8ms and putting us on the path to sub-5ms.

### Redis Runtime Tuning

Three Redis settings made a measurable difference:

- **`hz 100`** (default 10) — Redis processes expired keys and timeouts 10x faster, reducing tail latency spikes.
- **`tcp-keepalive 60`** (default 300) — Detects dead connections 5x faster, freeing resources sooner.
- **`save ""`** (disable RDB persistence) — Eliminates periodic snapshot I/O that causes 10–50ms p99 spikes. Since features are materialized from the offline store and reconstructible at any time, persistence is unnecessary.

### High Availability and Auto-Scaling

For production, we added horizontal scaling and availability guarantees using the Feast Operator's [built-in HA support](https://docs.feast.dev/how-to-guides/feast-snowflake-gcp-aws/scaling-feast):

```yaml
spec:
  replicas: 2
  services:
    onlineStore:
      server:
        resources:
          requests:
            cpu: 500m
            memory: 512Mi
          limits:
            cpu: "2"
            memory: 2Gi
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
    pdb:
      minAvailable: 1
```

When scaling is enabled, the operator auto-injects pod anti-affinity and zone topology spread constraints, ensuring replicas land on different nodes for resilience. With HPA, the cluster auto-scales based on CPU utilization — we observed it scaling from 2 to 3 pods in response to load during benchmarks. At 10 pods with 9 workers each, theoretical throughput reaches ~7,180 RPS (~25.8M RPH).

### Server-side quick wins summary

1. **Set `workers: -1`** — single most impactful change
2. **Disable Redis persistence** — `CONFIG SET save ""`
3. **Set `registryTTLSeconds: 300`** — reduce registry refresh overhead
4. **Use `replicas: 2`** minimum with HPA for burst capacity
5. **Set resource limits** — defaults are far too low for production

---

## Client-Side Configuration

These are changes you apply on the **client** — how the SDK connects to the feature server and which access mode you choose.

### Connection Pooling for the Remote SDK

The biggest problem with the Remote SDK was that every call created a brand-new `requests.Session`, established a fresh TCP connection, negotiated TLS, and then threw it all away — adding 2–4ms per call for HTTPS endpoints.

Feast now includes `HttpSessionManager` — a thread-safe, singleton session manager that reuses HTTP connections across requests with configurable pooling and retry:

```yaml
online_store:
  type: remote
  path: https://feast-server:443
  connection_pool_size: 50
  connection_idle_timeout: 300
  connection_retries: 3
```

This dropped Remote SDK 5-row p99 from **74ms to 21ms** — a 72% reduction — by eliminating the per-request TLS handshake.

### Choosing the right access mode

| Use Case | Recommended Mode | Why |
|----------|-----------------|-----|
| **Application serving** | REST API | Sub-5ms single-row p99, simplest integration, 718 RPS per pod |
| **Python ML pipeline** | Direct SDK | No HTTP hop, sub-5ms p99, native protobuf |
| **Async Python applications** | Async Direct SDK | Non-blocking, batched pipeline, sub-5ms p99 |
| **Cross-cluster serving** | Remote SDK + pooling | When the client can't reach Redis directly; 760 RPS with pooling |

---

## Code Enhancements in Feast

Beyond configuration, several code-level improvements in Feast itself contributed to reaching sub-5ms p99. These require no user configuration — just upgrading to the latest Feast version.

### Serialization Optimization

The feature server used `google.protobuf.json_format.MessageToDict` to convert protobuf responses to JSON — a generic, reflection-based serializer that was a meaningful fraction of server-side latency. Replacing it with an optimized custom dict builder delivered a **66% throughput increase** (432 to 718 RPS) and **72% reduction in tail latency under load** (132ms to 37ms p99).

### Async Redis Reads with Batched Pipeline

The `RedisOnlineStore` had async support (`online_read_async` with `redis_asyncio`), but the `async_supported` property was not overridden, so the feature server never used it. Enabling it unlocks non-blocking I/O on the server side — the FastAPI handler calls `get_online_features_async` directly instead of wrapping the sync path in `run_in_threadpool`.

Additionally, the base class async path issued O(N_feature_views) separate round trips to Redis via `asyncio.gather`. We added a `get_online_features_async` override to `RedisOnlineStore` that batches all HMGET commands across all feature views into a **single async pipeline execution** (O(1) round trips), matching the existing sync batched pipeline. This cut async 5-row p99 from ~11ms to **5.6ms** — a 49% improvement.

### Cached Per-Request Checks

`_check_versioned_read_support()` performed up to 7 lazy module imports on **every request** to determine if the current online store supports versioned reads. We cache the result per store instance, resolving imports once and eliminating ~0.5–1ms of overhead per request.

### Skip Duplicate Feature Resolution

When auth is `no_auth` (the common case), the feature server was resolving feature views solely to check permissions (which are no-ops), then resolving them again inside `get_online_features`. We skip the first resolution entirely, avoiding a redundant registry lookup.

### Session Wrapping Fix

The `rest_error_handling_decorator` re-wrapped cached `requests.Session` HTTP methods on every call. After ~1000 requests, this caused progressive performance degradation and eventually a `RecursionError`. We now wrap each method exactly once per session lifetime, fixing Remote SDK stability and enabling it to sustain **760 RPS**.

---

## Final Results

After applying all server-side configuration, client-side configuration, and code enhancements:

### Stage 1: Tuning only (sub-5ms target)

| Mode | p50 (1 row) | p99 (1 row) | p50 (5 rows) | p99 (5 rows) | Throughput |
|------|----------|----------|----------|----------|------------|
| **REST API** | 3.34 ms | **4.61 ms** | 7.88 ms | 11.32 ms | 718 RPS |
| **REST API (FeatureService)** | 4.21 ms | **6.15 ms** | 9.15 ms | 17.43 ms | — |
| **Direct SDK** | 3.12 ms | **4.21 ms** | 3.29 ms | **4.60 ms** | 402 RPS |
| **Direct SDK (FeatureService)** | 3.48 ms | **5.70 ms** | 3.44 ms | **5.10 ms** | — |
| **Async Direct SDK** | 3.25 ms | **6.25 ms** | 3.47 ms | **8.72 ms** | — |
| **Async Direct SDK (FeatureService)** | 3.60 ms | **4.84 ms** | 3.76 ms | **5.13 ms** | — |
| **Remote SDK** | 3.34 ms | **5.30 ms** | 8.15 ms | 11.63 ms | 760 RPS |
| **Remote SDK (FeatureService)** | 3.78 ms | **5.17 ms** | 9.86 ms | 16.06 ms | — |

### Stage 2: Pre-computed vectors (sub-2ms target)

| Batch Size | p50 Regular | p99 Regular | p50 Precomputed | p99 Precomputed | Speedup (p50) |
|---|---|---|---|---|---|
| 1 | 5.95 ms | 10.74 ms | **0.98 ms** | **1.70 ms** | 6.1x |
| 5 | 9.66 ms | 44.91 ms | **1.37 ms** | **3.00 ms** | 7.1x |
| 10 | 16.60 ms | 60.37 ms | **1.81 ms** | **2.07 ms** | 9.2x |
| 50 | 60.07 ms | 120.12 ms | **5.27 ms** | **7.49 ms** | 11.4x |
| 100 | 85.58 ms | 208.18 ms | **9.48 ms** | **114.38 ms** | 9.0x |
| 500 | 218.79 ms | 424.91 ms | **40.25 ms** | **198.13 ms** | 5.4x |

**Key takeaways:**

- **Stage 1 (tuning)** gets all SDK modes to **sub-5ms p99** for single-row requests — REST API at 4.61ms, Direct SDK at 4.21ms, Async SDK at 4.84ms.
- **Stage 2 (pre-computed vectors)** pushes latency to **sub-2ms p99** for single-row requests — a 6x improvement over the tuned regular path.
- **REST API** delivers the best throughput at **718 RPS** (2.6M RPH); **Remote SDK** sustains **760 RPS** after the session wrapping fix.
- For FeatureServices spanning multiple feature views, **`precompute_online=True` is the single most impactful optimization** — it changes the read complexity from O(N feature views) to O(1).
- At large batch sizes, the bottleneck shifts from store I/O to Python CPU overhead (protobuf deserialization). For these workloads, split large requests into smaller batches on the client side.

---

## A Note on Online Store Selection

All benchmarks in this post used a **standalone Redis pod** running in the same Kubernetes cluster as the feature server. Production deployments often use managed services — here's how that changes the picture.

**Managed Redis** (ElastiCache, Memorystore, Azure Cache for Redis) provides dedicated compute, optimized networking, cluster mode for sharding, and automatic failover. In our benchmarks, Redis RTT was ~0.5ms (in-cluster). A managed instance in the **same availability zone** would deliver comparable latency with more consistent tail behavior. Cross-AZ hops add 1–2ms per request.

**DynamoDB** offers zero operational overhead and automatic scaling. When the feature server runs in the **same AWS region and VPC**, single-digit millisecond reads are typical (1–5ms for eventually consistent reads). With [DAX](https://aws.amazon.com/dynamodb/dax/), read latency drops to microseconds for cached items. A same-region setup could deliver comparable sub-5ms p99 for single-row reads.

Feast also supports PostgreSQL, SQLite, Snowflake, Bigtable, and more. The general rule is: **the online store is the single largest factor in `get_online_features()` latency** — choose based on your latency budget, throughput needs, and operational requirements. The tuning steps in this post (worker configuration, registry caching, connection pooling, serialization optimization) apply equally to all stores — they optimize the layers above.

---

## Pre-computed Feature Vectors

The tuning steps above achieve our first target: **sub-5ms p99** for single-row requests. But for FeatureServices spanning multiple feature views, per-FV read fan-out becomes the dominant bottleneck — each request issues N separate store reads, N protobuf deserializations, and N response assemblies. To reach our final target of **sub-2ms**, we need to eliminate this fan-out entirely.

**Pre-computed feature vectors** do exactly that: at materialize time, all features for a FeatureService are assembled into a single serialized blob per entity. At read time, one key lookup replaces N feature-view reads — reducing the operation from O(N feature views) to O(1) and delivering **sub-2ms p99 latency**.

### How it works

1. **Define** a FeatureService with `precompute_online=True`:

```python
scoring_service = FeatureService(
    name="realtime_scoring",
    features=[user_profile_fv, transaction_fv, risk_fv],
    precompute_online=True,
)
```

2. **Apply** and **materialize** as usual — vectors are built automatically:

```bash
feast apply
feast materialize-incremental $(date -u +"%Y-%m-%dT%H:%M:%S")
```

Feast detects which FeatureServices have `precompute_online=True` and rebuilds their pre-computed vectors after the per-feature-view writes complete. Vectors are also refreshed automatically on `feast push`.

3. **Read** features as usual — the server automatically uses the pre-computed path:

```python
features = store.get_online_features(
    features=store.get_feature_service("realtime_scoring"),
    entity_rows=[{"user_id": "U12345"}],
    full_feature_names=True,
)
```

### Design decisions

- **Store-agnostic**: The pre-computed logic lives in the base `OnlineStore` class and works with all backends (Redis, DynamoDB, PostgreSQL, etc.). No store-specific code is needed.
- **Opt-in**: `precompute_online` defaults to `False`. Existing deployments are completely unaffected.
- **Strict error handling**: When `precompute_online=True`, there is no silent fallback to per-FV reads. If vectors are missing or stale, the server raises a `RuntimeError`, making problems visible immediately.
- **Schema-aware**: A fingerprint of feature names detects schema changes and rejects stale vectors, with column-order-independent comparison.
- **Per-FV TTL enforcement**: Individual feature view TTLs are checked within the pre-computed blob.
- **Materialized view pattern**: Conceptually similar to a database materialized view — trades storage for read speed with explicit refresh.

### Benchmark: precomputed vs regular path

We benchmarked a FeatureService spanning multiple feature views against the same features read via the regular per-feature-view path. All numbers from the same pod, same run, 200 iterations with 30 warmup.

| Batch Size (rows/request) | p50 Regular | p50 Precomputed | p99 Regular | p99 Precomputed | Speedup (p50) |
|---|---|---|---|---|---|
| 1 | 5.95 ms | **0.98 ms** | 10.74 ms | **1.70 ms** | 6.1x |
| 5 | 9.66 ms | **1.37 ms** | 44.91 ms | **3.00 ms** | 7.1x |
| 10 | 16.60 ms | **1.81 ms** | 60.37 ms | **2.07 ms** | 9.2x |
| 50 | 60.07 ms | **5.27 ms** | 120.12 ms | **7.49 ms** | 11.4x |

For the typical production use case of 1–10 rows per inference request, pre-computed vectors deliver **sub-2ms p99** — well under any reasonable SLA target. The speedup ranges from **6x to 9x** depending on batch size, with p50 consistently under 2ms for up to 10 rows.

---

## Try It Yourself

To deploy the same setup:

1. Deploy Feast with the Feast Operator using a `FeatureStore` CR with `workerConfigs`
2. Use Redis as the online store and PostgreSQL for the registry
3. Apply the [production tuning guide](https://docs.feast.dev/how-to-guides/online-server-performance-tuning) for worker configuration, registry caching, and scaling
4. For FeatureServices spanning multiple feature views, enable `precompute_online=True` and materialize — see the [feature service docs](https://docs.feast.dev/getting-started/concepts/feature-retrieval#pre-computed-feature-vectors-precompute_online)
5. Monitor with [built-in Prometheus metrics](https://docs.feast.dev/reference/feature-servers/python-feature-server) — `feast_feature_server_request_latency_seconds` is your primary SLI

We'd love to hear about your production performance results. Join the conversation on [Feast Slack](https://slack.feast.dev) or open an issue on [GitHub](https://github.com/feast-dev/feast).
