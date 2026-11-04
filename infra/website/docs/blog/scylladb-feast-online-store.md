---
title: "Feast Feature and Vector Serving at ScyllaDB Speed"
description: Starting with Feast v0.65, a new tighter integration between Feast and ScyllaDB brings vector search support and better performance to the online store. This post covers what changed, why teams outgrow their existing online store, why ScyllaDB fits feature store workloads, and how to configure it in Feast.
date: 2026-09-22
authors: ["Attila Toth"]
---

<div class="hero-image">
<img class="scylladb-hero-light" src="/images/blog/scylladb-feast-architecture-hero.png" alt="Feast Feature and Vector Serving at ScyllaDB Speed" loading="lazy" />
<img class="scylladb-hero-dark" src="/images/blog/scylladb-feast-architecture-hero-dark.png" alt="Feast Feature and Vector Serving at ScyllaDB Speed" loading="lazy" />
</div>

<style>
.scylladb-hero-dark { display: none; }
@media (prefers-color-scheme: dark) {
  :root:not([data-theme="light"]) .scylladb-hero-light { display: none; }
  :root:not([data-theme="light"]) .scylladb-hero-dark { display: block; }
}
:root[data-theme="dark"] .scylladb-hero-light { display: none; }
:root[data-theme="dark"] .scylladb-hero-dark { display: block; }
</style>

Starting Feast [v0.65](https://github.com/feast-dev/feast/releases#release-v0.65.0), you can build with a new and tighter integration between Feast and [ScyllaDB](https://www.scylladb.com/) online store, including vector search support and better performance. This post covers what changed, what tends to push teams to outgrow their online store, why ScyllaDB fits feature store workloads, and how to configure it in Feast.

## What changed

Before Feast v0.65, you could use ScyllaDB as an online store, but only through the existing Cassandra connector. That came with two downsides. First, performance wasn't optimized: the Cassandra driver isn't [shard-aware](https://www.scylladb.com/product/scylla-drivers/), and shard-awareness is a key part of what makes ScyllaDB faster. Second, you couldn't store vector embeddings, since the Cassandra integration doesn't support Feast's newer vector-related functions. To solve these problems, we built a first-class, optimized, feature-rich integration instead:

* Uses ScyllaDB shard-aware [Python driver](https://pypi.org/project/scylla-driver/)
* Works with both self-hosted and ScyllaDB Cloud clusters
* Implements Feast vector database API
* Supports retrieval, indexing, V2 API support (retrieval of features along with vector embeddings), online read

This new integration gets you both the performance and vector embedding support ScyllaDB is meant to deliver. Now, let's discuss why teams decide to choose ScyllaDB as their online store choice.

## Why teams outgrow their online store

Teams building a large-scale feature store today are usually choosing between ScyllaDB and comparable databases like DynamoDB, Redis, MongoDB, or Cassandra. Each of those is a capable database. But a few problems tend to surface once a specific feature store access pattern – frequent small reads and writes on the hot path of a live prediction – hits real scale.

### Cost that stops scaling linearly

In the field, we've found that once most teams hit a certain scale (e.g., 200K+ ops/sec while holding a single-digit ms P99 latency SLA), the costs for online inference often stop scaling economically. This can be the case for [DynamoDB pricing](https://www.scylladb.com/2026/04/15/the-hidden-insanity-of-dynamodb-pricing/), where costs can climb in ways that don't track linearly with usage. At this point, they often start swapping out the existing store to ScyllaDB. Now that AI-assisted tooling handles so much of the migration grunt work, moving off a legacy database isn't the multi-quarter slog it used to be.

### Maintaining a cache as a second data layer

When a cache (e.g., Redis) sits in front of the database, someone on the team will (inevitably, eventually) ask why they're running two systems instead of one. After all, the cache adds invalidation logic, extra failover paths, and [operational overhead](https://www.scylladb.com/2024/05/29/eliminating-external-database-caches/). We've seen a lot of these teams migrate once they realize a single database can hit the same latency and availability targets for online inference, without the cache layer or the operational overhead of keeping two stores agreeing with each other.

### Tail latency under load

Another reason teams decide to switch their online store provider: latency becomes unpredictable. JVM-based stacks (e.g., Cassandra) can hit [garbage collection pauses](https://www.scylladb.com/glossary/cassandra-garbage-collection/) that spike P99 without warning. The app layer inherits that volatility, no matter how well the database underneath is tuned. Once teams move to a non-JVM database, that variance disappears.

### Multi-region distribution

If you have users in multiple regions, latency becomes a geography problem as well. A single-region database means every request outside that region pays a round-trip tax, and that tax shows up directly in model-serving latency. Teams end up needing writes and reads to occur close to the user, which pushes multi-region, [globally distributed](https://www.scylladb.com/2024/01/22/worldwide-local-latency/) architecture to be a baseline requirement.

ScyllaDB has helped lots of teams get these performance and scaling characteristics without special tuning.

## Why ScyllaDB fits feature store workloads

An online feature store serves a live model at inference time. When a recommendation engine, a fraud model, or a ranking system needs to predict an outcome right now, it first queries the online store for the latest feature values. Then, the model waits on that response before it can return an answer to the user. That puts three strict requirements on the database.

### Low latency

Feature retrieval is often triggered by a real-time user action. It must be fast enough that it doesn't add meaningful delay to the response. ScyllaDB targets ultra-low P99 latency, and production deployments back this up:

* [ShareChat's](https://www.scylladb.com/2024/08/27/how-sharechat-scaled-their-ml-feature-store/) (large social media platform in India) feature store serves a billion feature lookups per second at peak with P99 latency under 20ms
* Agoda's (online travel booking platform) feature store sustains a [10ms](https://www.scylladb.com/2026/03/03/agoda-scaled-its-feature-store-50x/) P99 SLA while serving 200K entities per second.

### High throughput

Feature stores built on ScyllaDB can handle 1 million read operations [per second](https://thenewstack.io/medium-scylladb-feature-store/) while keeping P99 latencies in single-digit milliseconds and P50 latencies around 1ms. A comparable database under the same workload saw P99s climb to 70-220ms.

<div style="text-align: center; margin: 20px 0;">
  <img src="/images/blog/scylladb-feast-medium-benchmark.png" alt="Medium's ScyllaDB-powered feature store benchmark — purple line is ScyllaDB, blue line is DynamoDB performance, lower means faster" loading="lazy" style="max-width: 100%; border: 1px solid #e0e0e0; border-radius: 8px;">
  <p><em><a href="https://medium.engineering/scylladb-implementation-lists-in-mediums-feature-store-part-2-905299c89392?gi=2b0a0689ccf2">Medium's</a> ScyllaDB-powered feature store. The purple line is ScyllaDB, blue line is DynamoDB performance - lower means faster.</em></p>
</div>

Numbers like these are becoming common for teams running real-time recommendation systems. Here, a few extra milliseconds at the tail can mean that results are stale by the time a user sees them.

### High availability and global distribution

A feature store that many teams and models depend on becomes shared infrastructure: if it goes down, every model reading from it goes down too. ScyllaDB uses a symmetric, peer-to-peer [architecture](https://www.scylladb.com/product/technology/) that does not rely on leaders, followers, or external components for replication. Every node is identical within a zone, region, or globally.

Topology and schema changes go through [Raft](https://raft.github.io/) to keep a strongly consistent, replicated record of cluster state across all nodes without slowing things down.

<div style="text-align: center; margin: 20px 0;">
  <a href="https://tzach.github.io/scylladb-ha-demo/">
    <img src="/images/blog/scylladb-feast-ha-topology-demo.gif" alt="ScyllaDB high-availability, peer-to-peer topology demo" loading="lazy" style="max-width: 100%; border: 1px solid #e0e0e0; border-radius: 8px;">
  </a>
  <p><em>ScyllaDB's peer-to-peer topology in action - <a href="https://tzach.github.io/scylladb-ha-demo/">try the interactive demo</a>.</em></p>
</div>

Data replicates automatically based on a replication factor you set. Even RF=3, which tolerates losing two of three copies, covers most high-availability needs. Consistency is also tunable per query – anywhere from a single acknowledging replica to all of them.

ScyllaDB is rack- and datacenter-aware too, so you can spread data across racks, availability zones, and regions with per-datacenter replication factors. A rack or an entire region can go down and feature freshness doesn't depend on it.

### Vector Search

ScyllaDB is one of the [few databases](https://docs.feast.dev/reference/alpha-vector-database) that support Feast vector search. This allows you to treat embeddings just like any other type of feature in your machine learning architecture and query it directly from Feast. This way you reuse the same standardized API layer to query all of your machine learning data. Furthermore, you also just need to maintain one database, ScyllaDB, instead of multiple specialized databases. ScyllaDB vector search runs natively in the same wide-column engine serving the rest of your features. It uses an integrated HNSW implementation so nearest-neighbor lookups share the same low-latency access as regular key-value retrieval. For RAG and recommendation use cases – where one inference call often needs both structured features and embedding similarity – that design holds single-digit-millisecond latencies even at [billions of vectors](https://www.scylladb.com/2025/12/01/scylladb-vector-search-1b-benchmark/).

## Get started with ScyllaDB and Feast

### Install

Install Feast with the ScyllaDB extra, which pulls in the ScyllaDB driver automatically:

```bash
pip install feast[scylladb]
```

### Configure your `feature_store.yaml`

Point the online store at your cluster:

```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
    type: scylladb
    hosts:
        - node-0.aws_us_east_1.xxxxxxxx.clusters.scylla.cloud
        - node-1.aws_us_east_1.xxxxxxxx.clusters.scylla.cloud
        - node-2.aws_us_east_1.xxxxxxxx.clusters.scylla.cloud
    keyspace: feast
    username: scylla
    password: xxxxxx
    local_dc: AWS_US_EAST_1
```

### Define a feature view with vector index

Standard feature views work as-is. If the feature store is also backing retrieval for recommendations or RAG, tag an embedding field with `vector_index` and a dimension count. Feast will then manage the index for you:

```python
Field(
    name="embedding",
    dtype=Array(Float32),
    tags={
        "vector_index": "true",
        "dimensions": "768",
        "similarity_function": "COSINE",
    },
)
```

### Apply and serve

```bash
feast apply
```

`feast apply` provisions the underlying tables and any vector indexes directly from your feature definitions (without a separate schema migration step). From there, standard `get_online_features` calls read from ScyllaDB like any other online store, and nearest-neighbor lookups are a single call:

```python
result = store.retrieve_online_documents_v2(
    features=["documents:text", "documents:embedding"],
    query=[0.1, 0.2, ...],
    top_k=10,
    distance_metric="COSINE",
)
```

The full configuration reference is in the [Feast ScyllaDB online store docs](https://docs.feast.dev/reference/online-stores/scylladb).

## Start building

* [ScyllaDB + Feast get started docs](https://docs.scylladb.com/stable/get-started/build-with-ai/integrations/feast.html)
* [ScyllaDB Cloud free trial](https://cloud.scylladb.com/)
* [ScyllaDB Feature Store examples on GitHub](https://github.com/scylladb/scylladb-feature-store)
