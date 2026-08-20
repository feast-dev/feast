---
title: "Aerospike now available as a Feast online store"
description: "Feast now supports Aerospike as an online store, so teams can serve features with low, predictable latency while keeping feature data on SSD instead of RAM."
date: 2026-08-20
authors: ["Valentyn Kahamlyk", "Francisco Javier Arceo"]
---

Feast, a popular open-source [feature store](https://aerospike.com/blog/feature-store), now supports Aerospike as an online store, giving ML teams a way to serve features with low, predictable latency as their feature sets and workloads grow.

With Aerospike as the serving backend, Feast users can scale beyond the practical limits and cost of an entirely in-memory store while maintaining the performance required for online inference.

Ready to try it? Jump to [getting started with Aerospike in Feast](#get-started-with-feast-and-aerospike).

## Aerospike for online feature serving

Customers like [Myntra](https://aerospike.com/resources/customer-stories/myntra/), [Sony](https://aerospike.com/resources/customer-stories/sony/), [MGID](https://aerospike.com/resources/customer-stories/mgid-aerospike-customer-story/), and [PhonePe](https://aerospike.com/resources/customer-stories/phonepe/) already use Aerospike to serve features for applications, such as [fraud detection](https://aerospike.com/solutions/use-cases/fraud-prevention/), personalization, and [real-time bidding](https://aerospike.com/solutions/industry/adtech/), where decisions need to be made in milliseconds as conditions change. The Feast integration lets customers connect feature views to Aerospike as the online store while continuing to manage definitions, the registry, and feature services in Feast. For existing Aerospike customers, this provides a straightforward way to connect Feast to a database they already use. For new deployments, Aerospike can serve as the online store from the beginning.

## Performance without an all-in-memory architecture

When selecting an online store for Feast, teams often start with an in-memory database. That works well when the feature set is small enough to fit comfortably in RAM. As the number of entities and features grows, however, keeping the entire dataset in memory can become increasingly expensive.

Aerospike is built to provide in-memory performance with the scale benefits of SSD. With [Hybrid Memory Architecture (HMA)](https://aerospike.com/blog/hybrid-memory-architecture-optimization/), indexes remain in memory while feature data resides on SSD, so the database does not need to keep the entire dataset in RAM. The incremental latency of an SSD read is measured in microseconds, while the network round trip for an online feature request typically takes low milliseconds, so retrieving feature data from SSD adds little to the total response time. This gives teams a path to larger feature sets without keeping every feature value in RAM. The same design also holds up as concurrency rises, because keeping everything in RAM does not by itself guarantee that a store will remain responsive.

## Benchmarking in Feast

We ran Feast's `feast-benchmarks` harness end to end, from the load generator through `feast serve` to the online store, varying the number of entities per request, the number of features per request, and the request rate. Three results stood out:

1. **On smaller requests, Aerospike matched the in-memory store**, serving single-entity and modest-batch requests in the same low-millisecond range.  
2. **As load increased, Aerospike continued serving successfully** on demanding request shapes where the in-memory store began to degrade.  
3. **Aerospike delivered that performance while storing feature data on SSD**, rather than requiring the entire feature set to remain in RAM.

Full methodology and per-workload results are in [Fast like a cache, priced like storage: Benchmarking Aerospike on Feast](/blog/aerospike-feast-benchmark-harness-results).

## Out of the box: Configuring Aerospike with Feast

Feast supports a range of online stores through a common interface, so using Aerospike is a configuration choice rather than a code change. Aerospike works out of the box with HMA: indexes remain in memory while feature data resides on SSD. No additional Feast configuration is required to get that behavior.

A basic configuration consists of the store type and a namespace:

```yaml
online_store:
  type: aerospike
  namespace: feast_ssd
```

HMA is the default and is the right choice for most feature views. If a small number of feature views are especially latency-sensitive, you can route them to a memory-backed namespace:

```yaml
online_store:
  type: aerospike
  namespace: feast_ssd
  namespace_overrides:
    scoring_velocity: feast_ram
```

This lets you use RAM selectively rather than making the entire feature store an in-memory deployment:

* **HMA namespace:** index in RAM, feature data on SSD  
* **Memory namespace:** index and feature data in RAM

### A final performance tip

To keep read times low, group feature views that are commonly read together on the same namespace. If a request pulls feature views from different namespaces, it requires a separate read from each. For wide feature services, you can also use `precompute_online=True` to combine features into a single lookup.

See the [Online server performance tuning guide](https://github.com/feast-dev/feast/blob/master/docs/how-to-guides/online-server-performance-tuning.md) and [Aerospike online store reference](https://github.com/feast-dev/feast/blob/master/docs/reference/online-stores/aerospike.md) for the configuration details, including TTL, timeouts, set overrides, and prewriting hooks.

## Get started with Feast and Aerospike

The fastest way to start running the integration is directly through [Feast here](https://github.com/feast-dev/feast/blob/master/docs/reference/online-stores/aerospike.md). Pin your Feast version and test failover, TTL, and namespace placement in a staging cluster before moving to production.

There are also several tutorials on using Aerospike as an online store:

* [Aerospike Feature store tutorial](https://aerospike.com/docs/develop/tutorials/applications/feature-store/)  
* [Serve real-time Feast features with Aerospike](https://aerospike.com/docs/develop/feast-aerospike-online-store)  
* [Route hot and cold features across RAM and SSD](https://aerospike.com/docs/develop/feast-aerospike-tiering)  
* [Stream fraud velocity features with Feast](https://aerospike.com/docs/develop/feast-aerospike-fraud-velocity)

With the integration now available through Feast, teams can use Aerospike as the serving layer as their feature workloads grow.