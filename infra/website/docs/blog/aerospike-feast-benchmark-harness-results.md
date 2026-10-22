---
title: "Fast like a cache, priced like storage: Benchmarking Aerospike on Feast"
description: "We ran Feast's benchmark harness against Aerospike and compared it with the published Redis and DynamoDB results, sweeping entities per request, features per request, and request rate."
date: 2026-08-20
authors: ["Valentyn Kahamlyk", "Francisco Javier Arceo"]
---

<div class="hero-image">
  <img src="/images/blog/aerospike-benchmark-p99-latency-entity-growth.png" alt="P99 latency with entity growth: Aerospike, Redis, and DynamoDB" loading="lazy">
</div>

We recently [launched an integration between Aerospike and Feast](/blog/aerospike-feast-now-available), giving teams a straightforward way to use Aerospike as the online store behind Feast's real-time feature serving. As part of that work, we wanted to understand how Aerospike performs under the workloads that matter for Feast users.

We benchmarked Aerospike as a Feast online store using [Feast’s benchmark harness](https://github.com/feast-dev/feast-benchmarks) and compared the results with the Redis and DynamoDB results published in the same repository. The tests varied the number of entities per request, the number of features per request, and the request rate.

## Benchmark methodology

We used Feast's benchmark harness end-to-end, from the load generator through the Python feature server to the online store.

We tested three dimensions of the workload:

1. **Entities per request.** A single-entity request represents a scoring call for one user or transaction. A request containing many entities represents batch scoring or a model evaluating many candidates at once.  
2. **Features per request.** More features represent a richer model and a larger amount of data that must be retrieved for each prediction.  
3. **Request rate.** Increasing the request rate tests how each online store behaves as concurrency and load increase.

Sweeping these axes separately allows us to observe how a store behaves under specific workload shapes rather than relying on a single latency number.

### A note on methodology and reproducibility

Teams can use the publicly available [harness](https://github.com/feast-dev/feast-benchmarks) to run the tests on their own hardware and workloads. The repository publishes results for several online stores. We reviewed all of them and compared Aerospike against [Redis](https://aerospike.com/compare/redis-vs-aerospike/) and [DynamoDB](https://aerospike.com/compare/dynamodb-vs-aerospike/), setting Datastore aside because its data is several years old and the service has since been rebuilt as Firestore in Datastore mode.

The Aerospike tests used a c2-standard-16 GCP VM. Redis and Aerospike were co-located with the feature server, while DynamoDB was accessed as a managed service over a same-region network connection. We reproduced the community harness as published rather than running a controlled benchmark of our own, so running the load generator, feature server, and store together on one VM reflects that harness convention rather than a production topology.

Because competitor results were collected on different hardware and at different times, we focus on workload behavior rather than precise latency multiples. These results should be read as workload comparisons rather than universal performance claims.

## Measuring latency across request sizes

The first set of tests increases the number of entities requested at once.

A single-entity request is the common case, a fraud check or a personalization call scoring one user. Larger requests show up in recommendation and bidding systems that score many candidates at once to pick among them, so a single prediction can require tens or hundreds of entities in one read.

Aerospike tracks the in-memory store closely on these ordinary reads. On single-entity and modest-batch requests, Aerospike and Redis sit together in the low tens of milliseconds, with the lead moving between runs within normal measurement variance.

DynamoDB is several times higher in these tests.

![P99 latency with entity growth: Aerospike, Redis, and DynamoDB](/images/blog/aerospike-benchmark-p99-latency-entity-growth.png)

**Chart 1: Aerospike P99 latency stays relatively low as more entities are added**   
*Note: Features \= 50, RPS \= 10 for all runs*

This matters because Aerospike is not keeping the entire dataset in RAM. Its [Hybrid Memory Architecture (HMA)](https://aerospike.com/blog/hybrid-memory-architecture-optimization/) keeps primary indexes in memory while storing feature data on SSD.

A read that an in-memory store handles quickly, Aerospike can handle in a similar latency range without requiring an all-in-memory deployment.

## Increasing the number of features

The second sweep increases the number of features retrieved for each entity.

As the feature count increases, each request contains more data and places more work on the online store. Feature counts climb as teams add signals to a model: a mature fraud model may pull a wide set of velocity, history, and device features about a single entity, which is why the widest requests are realistic and not a stress-test artifact.

![P99 latency with feature growth: Aerospike, Redis, and DynamoDB](/images/blog/aerospike-benchmark-p99-latency-feature-growth.png)

**Chart 2: Aerospike P99 latency remains low as more features are added**  
*Note: Entities \= 1, RPS \= 10 for all runs*

As the request gets wider, Aerospike stays in the same low range as the in-memory store, and both stay well below DynamoDB. A production feature store needs to maintain predictable serving behavior as feature sets become larger.

## Holding performance under load

The third sweep increases the request rate, which is driven by traffic, not by the model. An ad-bidding or fraud-scoring service, at peak, fields thousands of decisions per second, and each decision can be one of these multi-entity reads, so entity-batch size and request rate rise together rather than independently.

The harness includes running 100 entities and 50 features per request at increasing requests per second (RPS).

* Aerospike maintains 100 percent success across the full range tested.  
* Redis holds to about 60 requests per second before success rates begin to fall. At 80 requests per second, only about a quarter of requests succeed, and at 90 requests per second success approaches zero.  
* DynamoDB is degraded from the first step, with about 72 percent success even at 10 requests per second. In this case, the limiting behavior is throttling rather than latency.

The failures under load are different. Throttling and timeouts represent the behavior of the stores under the tested workloads and do not depend on the same topology effect.

![Success rate with growing requests per second: Aerospike, Redis, and DynamoDB](/images/blog/aerospike-benchmark-success-rate-rps.png)

**Chart 3: Aerospike successfully completes all runs up to the max 100 RPS**  
*Note: Entities \= 100, Features \= 50 for all runs*

Aerospike keeps serving successfully as both request size and request rate increase, while the other stores stop completing requests.

## Behavior at the highest scale

We also tested a request shape of 100 entities by 250 features. At this workload, Aerospike is the only store that completes any requests at all, and only at the lowest request rates.

That result marks the practical ceiling of the complete serving stack under this test configuration rather than the maximum capability of any individual database. The feature server, network, client, and load generator all contribute to the result.

![Success rate with growing requests per second at 100 entities by 250 features](/images/blog/aerospike-benchmark-success-rate-rps-wide-requests.png)

**Chart 4: Redis and DynamoDB are unable to complete these runs. Aerospike succeeds up to 30 RPS**  
*Note: Entities \= 100, Features \= 250 for all runs*

This test is useful for a different reason than the smaller requests. It shows what happens when the amount of data required for each request becomes large enough that the serving stack itself becomes the limiting factor.

## What the results mean

Taken together, the tests show a consistent pattern. Aerospike delivers near in-memory latency on smaller feature requests while maintaining successful serving as request sizes and rates increase.

That combination matters for feature stores because the workload can change in both dimensions over time. A model may start with a relatively small feature set and later add more features. An application may begin with modest traffic and eventually serve millions of predictions.

Aerospike’s HMA addresses these requirements: indexes remain in memory for fast lookups, while feature data can reside on SSD. The performance results address one side of the tradeoff. The other is infrastructure cost: how much memory is required to deliver that performance as the feature set grows?

## The cost of keeping everything in RAM

Performance is only part of the equation. The infrastructure required to keep an online feature store entirely in memory can become a significant cost as the dataset grows.

Consider a deployment with:

* 50 million entities  
* 100 online features per entity  
* An average value size of 8 bytes

That produces about 40 GB of raw feature data.

Encoding overhead and replicas for high availability increase the provisioned footprint to roughly 120 GB.

An all-in-memory Redis deployment would need to hold that entire footprint in RAM. With Aerospike HMA, the feature data can reside on SSD while only the index needs to remain in memory. For this example, the index requires only a few gigabytes of RAM.

The advantage comes from how much memory each design needs:

| Model | In RAM | On SSD |
| :---- | :---- | :---- |
| **Redis** (RAM) | \~120 GB | None |
| **Aerospike**  (index RAM, data SSD) |    \~6 GB | 120 GB |

Redis holds the entire footprint in RAM. Aerospike keeps only the index there and puts the feature data on SSD. Because RAM costs more than fifty times as much per gigabyte as SSD, moving the bulk of the footprint off memory cuts the RAM you have to provision by roughly 20 times, and the total infrastructure cost by an order of magnitude.

![Infrastructure cost with increasing scale: Aerospike versus Redis](/images/blog/aerospike-benchmark-infrastructure-cost.png)

**Chart 5: Costs dramatically rise with entity count for an all-RAM architecture**

The gap widens as the dataset grows, which is what the chart above shows. These figures are illustrative rather than a cloud-provider quote, so use your own entity counts, prices, and latency targets for planning.

The difference grows as entity counts reach hundreds of millions or feature vectors become wider. It narrows when the entire working set is small enough to fit comfortably in RAM or when the workload requires every feature to have the lowest possible latency.

Lower infrastructure cost only helps if it still meets the latency target. The benchmark results show that SSD-based feature storage does in fact deliver near in-memory performance for these Feast workloads.

## Try it yourself

For teams using Feast, Aerospike provides an alternative that combines the performance of an in-memory architecture with the capacity and economics of SSD-based storage. For controlled, head-to-head benchmarks run under our own methodology, see Aerospike's competitive research, including a recent [Redis comparison](https://aerospike.com/resources/benchmarks/aerospike-vs-redis-benchmark-report/) and [DynamoDB results](https://aerospike.com/resources/benchmarks/aerospike-dynamodb-benchmark/).

For production planning, use your own entity counts, feature widths, request rates, and latency targets. The right online store depends on the shape of your specific workload as well as the total size of your dataset.

Get started with the Aerospike online store for [Feast here](https://github.com/feast-dev/feast/blob/master/docs/reference/online-stores/aerospike.md).
