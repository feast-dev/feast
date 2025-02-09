# Serving features in milliseconds with Feast feature store

*February 1, 2022* | *Tsotne Tabidze, Oleksii Moskalenko, Danny Chiao*

Feature stores are operational ML systems that serve data to models in production. The speed at which a feature store can serve features can have an impact on the performance of a model and user experience. In this blog post, we show how fast Feast is at serving features in production and describe considerations for deploying Feast.

## Updates
Apr 19: Updated DynamoDB benchmarks for Feast 0.20 given batch retrieval improvements

## Background

One of the most common questions Feast users ask in our [community Slack](http://slack.feastsite.wpenginepowered.com/) is: how scalable / performant is Feast? (spoiler alert: Feast is *very* fast, serving features at <1.5ms @p99 when using Redis in the below benchmarks)

In a survey conducted last year ([results](https://docs.google.com/forms/d/e/1FAIpQLScV2RX)), we saw that most users were tackling challenging problems like recommender systems (e.g. recommending items to buy) and fraud detection, and had strict latency requirements.

Over 80% of survey respondents needed features to be read at less than 100ms (@p99). Taking into account that most users in this survey were supporting recommender systems, which often require ranking 100s-1000s of entities simultaneously, this becomes even more strict. Feature serving latency scales with batch size because of the need to query features for random entities and other sources of tail latency.

In this blog, we present results from a benchmark suite ([RFC](https://docs.google.com/document/d/12UuvTQnTTCJ)), describe the benchmark setup, and provide recommendations for how to deploy Feast to meet different operational goals.

## Considerations when deploying Feast

There are a couple of decisions users need to make when deploying Feast to support online inference. There are two key decisions when it comes to performance:

1. How to deploy a feature server
2. Choice of online store

Each approach comes with different tradeoffs in terms of performance, scalability, flexibility, and ease of use. This post aims to help users decide between these approaches and enable users to easily set up their own benchmarks to see if Feast meets their own latency requirements.

### How to deploy a feature server

While all users setup a Feast feature repo in the same way (using the Python SDK to define and materialize features), users retrieve features from Feast in a few different ways (see also [Running Feast in Production](https://docs.feastsite.wpenginepowered.com/how-to-guides/running-feast-in-production)):

1. Deploy a Java gRPC feature server (Beta)
2. Deploy a Python HTTP feature server
3. Deploy a serverless Python HTTP feature server on AWS Lambda
4. Use the Python client SDK to directly fetch features
5. (Advanced) Build a custom client (e.g in Go or Java) to directly read the registry and read from an online store

The first four above come for free with Feast, while the fifth requires custom work. All options communicate with the same Feast registry component (managed by feast apply) to understand where features are stored.

Deploying a feature server service (compared to using a Feast client that directly communicates with online stores) can enable many improvements such as better caching (e.g. across clients), improved data access management, rate limiting, centralized monitoring, supporting client libraries across multiple languages, etc. However, this comes at the cost of increased architectural complexity. Serverless architectures are on the other end of the spectrum, enabling simple deployments at the cost of latency overhead.

### Choice of online stores

Feast is highly pluggable and extensible and supports serving features from a range of online stores (e.g. Amazon DynamoDB, Google Cloud Datastore, Redis, PostgreSQL). Many users build their own plugins to support their specific needs / online stores. [Building a Feature Store](https://www.tecton.ai/blog/how-to-build-a-feature-store/) dives into some of the trade-offs between online stores. Easier to manage solutions like DynamoDB or Datastore often lose against Redis in terms of read performance and cost. Each store also has its own API idiosyncrasies that can impact performance. The Feast community is continuously optimizing store-specific performance.

## Benchmark Results

The raw data exists at [https://github.com/feast-dev/feast-benchmarks](https://github.com/feast-dev/feast-benchmarks). We choose a subset of comparisons here to answer some of the most common questions we hear from the community.

### Summary

* The Java feature server is very fast (e.g. p99 latency is ~1.3 ms for a single row fetch of 250 features)
  * Note: The Java feature server is in Beta and does not support new functionality such as the more scalable SQL registry.

The Beta Feast Java feature server with Redis provides very low latency retrieval (p99 < 1.5ms for single row retrieval of 250 features), but at increased architectural complexity, less first class support for functionality (e.g. no SQL registry support), and more overhead in managing Redis clusters. Using a Python server with other managed online stores like DynamoDB or Datastore is easier to manage.

Note: there are managed services for Redis like Redis Enterprise Cloud which remove the additional complexity associated with managing Redis clusters and provide additional benefits.

### What's next

The community is always improving Feast performance, and we'll post updates to performance improvements in the future. Future improvements in the works include:

* Improved on demand transformation performance
* Improved pooling of clients (e.g. we've seen that caching Google clients significantly improves response times and reduces memory consumption)
