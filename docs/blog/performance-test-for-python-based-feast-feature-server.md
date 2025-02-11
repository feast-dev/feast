# Performance Test for the Python-Based Feast Feature Server: Comparison Between DataStax Astra DB (Based on Apache Cassandra), Google Datastore & Amazon DynamoDB

*April 17, 2023* | *Stefano Lottini*

## Introduction

Feature stores are an essential part of the modern stack around machine learning (ML); in particular, the effort aimed at rationalizing the access patterns to the features associated with ML models by the various functions revolving around it (from data engineers to data scientists). At its core, a feature store provides a layer atop a persistent data store (a database) that facilitates shared access to the features associated with the entities belonging to a business domain, making it easier to retrieve them consistently for both training and prediction.

Out of several well-established feature stores available today, the most popular open-source solution is arguably [Feast](https://feastsite.wpenginepowered.com/). With its active base of contributors and support for a growing list of backends to choose from, ML practitioners don't have to worry about the boilerplate setup of their data system and can focus on delivering their product—all while retaining the freedom to choose the backend that best suits their needs.

Last year, the Feast team published [extensive benchmarks](https://feastsite.wpenginepowered.com/blog/feast-benchmarks/) comparing the performance of the feature store when using different storage layers for retrieval of "online" features (that is, up-to-date reads to calculate inferences, as opposed to batch or historical "offline" reads). The storage backends used in the test, each powered by its own Feast plugin, were: Redis (running locally), Google Datastore, and Amazon DynamoDB—the latter on the same cloud region as the testing client. The main takeaways were:

* Redis yields the lowest response times (but at a cost; see below)
* Among the cloud DB vendors, DynamoDB is noticeably faster than Datastore
* Latencies increase with the number of features needed and, albeit less so, with the number of rows ("entities")

Moreover, Feast offers an SDK for both Java and Python. Although choosing the Java stack for the feature server results in faster responses, the vast majority of Feast users work with a Python-centered stack. So in our tests, we'll focus on Python start-to-end setups.

Surveys done by Feast also showed that more than 60% of the interviewees required that P99 latency stay below 50 ms. These ultra-low-latency ML use cases often fall in the [fraud detection](https://www.tecton.ai/blog/how-to-build-a-fraud-detection-ml-system/) and [recommender system](https://www.tecton.ai/blog/guide-to-building-online-recommender-systems/) categories.

## Feature stores & Cassandra / Astra DB

The need for persistent data stores is ubiquitous in any ML application—and it comes in all sizes and shapes, of which the "feature store" pattern is but a certain, albeit very common, instance. As is discussed at length in the Feast blog post, many factors influence the architectural choices for ML-based systems. Besides serving latency, there are considerations about fault tolerance and data redundancy, ease of use, pricing, ease of integration with the rest of the stack, and so forth. For example, in some cases, it may be convenient to employ an in-memory store such as Redis, trading data durability and ease of scaling for reduced response times.

In this [recently published guide](https://planetcassandra.org/post/practitioners-guide-to-cassandra-for-ml/), the author highlights the fact that a feature store lies at the core of most ML-centered architectures lies, possibly (and, looking forward, more and more so) augmented with real-time capabilities owing to a combination of CDC (Change Data Capture), event-streaming technologies, and sometimes in-memory cache layers. The guide makes the case that Cassandra and DataStax's cloud-based DBaaS [Astra DB](https://astra.datastax.com/) (which is built on Cassandra) are great databases to build a feature store on top of, owing to the world-class fault tolerance, 100% uptime, and extremely low latencies it can offer out of the box.

We then set out to extend the performance measurements to Astra DB, with the intent to provide hard data corroborating our claim that Cassandra and Astra DB are performant first-class choices for an online feature store. In other words, once the plugin made its way to Feast, we took the next logical step: running the very same testing already done for the other DBaaS choice, but this time on Astra DB. The next section reports on our findings.

## Performance benchmarks for Feast on Astra DB

The Feast team published a Github [repository](https://github.com/feast-dev/feast-benchmarks) with the code used for the benchmarks. We added coverage for Astra DB (plus a one-node Cassandra cluster running locally, serving the purpose of a functional test) and upgraded the Feast version used in all benchmarks to use v0.26 consistently.

*Note: The original tests used v0.20 for DynamoDB, v0.17 for Datastore and v0.21 for Redis. Because we reproduced all pre-existing benchmarks, finding most values to be in acceptable agreement (see below for more remarks on this point), we are confident that upgrading the Feast version does not significantly alter the performance.*

The tests have been run on comparable AWS and GCP machines (respectively c5.4xlarge and c2-standard-16 instances) running in the same region as the cloud database (thereby mimicking the desired architecture for a production system). We did not change any benchmark parameter in order to keep the comparison meaningful, even with prior results. As stated earlier, we focused on the Python feature server, which has a wider adoption among the Feast community and supports a broader ecosystem of plugins.

Here's how we conducted the benchmarking. First, a moderate amount of synthetic "feature data" (10k entities with 250 integer features each, for a total of about 11 MB) was materialized to the online store. Then various one-minute test runs were performed, each with a certain choice of feature-retrieval parameters, all while collecting statistics (in particular, high percentiles) on the response time of these retrieval operations. The parameters that varied between runs were:

* batch size (1 to 100 entities per request)

Let's go back to the Cassandra plugin for Feast and examine some properties of how it was structured.

First, one might notice that, regardless of which features are requested at runtime, the whole partition (i.e., all features for a given entity) is read. This was chosen to avoid using IN clauses when querying Cassandra; these are indeed discouraged unless the number of values is very, very small (as a rule of thumb, less than half a dozen). Moreover, since one does not know at write-time which features will be read together, there is no preferred way to arrange the clustering column(s) to have these features grouped together in the partition (as done, for example, with Facebook's ["feature re-ordering"](https://engineering.fb.com/2022/09/19/ml-applications/feature-store-announcement/) which purportedly results in a 30%-70% latency reduction). A reasonable compromise was then to always read the whole partition and apply client-side post-query filtering to avoid burdening the query coordinators with additional work—at the cost, of course, of increased network throughput.

Second, when features from multiple entities are needed, the plugin makes good use of the execute_concurrently_with_args primitive offered by the Cassandra Python driver, thereby spawning one thread per partition and firing all requests at once (up to a maximum concurrency threshold, which can be configured). This leverages the excellent support for concurrency by the Cassandra architecture, which accounts for the observed moderate dependency of latencies on the batch size.

## Conclusion

We put the Cassandra plugin for Feast to test in the same way as other DBaaS plugins were tested; that is, using the Astra DB cloud database built on Cassandra, and we ran the same benchmarks that were applied to Redis, Datastore, and DynamoDB.

Besides broadly confirming the previous results published by the Feast team, our main finding is that the performance with Astra DB is on par with that of AWS DynamoDB and noticeably better than that of Google Datastore.

All these tests target the Python implementation. As mentioned in the Feast article, switching to a Java feature server greatly improves the performance, but requires a more convoluted setup and architecture and overall more expertise both for setup and maintenance.

Other evidence points to the fact that, *if one is mainly concerned about performance*, replacing any feature store with a direct-to-DB implementation may be the best choice. In this regard, our extensive investigations clearly make the case that Cassandra is a good fit for ML applications, regardless of whether a feature store is involved or not.

Some results might be made statistically stronger by more extensive tests, which could be a task for a future iteration of these performance benchmarks. It is possible that longer runs and/or much larger amounts of stored data would better highlight the underlying patterns in how the response times behave as a function of batch size and/or number of requested features.

## Acknowledgements

The author would like to thank Alan Ho, Scott Regan, and Jonathan Shook for a critical reading of this manuscript, and the Feast team for a pleasant and fruitful collaboration around the development (first) and the benchmarking (afterwards) of the Cassandra / Astra DB plugin for the namesake feature store.
