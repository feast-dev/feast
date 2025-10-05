# Go feature server benchmarks

*July 19, 2022* | *Felix Wang*

## Background

The Feast team published a [blog post](https://feastsite.wpenginepowered.com/blog/feast-benchmarks/) several months ago with latency benchmarks for all of our online feature retrieval options. Since then, we have built a Go feature server. It is currently in alpha mode, and only supports Redis as an online store. The docs are [here](https://docs.feastsite.wpenginepowered.com/reference/feature-servers/go-feature-server/). We recommend teams that require extremely low-latency feature serving to try the Go feature server. To test it, we ran our benchmarks against it; the results are presented below.

## Benchmark Setup

See [https://github.com/feast-dev/feast-benchmarks](https://github.com/feast-dev/feast-benchmarks) for the exact benchmark code. The feature servers were deployed in Docker on AWS EC2 instances (c5.4xlarge, 16vCPU, 64GiB memory).

## Data and query patterns

Feast's feature retrieval primarily manages retrieving the latest values of a given feature for specified entities. In this benchmark, the online stores contain:

* 25 feature views (with 10 features per feature view) for a total of 250 features
* 1M entity rows

As described in [RFC-031](https://docs.google.com/document/d/12UuvTQnTTCJ), we simulate different query patterns by additionally varying by number of entity rows in a request (i.e. *batch size*), requests per second, and the concurrency of the feature server. The goal here is to have numbers that apply to a diverse set of teams, regardless of their scale and typical query patterns. Users are welcome to extend the benchmark suite to better test their own setup.

## Online store setup

These benchmarks only used Redis as an online store. We used a single Redis server, run locally with Docker Compose on an EC2 instance. This should closely approximate usage of a separate Redis server in AWS. Typical network latency within the same availability zone in AWS is [< 1-2 ms](https://aws.amazon.com/blogs/architecture/improving-performance-and-reducing-cost-using-availability-zone-affinity/). In these benchmarks, we did not hit limits that required use of a Redis cluster. With higher batch sizes, the benchmark suite would likely only work with Redis clusters. Redis clusters should improve Feast's performance.

## Benchmark Results

### Summary

* The Go feature server is very fast (e.g. p99 latency is ~3.9 ms for a single row fetch of 250 features)
* For the same number of features and batch size, the Go feature server is about 3-5x faster than the Python feature server
  * Despite this, there are still compelling reasons to use Python, depending on your situation (e.g. simplicity of deployment)
* Feature server latency…
  * scales linearly (moderate slope) with batch size
  * scales linearly (low slope) with number of features
  * does not substantially change as requests per seconds increase

### Latency when varying by batch size

For this comparison, we check retrieval of 50 features across 5 feature views. At p99, we see that Go significantly outperforms Python, by ~3-5x. It also scales much better with batch size.

| Batch size | 1 | 10 | 20 | 30 | 40 | 50 | 60 | 70 | 80 | 90 | 100 |
|------------|---|----|----|----|----|----|----|----|----|----|----|
| Python | 7.23 | 15.14 | 23.96 | 32.80 | 41.44 | 50.43 | 59.88 | 94.57 | 103.28 | 111.93 | 124.87 |
| Go | 4.32 | 3.88 | 6.09 | 8.16 | 10.13 | 12.32 | 14.3 | 16.28 | 18.53 | 20.27 | 22.18 |

### Latency when varying by number of requested features

The Go feature server scales a bit better than the Python feature server in terms of supporting a large number of features:
p99 retrieval times (ms), varying by number of requested features (batch size = 1)

| Num features | 50 | 100 | 150 | 200 | 250 |
|-------------|----|----|-----|-----|-----|
| Python | 8.42 | 10.28 | 13.36 | 16.69 | 45.41 |
| Go | 1.78 | 2.43 | 2.98 | 3.33 | 3.92 |
