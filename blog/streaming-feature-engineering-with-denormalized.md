# Streaming Feature Engineering with Denormalized

*December 17, 2024* | *Matt Green*

Learn how to use Feast with [Denormalized](https://www.denormalized.io/)

Thank you to [Matt Green](https://www.linkedin.com/in/mgreen9/) and [Francisco Javier Arceo](https://www.linkedin.com/in/franciscojavierarceo) for their contributions!

## Introduction

Feature stores have become a critical component of the modern AI stack where they serve as a centralized repository for model features. Typically, they consist of both an offline store for aggregating large amounts of data while training models, and an online store, which allows for low latency delivery of specific features when running inference.

A popular open source example is [Feast](https://feast.dev/), which allows users to store features together by ingesting data from different data sources. While Feast allows you to define features and query data stores using those definitions, it relies on external systems to calculate and update online features. This post will demonstrate how to use [denormalized](https://www.denormalized.io/) to build real-time feature pipelines.

The full working example is available at the [feast-dev/feast-denormalized-tutorial](https://github.com/feast-dev/feast-denormalized-tutorial) repo. Instructions for configuring and running the example can be found in the README file.

## The Problem

Fraud detection is a classic example of a model that uses real-time features. Imagine you are building a model to detect fraudulent user sessions. One feature you would be interested in is the number of login attempts made by a user and how many of those were successful. You could calculate this feature by looking back in time over a sliding interval (AKA "a sliding window"). If you notice a large amount of failed login attempts over the previous 5 seconds, you might infer the account is being brute-forced and choose to invalidate the session and lock the account.

To simulate this scenario, we wrote a simple script that emits fake login events to a Kafka cluster: [session_generator](https://github.com/feast-dev/feast-denormalized-tutorial).

This script will emit json events according to the following schema:

```python
@dataclass
timestamp: datetime
user_id: str
ip_address: str
success: bool
```

## Configuring the Feature Store with Feast

Before we can start writing our features, we need to first configure the feature store. Feast makes this easy using a Python API. In Feast, features are referred to as Fields and are grouped into FeatureViews. FeatureViews have corresponding PushSources for ingesting data from online sources (i.e., we can push data to Feast). We also define an offline data store using the FileSource class, though we won't be using that in this example.

```python
file_sources = []
push_sources = []
feature_views = []

for i in [1, 5, 10, 15]:
    file_source = FileSource(
        path=str(Path(__file__).parent / f"./data/auth_attempt_{i}.parquet"),
        timestamp_field="timestamp",
    )
    file_sources.append(file_source)

    push_source = PushSource(
        name=f"auth_attempt_push_{i}",
        batch_source=file_source,
    )
    push_sources.append(push_source)

    feature_views.append(
        FeatureView(
            name=f"auth_attempt_view_w{i}",
            entities=[auth_attempt],
            schema=[
                Field(name="user_id", dtype=feast_types.String,),
                Field(name="timestamp", dtype=feast_types.String,),
                Field(name=f"{i}_success", dtype=feast_types.Int32,),
                Field(name=f"{i}_total", dtype=feast_types.Int32,),
                Field(name=f"{i}_ratio", dtype=feast_types.Float32,),
            ],
            source=push_source,
            online=True,
        )
    )
```

The code creates 4 different FeatureViews each containing their own features. As discussed previously, fraud features can be calculated over a sliding interval. It can be useful to not only look at recent failed authentication attempts but also the aggregate of attempts made over longer time intervals. This could be useful when trying to detect things like credential testing which can happen over a longer period of time.

In our example, we're creating 4 different FeatureViews that will ultimately be populated by 4 different window lengths. This can help our model detect various types of attacks over different time intervals. Before we can use our features, we'll need to run `feast apply` to set-up the online datastore.

## Writing the Pipelines with Denormalized

Now that we have our online data store configured, we need to write our data pipelines for computing the features. Simply speaking, these pipelines need to:

1. Read messages from Kafka
2. Aggregate those messages over a varied timeframe
3. Write the resulting aggregate value to the feature store

Denormalized makes this really easy. First, we create our DataStream object from a Context():

```python
ds = FeastDataStream(
    Context().from_topic(
        config.kafka_topic,
        feature_service, f"auth_attempt_push*{config.feature_prefix}"
    )
)
```

This will start the Denormalized Rust stream processing engine, which is powered by DataFusion so it's ultra-fast.

## Running Multiple Pipelines

The write_feast_feature() method is a blocking call that continuously executes one pipeline to produce a set of features across a single sliding window. If we want to calculate features for using different sliding window lengths, will need to configure and start multiple pipelines. We can easily do this using the multiprocessing library in python:

```python
for window_length in [1, 5, 10, 15]:
    config = PipelineConfig(
        window_length_ms=window_length * 1000,
        slide_length_ms=1000,
        feature_prefix=f"{window_length}",
        kafka_bootstrap_servers=args.kafka_bootstrap_servers,
        kafka_topic=args.kafka_topic,
    )
    process = multiprocessing.Process(
        target=run_pipeline,
        args=(config,),
        name=f"PipelineProcess-{window_length}",
        daemon=False,
    )
    processes.append(process)

for p in processes:
    try:
        p.start()
    except Exception as e:
        logger.error(f"Failed to start process {p.name}: {e}")
        cleanup_processes(processes)
        return
```

For each group of features we defined earlier, we spin up a different system process with a different window length. Each process will then execute its own instance of the Denormalized stream processing engine, which has its own thread pools for effective parallelization of work.

While this example demonstrates how you can easily run multiple Denormalized pipelines, in a production environment, you'd probably want each pipeline running in its own container.

## Final Thoughts

We've demonstrated how you can easily create real-time features using Feast and Denormalized. While working with streaming data can be a challenge, modern python libraries backed by fast native code are making it easier than ever to quickly iterate on model inputs.

Denormalized is currently in the early stages of development. If you have any feedback or questions, feel free to reach out at [hello@denormalized.io](mailto:hello@denormalized.io).
