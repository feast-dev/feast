from pyspark.sql import SparkSession# Running Feast in production

## Overview

After learning about Feast concepts and playing with Feast locally, you're now ready to use Feast in production. This guide aims to help with the transition from a sandbox project to production-grade deployment in the cloud or on-premise.

Overview of typical production configuration is given below:

![Overview](production-simple.png)

{% hint style="success" %}
**Important note:** Feast is highly customizable and modular.

Most Feast blocks are loosely connected and can be used independently. Hence, you are free to build your own production configuration.

For example, you might not have a stream source and, thus, no need to write features in real-time to an online store. Or you might not need to retrieve online features. Feast also often provides multiple options to achieve the same goal. We discuss tradeoffs below.

Additionally, please check the how-to guide for some specific recommendations on [how to scale Feast](./scaling-feast.md).
{% endhint %}

In this guide we will show you how to:

1. Deploy your feature store and keep your infrastructure in sync with your feature repository
2. Keep the data in your online store up to date
3. Use Feast for model training and serving
4. Ingest features from a stream source
5. Monitor your production deployment

## 1. Automatically deploying changes to your feature definitions

### 1.1 Setting up a feature repository

The first step to setting up a deployment of Feast is to create a Git repository that contains your feature definitions. The recommended way to version and track your feature definitions is by committing them to a repository and tracking changes through commits. If you recall, running `feast apply` commits feature definitions to a **registry**, which users can then read elsewhere.

### 1.2 Setting up a database-backed registry

Out of the box, Feast serializes all of its state into a file-based registry. When running Feast in production, we recommend using the more scalable SQL-based registry that is backed by a database. Details are available [here](./scaling-feast.md#scaling-feast-registry).

### 1.3 Setting up CI/CD to automatically update the registry

We recommend typically setting up CI/CD to automatically run `feast plan` and `feast apply` when pull requests are opened / merged.

### 1.4 Setting up multiple environments

A common scenario when using Feast in production is to want to test changes to Feast object definitions. For this, we recommend setting up a _staging_ environment for your offline and online stores, which mirrors _production_ (with potentially a smaller data set).
Having this separate environment allows users to test changes by first applying them to staging, and then promoting the changes to production after verifying the changes on staging.

Different options are presented in the [how-to guide](structuring-repos.md).

## 2. How to load data into your online store and keep it up to date

To keep your online store up to date, you need to run a job that loads feature data from your feature view sources into your online store. In Feast, this loading operation is called materialization.

### 2.1 Scalable Materialization

Out of the box, Feast's materialization process uses an in-process materialization engine. This engine loads all the data being materialized into memory from the offline store, and writes it into the online store. 

This approach may not scale to large amounts of data, which users of Feast may be dealing with in production.
In this case, we recommend using one of the more [scalable materialization engines](./scaling-feast.md#scaling-materialization), such as the [Bytewax Materialization Engine](../reference/batch-materialization/bytewax.md), or the [Snowflake Materialization Engine](../reference/batch-materialization/snowflake.md).
Users may also need to [write a custom materialization engine](../how-to-guides/customizing-feast/creating-a-custom-materialization-engine.md) to work on their existing infrastructure.  

The Bytewax materialization engine can run materialization on existing kubernetes cluster. An example configuration of this in a `feature_store.yaml` is as follows:

```yaml
batch_engine:
  type: bytewax
  namespace: bytewax
  image: bytewax/bytewax-feast:latest
  env:
    - name: AWS_ACCESS_KEY_ID
      valueFrom:
        secretKeyRef:
          name: aws-credentials
          key: aws-access-key-id
    - name: AWS_SECRET_ACCESS_KEY
      valueFrom:
        secretKeyRef:
          name: aws-credentials
          key: aws-secret-access-key
```



### 2.1. Manual materialization

The simplest way to schedule materialization is to run an **incremental** materialization using the Feast CLI:

```
feast materialize-incremental 2022-01-01T00:00:00
```

The above command will load all feature values from all feature view sources into the online store up to the time `2022-01-01T00:00:00`.

A timestamp is required to set the end date for materialization. If your source is fully up-to-date then the end date would be the current time. However, if you are querying a source where data is not yet available, then you do not want to set the timestamp to the current time. You would want to use a timestamp that ends at a date for which data is available. The next time `materialize-incremental` is run, Feast will load data that starts from the previous end date, so it is important to ensure that the materialization interval does not overlap with time periods for which data has not been made available. This is commonly the case when your source is an ETL pipeline that is scheduled on a daily basis.

An alternative approach to incremental materialization (where Feast tracks the intervals of data that need to be ingested), is to call Feast directly from your scheduler like Airflow. In this case, Airflow is the system that tracks the intervals that have been ingested.

```
feast materialize -v driver_hourly_stats 2020-01-01T00:00:00 2020-01-02T00:00:00
```

In the above example we are materializing the source data from the `driver_hourly_stats` feature view over a day. This command can be scheduled as the final operation in your Airflow ETL, which runs after you have computed your features and stored them in the source location. Feast will then load your feature data into your online store.

The timestamps above should match the interval of data that has been computed by the data transformation system.

### 2.2. Automate periodic materialization

It is up to you which orchestration/scheduler to use to periodically run `$ feast materialize`. Feast keeps the history of materialization in its registry so that the choice could be as simple as a [unix cron util](https://en.wikipedia.org/wiki/Cron). Cron util should be sufficient when you have just a few materialization jobs (it's usually one materialization job per feature view) triggered infrequently. However, the amount of work can quickly outgrow the resources of a single machine. That happens because the materialization job needs to repackage all rows before writing them to an online store. That leads to high utilization of CPU and memory. In this case, you might want to use a job orchestrator to run multiple jobs in parallel using several workers. Kubernetes Jobs or Airflow are good choices for more comprehensive job orchestration.

If you are using Airflow as a scheduler, Feast can be invoked through the [BashOperator](https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/bash.html) after the [Python SDK](https://pypi.org/project/feast/) has been installed into a virtual environment and your feature repo has been synced:

```python
import datetime

materialize = BashOperator(
    task_id='materialize',
    bash_command=f'feast materialize-incremental {datetime.datetime.now().replace(microsecond=0).isoformat()}',
)
```

{% hint style="success" %}
Important note: Airflow worker must have read and write permissions to the registry file on GS / S3 since it pulls configuration and updates materialization history.
{% endhint %}



## 3. How to use Feast for model training

After we've defined our features and data sources in the repository, we can generate training datasets.

The first thing we need to do in our training code is to create a `FeatureStore` object with a path to the registry.

One way to ensure your production clients have access to the feature store is to provide a copy of the `feature_store.yaml` to those pipelines. This `feature_store.yaml` file will have a reference to the feature store registry, which allows clients to retrieve features from offline or online stores.

```python
from feast import FeatureStore

fs = FeatureStore(repo_path="production/")
```

Then, training data can be retrieved as follows:

```python
feature_refs = [
    'driver_hourly_stats:conv_rate',
    'driver_hourly_stats:acc_rate',
    'driver_hourly_stats:avg_daily_trips'
]

training_df = fs.get_historical_features(
    entity_df=entity_df,
    features=feature_refs,
).to_df()

model = ml.fit(training_df)
```

The most common way to productionize ML models is by storing and versioning models in a "model store", and then deploying these models into production. When using Feast, it is recommended that the list of feature references also be saved alongside the model. This ensures that models and the features they are trained on are paired together when being shipped into production:

```python
import json
# Save model
model.save('my_model.bin')

# Save features
with open('feature_refs.json', 'w') as f:
    json.dump(feature_refs, f)
```

To test your model locally, you can simply create a `FeatureStore` object, fetch online features, and then make a prediction:

```python
# Load model
model = ml.load('my_model.bin')

# Load feature references
with open('feature_refs.json', 'r') as f:
    feature_refs = json.load(f)

# Create feature store object
fs = FeatureStore(repo_path="production/")

# Read online features
feature_vector = fs.get_online_features(
    features=feature_refs,
    entity_rows=[{"driver_id": 1001}]
).to_dict()

# Make a prediction
prediction = model.predict(feature_vector)
```

{% hint style="success" %}
It is important to note that both the training pipeline and model serving service need only read access to the feature registry and associated infrastructure. This prevents clients from accidentally making changes to the feature store.
{% endhint %}

## 4. Retrieving online features for prediction

Once you have successfully loaded (or in Feast terminology materialized) your data from batch sources into the online store, you can start consuming features for model inference. There are three approaches for that purpose sorted from the most simple one (in an operational sense) to the most performant (benchmarks to be published soon):

### 4.1. Use the Python SDK within an existing Python service

This approach is the most convenient to keep your infrastructure as minimalistic as possible and avoid deploying extra services. The Feast Python SDK will connect directly to the online store (Redis, Datastore, etc), pull the feature data, and run transformations locally (if required). The obvious drawback is that your service must be written in Python to use the Feast Python SDK. A benefit of using a Python stack is that you can enjoy production-grade services with integrations with many existing data science tools.

To integrate online retrieval into your service use the following code:

```python
from feast import FeatureStore

with open('feature_refs.json', 'r') as f:
    feature_refs = json.loads(f)

fs = FeatureStore(repo_path="production/")

# Read online features
feature_vector = fs.get_online_features(
    features=feature_refs,
    entity_rows=[{"driver_id": 1001}]
).to_dict()
```

### 4.2. Consume features via HTTP API from Serverless Feature Server

If you don't want to add the Feast Python SDK as a dependency, or your feature retrieval service is written in a non-Python language, Feast can deploy a simple feature server on serverless infrastructure (eg, AWS Lambda, Google Cloud Run) for you. This service will provide an HTTP API with JSON I/O, which can be easily used with any programming language.

[Read more about this feature](../reference/feature-servers/alpha-aws-lambda-feature-server.md)

### 4.3. Go feature server deployed on Kubernetes

For users with very latency-sensitive and high QPS use-cases, Feast offers a high-performance [Go feature server](../reference/feature-servers/go-feature-server.md). It can use either HTTP or gRPC.

The Go feature server can be deployed to a Kubernetes cluster via Helm charts in a few simple steps:

1. Install [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/) and [helm 3](https://helm.sh/)
2. Add the Feast Helm repository and download the latest charts:

```
helm repo add feast-charts https://feast-helm-charts.storage.googleapis.com
helm repo update
```

1. Run Helm Install

```
helm install feast-release feast-charts/feast-feature-server \
    --set global.registry.path=s3://feast/registries/prod \
    --set global.project=<project name>
```

This chart will deploy a single service. The service must have read access to the registry file on cloud storage. It will keep a copy of the registry in their memory and periodically refresh it, so expect some delays in update propagation in exchange for better performance. In order for the Go feature server to be enabled, you should set `go_feature_serving: True` in the `feature_store.yaml`.

## 5. Ingesting features from a stream source

Recently Feast added functionality for [stream ingestion](../reference/data-sources/push.md). Please note that this is still in an early phase and new incompatible changes may be introduced.

### 5.1. Using Python SDK in your Apache Spark / Beam pipeline

The default option to write features from a stream is to add the Python SDK into your existing PySpark / Beam pipeline. Feast SDK provides writer implementation that can be called from `foreachBatch` stream writer in PySpark like this:

```python
from feast import FeatureStore

store = FeatureStore(...)

spark = SparkSession.builder.getOrCreate()

streamingDF = spark.readStream.format(...).load()

def feast_writer(spark_df):
    pandas_df = spark_df.to_pandas()
    store.push("driver_hourly_stats", pandas_df)

streamingDF.writeStream.foreachBatch(feast_writer).start()
```

### 5.2. Push Service (Alpha)

Alternatively, if you want to ingest features directly from a broker (eg, Kafka or Kinesis), you can use the "push service", which will write to an online store and/or offline store. This service will expose an HTTP API or when deployed on Serverless platforms like AWS Lambda or Google Cloud Run, this service can be directly connected to Kinesis or PubSub.

If you are using Kafka, [HTTP Sink](https://docs.confluent.io/kafka-connect-http/current/overview.html) could be utilized as a middleware. In this case, the "push service" can be deployed on Kubernetes or as a Serverless function.

## 6. Using environment variables in your yaml configuration

You might want to dynamically set parts of your configuration from your environment. For instance to deploy Feast to production and development with the same configuration, but a different server. Or to inject secrets without exposing them in your git repo. To do this, it is possible to use the `${ENV_VAR}` syntax in your `feature_store.yaml` file. For instance:

```yaml
project: my_project
registry: data/registry.db
provider: local
online_store:
    type: redis
    connection_string: ${REDIS_CONNECTION_STRING}
```

It is possible to set a default value if the environment variable is not set, with `${ENV_VAR:"default"}`. For instance:

```yaml
project: my_project
registry: data/registry.db
provider: local
online_store:
    type: redis
    connection_string: ${REDIS_CONNECTION_STRING:"0.0.0.0:6379"}
```

***

## Summary

Summarizing it all together we want to show several options of architecture that will be most frequently used in production:

### Current Suggestions 

* Feast SDK is being triggered by CI (eg, Github Actions). It applies the latest changes from the feature repo to the Feast registry
* Airflow manages materialization jobs to ingest data from DWH to the online store periodically
* For the stream ingestion Feast Python SDK is used in the existing Spark / Beam pipeline
* For Batch Materialization Engine:
  * If your offline and online workloads are in Snowflake, the Snowflake Engine is likely the best option.
  * If your offline and online workloads are not using Snowflake, but using Kubernetes is an option, the Bytewax engine is likely the best option.
  * If none of these engines suite your needs, you may continue using the in-process engine, or write a custom engine.
* Online features are served via the Python feature server over HTTP, or consumed using the Feast Python SDK.
* Feast Python SDK is called locally to generate a training dataset

![From Repository to Production: Feast Production Architecture](production-spark-bytewax.png)
