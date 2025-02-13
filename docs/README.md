# Introduction

## What is Feast?

Feast (**Fea**ture **St**ore) is an [open-source](https://github.com/feast-dev/feast) feature store that helps teams 
operate production ML systems at scale by allowing them to define, manage, validate, and serve features for production 
AI/ML. 

Feast's feature store is composed of two foundational components: (1) an [offline store](getting-started/components/offline-store.md) 
for historical feature extraction used in model training and an (2) [online store](getting-started/components/online-store.md) 
for serving features at low-latency in production systems and applications.

Feast is a configurable operational data system that re-uses existing infrastructure to manage and serve machine learning 
features to realtime models. For more details, please review our [architecture](getting-started/architecture/overview.md).

Concretely, Feast provides:

* A Python SDK for programmatically defining features, entities, sources, and (optionally) transformations
* A Python SDK for reading and writing features to configured offline and online data stores
* An [optional feature server](reference/feature-servers/README.md) for reading and writing features (useful for non-python languages)
* A [UI](reference/alpha-web-ui.md) for viewing and exploring information about features defined in the project
* A [CLI tool](reference/feast-cli-commands.md) for viewing and updating feature information

Feast allows ML platform teams to:

* **Make features consistently available for training and low-latency serving** by managing an _offline store_ (to process historical data for scale-out batch scoring or model training), a low-latency _online store_ (to power real-time prediction)_,_ and a battle-tested _feature server_ (to serve pre-computed features online).
* **Avoid data leakage** by generating point-in-time correct feature sets so data scientists can focus on feature engineering rather than debugging error-prone dataset joining logic. This ensures that future feature values do not leak to models during training.
* **Decouple ML from data infrastructure** by providing a single data access layer that abstracts feature storage from feature retrieval, ensuring models remain portable as you move from training models to serving models, from batch models to real-time models, and from one data infra system to another.

{% hint style="info" %}
**Note:** Feast today primarily addresses _timestamped structured data_.
{% endhint %}

![](assets/feast_marchitecture.png)

{% hint style="info" %}
**Note:** Feast uses a push model for online serving. This means that the feature store pushes feature values to the 
online store, which reduces the latency of feature retrieval. This is more efficient than a pull model, where the model 
serving system must make a request to the feature store to retrieve feature values. See 
[this document](getting-started/architecture/push-vs-pull-model.md) for a more detailed discussion.
{% endhint %}

## Who is Feast for?

Feast helps ML platform/MLOps teams with DevOps experience productionize real-time models. Feast also helps these teams build a feature platform that improves collaboration between data engineers, software engineers, machine learning engineers, and data scientists.

* *For Data Scientists*: Feast is a tool where you can easily define, store, and retrieve your features for both model development and model deployment. By using Feast, you can focus on what you do best: build features that power your AI/ML models and maximize the value of your data.
    
* *For MLOps Engineers*: Feast is a library that allows you to connect your existing infrastructure (e.g., online database, application server, microservice, analytical database, and orchestration tooling) that enables your Data Scientists to ship features for their models to production using a friendly SDK without having to be concerned with software engineering challenges that occur from serving real-time production systems. By using Feast, you can focus on maintaining a resilient system, instead of implementing features for Data Scientists.
    
* *For Data Engineers*: Feast provides a centralized catalog for storing feature definitions, allowing one to maintain a single source of truth for feature data. It provides the abstraction for reading and writing to many different types of offline and online data stores. Using either the provided Python SDK or the feature server service, users can write data to the online and/or offline stores and then read that data out again in either low-latency online scenarios for model inference, or in batch scenarios for model training.

* *For AI Engineers*: Feast provides a platform designed to scale your AI applications by enabling seamless integration of richer data and facilitating fine-tuning. With Feast, you can optimize the performance of your AI models while ensuring a scalable and efficient data pipeline.

## What Feast is not?

### Feast is not

* **An** [**ETL**](https://en.wikipedia.org/wiki/Extract,\_transform,\_load) / [**ELT**](https://en.wikipedia.org/wiki/Extract,\_load,\_transform) **system.** Feast is not a general purpose data pipelining system. Users often leverage tools like [dbt](https://www.getdbt.com) to manage upstream data transformations. Feast does support some [transformations](getting-started/architecture/feature-transformation.md).
* **A data orchestration tool:** Feast does not manage or orchestrate complex workflow DAGs. It relies on upstream data pipelines to produce feature values and integrations with tools like [Airflow](https://airflow.apache.org) to make features consistently available.
* **A data warehouse:** Feast is not a replacement for your data warehouse or the source of truth for all transformed data in your organization. Rather, Feast is a lightweight downstream layer that can serve data from an existing data warehouse (or other data sources) to models in production.
* **A database:** Feast is not a database, but helps manage data stored in other systems (e.g. BigQuery, Snowflake, DynamoDB, Redis) to make features consistently available at training / serving time

### Feast does not _fully_ solve
* **reproducible model training / model backtesting / experiment management**: Feast captures feature and model metadata, but does not version-control datasets / labels or manage train / test splits. Other tools like [DVC](https://dvc.org/), [MLflow](https://www.mlflow.org/), and [Kubeflow](https://www.kubeflow.org/) are better suited for this.
* **batch feature engineering**: Feast supports on-demand and streaming transformations. Feast is also investing in supporting batch transformations. 
* **native streaming feature integration:** Feast enables users to push streaming features, but does not pull from streaming sources or manage streaming pipelines.
* **lineage:** Feast helps tie feature values to model versions, but is not a complete solution for capturing end-to-end lineage from raw data sources to model versions. Feast also has community contributed plugins with [DataHub](https://datahubproject.io/docs/generated/ingestion/sources/feast/) and [Amundsen](https://github.com/amundsen-io/amundsen/blob/4a9d60176767c4d68d1cad5b093320ea22e26a49/databuilder/databuilder/extractor/feast\_extractor.py). 
* **data quality / drift detection**: Feast has experimental integrations with [Great Expectations](https://greatexpectations.io/), but is not purpose built to solve data drift / data quality issues. This requires more sophisticated monitoring across data pipelines, served feature values, labels, and model versions.

## Example use cases

Many companies have used Feast to power real-world ML use cases such as:

* Personalizing online recommendations by leveraging pre-computed historical user or item features.
* Online fraud detection, using features that compare against (pre-computed) historical transaction patterns
* Churn prediction (an offline model), generating feature values for all users at a fixed cadence in batch
* Credit scoring, using pre-computed historical features to compute the probability of default

## How can I get started?

{% hint style="info" %}
The best way to learn Feast is to use it. Head over to our [Quickstart](getting-started/quickstart.md) and try it out!
{% endhint %}

Explore the following resources to get started with Feast:

* [Quickstart](getting-started/quickstart.md) is the fastest way to get started with Feast
* [Concepts](getting-started/concepts/) describes all important Feast API concepts
* [Architecture](getting-started/architecture/) describes Feast's overall architecture.
* [Tutorials](tutorials/tutorials-overview/) shows full examples of using Feast in machine learning applications.
* [Running Feast with Snowflake/GCP/AWS](how-to-guides/feast-snowflake-gcp-aws/) provides a more in-depth guide to using Feast.
* [Reference](reference/feast-cli-commands.md) contains detailed API and design documents.
* [Contributing](project/contributing.md) contains resources for anyone who wants to contribute to Feast.
