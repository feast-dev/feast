# Introduction

## What is Feast?

Feast (**Fea**ture **St**ore) is a customizable operational data system that re-uses existing infrastructure to manage and serve machine learning features to realtime models.

Feast allows ML platform teams to:

* **Make features consistently available for training and serving** by managing an _offline store_ (to process historical data for scale-out batch scoring or model training), a low-latency _online store_ (to power real-time prediction)_,_ and a battle-tested _feature server_ (to serve pre-computed features online).
* **Avoid data leakage** by generating point-in-time correct feature sets so data scientists can focus on feature engineering rather than debugging error-prone dataset joining logic. This ensure that future feature values do not leak to models during training.
* **Decouple ML from data infrastructure** by providing a single data access layer that abstracts feature storage from feature retrieval, ensuring models remain portable as you move from training models to serving models, from batch models to realtime models, and from one data infra system to another.

{% hint style="info" %}
**Note:** Feast today primarily addresses _timestamped structured data_.
{% endhint %}

![](assets/feast_marchitecture.png)

## Who is Feast for?

Feast helps ML platform teams with DevOps experience productionize real-time models. Feast can also help these teams build towards a feature platform that improves collaboration between engineers and data scientists.

Feast is likely **not** the right tool if you

* are in an organization that’s just getting started with ML and is not yet sure what the business impact of ML is
* rely primarily on unstructured data
* need very low latency feature retrieval (e.g. p99 feature retrieval << 10ms)
* have a small team to support a large number of use cases

## What Feast is not?

### Feast is not

* **an** [**ETL**](https://en.wikipedia.org/wiki/Extract,\_transform,\_load) / [**ELT**](https://en.wikipedia.org/wiki/Extract,\_load,\_transform) **system:** Feast is not (and does not plan to become) a general purpose data transformation or pipelining system. Users often leverage tools like [dbt](https://www.getdbt.com) to manage upstream data transformations.
* **a data orchestration tool:** Feast does not manage or orchestrate complex workflow DAGs. It relies on upstream data pipelines to produce feature values and integrations with tools like [Airflow](https://airflow.apache.org) to make features consistently available.
* **a data warehouse:** Feast is not a replacement for your data warehouse or the source of truth for all transformed data in your organization. Rather, Feast is a light-weight downstream layer that can serve data from an existing data warehouse (or other data sources) to models in production.
* **a database:** Feast is not a database, but helps manage data stored in other systems (e.g. BigQuery, Snowflake, DynamoDB, Redis) to make features consistently available at training / serving time

### Feast does not _fully_ solve

* **reproducible model training / model backtesting / experiment management**: Feast captures feature and model metadata, but does not version-control datasets / labels or manage train / test splits. Other tools like [DVC](https://dvc.org/), [MLflow](https://www.mlflow.org/), and [Kubeflow](https://www.kubeflow.org/) are better suited for this.
* **batch + streaming feature engineering**: Feast primarily processes already transformed feature values (though it offers experimental light-weight transformations). Users usually integrate Feast with upstream systems (e.g. existing ETL/ELT pipelines). [Tecton](http://tecton.ai/) is a more fully featured feature platform which addresses these needs.
* **native streaming feature integration:** Feast enables users to push streaming features, but does not pull from streaming sources or manage streaming pipelines. [Tecton](http://tecton.ai/) is a more fully featured feature platform which orchestrates end to end streaming pipelines.
* **feature sharing**: Feast has experimental functionality to enable discovery and cataloguing of feature metadata with a [Feast web UI (alpha)](https://docs.feast.dev/reference/alpha-web-ui). Feast also has community contributed plugins with [DataHub](https://datahubproject.io/docs/generated/ingestion/sources/feast/) and [Amundsen](https://github.com/amundsen-io/amundsen/blob/4a9d60176767c4d68d1cad5b093320ea22e26a49/databuilder/databuilder/extractor/feast\_extractor.py). [Tecton](http://tecton.ai/) also more robustly addresses these needs.
* **lineage:** Feast helps tie feature values to model versions, but is not a complete solution for capturing end-to-end lineage from raw data sources to model versions. Feast also has community contributed plugins with [DataHub](https://datahubproject.io/docs/generated/ingestion/sources/feast/) and [Amundsen](https://github.com/amundsen-io/amundsen/blob/4a9d60176767c4d68d1cad5b093320ea22e26a49/databuilder/databuilder/extractor/feast\_extractor.py). [Tecton](http://tecton.ai/) captures more end-to-end lineage by also managing feature transformations.
* **data quality / drift detection**: Feast has experimental integrations with [Great Expectations](https://greatexpectations.io/), but is not purpose built to solve data drift / data quality issues. This requires more sophisticated monitoring across data pipelines, served feature values, labels, and model versions.

## Example use cases

Many companies have used Feast to power real-world ML use cases such as:

* Personalizing online recommendations by leveraging pre-computed historical user or item features.
* Online fraud detection, using features that compare against (pre-computed) historical transaction patterns
* Churn prediction (an offline model), generating feature values for all users at a fixed cadence in batch
* Credit scoring, using pre-computed historical features to compute probability of default

## How can I get started?

{% hint style="info" %}
The best way to learn Feast is to use it. Join our [Slack channel](http://slack.feast.dev) and head over to our [Quickstart](getting-started/quickstart.md) and try it out!
{% endhint %}

Explore the following resources to get started with Feast:

* [Quickstart](getting-started/quickstart.md) is the fastest way to get started with Feast
* [Concepts](getting-started/concepts/) describes all important Feast API concepts
* [Architecture](getting-started/architecture-and-components/) describes Feast's overall architecture.
* [Tutorials](tutorials/tutorials-overview/) shows full examples of using Feast in machine learning applications.
* [Running Feast with Snowflake/GCP/AWS](how-to-guides/feast-snowflake-gcp-aws/) provides a more in-depth guide to using Feast.
* [Reference](reference/feast-cli-commands.md) contains detailed API and design documents.
* [Contributing](project/contributing.md) contains resources for anyone who wants to contribute to Feast.
