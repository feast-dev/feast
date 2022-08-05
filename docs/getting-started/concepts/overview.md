# Overview

### Feast project structure

The top-level namespace within Feast is a **project**. Users define one or more [feature views](feature-view.md) within a project. Each feature view contains one or more [features](feature-view.md#feature). These features typically relate to one or more [entities](entity.md). A feature view must always have a [data source](data-ingestion.md), which in turn is used during the generation of training [datasets](feature-retrieval.md#dataset) and when materializing feature values into the online store.

![](<../../.gitbook/assets/image (7).png>)

**Projects** provide complete isolation of feature stores at the infrastructure level. This is accomplished through resource namespacing, e.g., prefixing table names with the associated project. Each project should be considered a completely separate universe of entities and features. It is not possible to retrieve features from multiple projects in a single request. We recommend having a single feature store and a single project per environment (`dev`, `staging`, `prod`).

### Data ingestion

For _offline use cases_ that only rely on batch data, Feast does not need to ingest data and can query your existing data (leveraging a compute engine, whether it be a data warehouse or (experimental) Spark / Trino). Feast can help manage **pushing** streaming features to a batch source to make features available for training.

For _online use cases_, Feast supports **ingesting** features from batch sources to make them available online (through a process called **materialization**), and **pushing** streaming features to make them available both offline / online. We explore this more in the next concept page ([Data ingestion](data-ingestion.md))

### Feature registration and retrieval

Features are _registered_ as code in a version controlled repository, and tie to data sources + model versions via the concepts of **entities, feature views,** and **feature services.** We explore these concepts more in the upcoming concept pages. These features are then _stored_ in a **registry**, which can be accessed across users and services. The features can then be _retrieved_ via SDK API methods or via a deployed **feature server** which exposes endpoints to query for online features (to power real time models).



Feast supports several patterns of feature retrieval.

|                         Use case                         |                                                Example                                                 |            API            |
| :------------------------------------------------------: | :----------------------------------------------------------------------------------------------------: | :-----------------------: |
|                 Training data generation                 | Fetching user and item features for (user, item) pairs when training a production recommendation model | `get_historical_features` |
|     Offline feature retrieval for batch predictions      |                          Predicting user churn for all users on a daily basis                          | `get_historical_features` |
| Online feature retrieval for real-time model predictions |  Fetching pre-computed features to predict whether a real-time credit card transaction is fraudulent   |   `get_online_features`   |
