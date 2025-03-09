# Overview

![Feast Architecture Diagram](<../../.gitbook/assets/image (4).png>)

## Functionality

* **Create Batch Features:** ELT/ETL systems like Spark and SQL are used to transform data in the batch store.
* **Create Stream Features:** Stream features are created from streaming services such as Kafka or Kinesis, and can be pushed directly into Feast via the [Push API](../../reference/data-sources/push.md).
* **Feast Apply:** The user (or CI) publishes versioned controlled feature definitions using `feast apply`. This CLI command updates infrastructure and persists definitions in the object store registry.
* **Feast Materialize:** The user (or scheduler) executes `feast materialize` which loads features from the offline store into the online store.
* **Model Training:** A model training pipeline is launched. It uses the Feast Python SDK to retrieve a training dataset that can be used for training models.
* **Get Historical Features:** Feast exports a point-in-time correct training dataset based on the list of features and entity dataframe provided by the model training pipeline.
* **Deploy Model:** The trained model binary (and list of features) are deployed into a model serving system. This step is not executed by Feast.
* **Prediction:** A backend system makes a request for a prediction from the model serving service.
* **Get Online Features:** The model serving service makes a request to the Feast Online Serving service for online features using a Feast SDK.
* **Feature Retrieval:** The online serving service retrieves the latest feature values from the online store and returns them to the model serving service.

## Components

A complete Feast deployment contains the following components:

* **Feast Registry**: An object store (GCS, S3) based registry used to persist feature definitions that are registered with the feature store. Systems can discover feature data by interacting with the registry through the Feast SDK.
* **Feast Python SDK/CLI:** The primary user facing SDK. Used to:
  * Manage version controlled feature definitions.
  * Materialize (load) feature values into the online store.
  * Build and retrieve training datasets from the offline store.
  * Retrieve online features.
* **Feature Server:** The Feature Server is a REST API server that serves feature values for a given entity key and feature reference. The Feature Server is designed to be horizontally scalable and can be deployed in a distributed manner.
* **Stream Processor:** The Stream Processor can be used to ingest feature data from streams and write it into the online or offline stores. Currently, there's an experimental Spark processor that's able to consume data from Kafka.
* **Batch Materialization Engine:** The [Batch Materialization Engine](batch-materialization-engine.md) component launches a process which loads data into the online store from the offline store. By default, Feast uses a local in-process engine implementation to materialize data. However, additional infrastructure can be used for a more scalable materialization process.
* **Online Store:** The online store is a database that stores only the latest feature values for each entity. The online store is either populated through materialization jobs or through [stream ingestion](../../reference/data-sources/push.md).
* **Offline Store:** The offline store persists batch data that has been ingested into Feast. This data is used for producing training datasets. For feature retrieval and materialization, Feast does not manage the offline store directly, but runs queries against it. However, offline stores can be configured to support writes if Feast configures logging functionality of served features.
* **Authorization Manager**: The authorization manager detects authentication tokens from client requests to Feast servers and uses this information to enforce permission policies on the requested services.
