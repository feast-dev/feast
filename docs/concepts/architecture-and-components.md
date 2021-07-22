# Architecture

![Feast Architecture Diagram](../.gitbook/assets/image%20%284%29.png)

#### Functionality

* **Create Batch Features:** ELT/ETL systems like Spark and SQL are used to transform data in the batch store.
* **Feast Apply:**  The user \(or CI\) publishes versioned controlled feature definitions using `feast apply`. This CLI command updates infrastructure and persists definitions in the object store registry.
* **Feast Materialize:** The user \(or scheduler\) executes `feast materialize` which loads features from the offline store into the online store.
* **Model Training:** A model training pipeline is launched. It uses the Feast Python SDK to retrieve a training dataset and trains a model.
* **Get Historical Features:** Feast exports a point-in-time correct training dataset based on the list of features and entity dataframe provided by the model training pipeline.
* **Deploy Model:** The trained model binary \(and list of features\) are deployed into a model serving system. This step is not executed by Feast.
* **Prediction:** A backend system makes a request for a prediction from the model serving service.
* **Get Online Features:** The model serving service makes a request to the Feast Online Serving service for online features using a Feast SDK.

#### Components

A complete Feast deployment contains the following components:

* **Feast Online Serving:** Provides low-latency access to feature values stores in the online store. This component is optional. Teams can also read feature values directly from the online store if necessary.
* **Feast Registry**: An object store \(GCS, S3\) based registry used to persist feature definitions that are registered with the feature store. Systems can discover feature data by interacting with the registry through the Feast SDK.
* **Feast Python SDK/CLI:** The primary user facing SDK. Used to:
  * Manage version controlled feature definitions.
  * Materialize \(load\) feature values into the online store.
  * Build and retrieve training datasets from the offline store.
  * Retrieve online features.
* **Online Store:** The online store is a database that stores only the latest feature values for each entity. The online store is populated by materialization jobs.
* **Offline Store:** The offline store persists batch data that has been ingested into Feast. This data is used for producing training datasets. Feast does not manage the offline store directly, but runs queries against it.

{% hint style="info" %}
Java and Go Clients are also available for online feature retrieval. See [API Reference](../feast-on-kubernetes-1/reference-1/api/).
{% endhint %}

