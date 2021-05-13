# Architecture

![](../../.gitbook/assets/image%20%286%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29%20%282%29%20%281%29%20%282%29.png)

## Sequence description

1. **Log Raw Events:** Production backend applications are configured to emit internal state changes as events to a stream.
2. **Create Stream Features:** Stream processing systems like Flink, Spark, and Beam are used to transform and refine events and to produce features that are logged back to the stream.
3. **Log Streaming Features:** Both raw and refined events are logged into a data lake or batch storage location.
4. **Create Batch Features:** ELT/ETL systems like Spark and SQL are used to transform data in the batch store.
5. **Define and Ingest Features:** The Feast user defines [feature tables](feature-tables.md) based on the features available in batch and streaming sources and publish these definitions to Feast Core.
6. **Poll Feature Definitions:** The Feast Job Service polls for new or changed feature definitions.
7. **Start Ingestion Jobs:** Every new feature table definition results in a new ingestion job being provisioned \(see limitations\).
8. **Batch Ingestion:** Batch ingestion jobs are short-lived jobs that load data from batch sources into either an offline or online store \(see limitations\).
9. **Stream Ingestion:** Streaming ingestion jobs are long-lived jobs that load data from stream sources into online stores. A stream source and batch source on a feature table must have the same features/fields.
10. **Model Training:** A model training pipeline is launched. It uses the Feast Python SDK to retrieve a training dataset and trains a model.
11. **Get Historical Features:** Feast exports a point-in-time correct training dataset based on the list of features and entity DataFrame provided by the model training pipeline.
12. **Deploy Model:** The trained model binary \(and list of features\) are deployed into a model serving system.
13. **Get Prediction:** A backend system makes a request for a prediction from the model serving service.
14. **Retrieve Online Features:** The model serving service makes a request to the Feast Online Serving service for online features using a Feast SDK.
15. **Return Prediction:** The model serving service makes a prediction using the returned features and returns the outcome.

{% hint style="warning" %}
Limitations

* Only Redis is supported for online storage.
* Batch ingestion jobs must be triggered from your own scheduler like Airflow. Streaming ingestion jobs are automatically launched by the Feast Job Service.
{% endhint %}

## Components:

A complete Feast deployment contains the following components:

* **Feast Core:** Acts as the central registry for feature and entity definitions in Feast. 
* **Feast Job Service:** Manages data processing jobs that load data from sources into stores, and jobs that export training datasets.
* **Feast Serving:** Provides low-latency access to feature values in an online store.
* **Feast Python SDK CLI:** The primary user facing SDK. Used to:
  * Manage feature definitions with Feast Core.
  * Launch jobs through the Feast Job Service.
  * Retrieve training datasets.
  * Retrieve online features.
* **Online Store:** The online store is a database that stores only the latest feature values for each entity. The online store can be populated by either batch ingestion jobs \(in the case the user has no streaming source\), or can be populated by a streaming ingestion job from a streaming source. Feast Online Serving looks up feature values from the online store.
* **Offline Store:** The offline store persists batch data that has been ingested into Feast. This data is used for producing training datasets.
* **Feast Spark SDK:** A Spark specific Feast SDK. Allows teams to use Spark for loading features into an online store and for building training datasets over offline sources.

Please see the [configuration reference](../reference-1/configuration-reference.md#overview) for more details on configuring these components.

{% hint style="info" %}
Java and Go Clients are also available for online feature retrieval. See [API Reference](../reference-1/api/).
{% endhint %}

