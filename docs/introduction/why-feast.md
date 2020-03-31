# Why Feast?

## Lack of feature reuse

**Problem:** The process of engineering features is one of the most time consuming activities in building an end-to-end ML system. Despite this, many teams continue to redevelop the same features from scratch for every new project. Often these features never leaving the notebooks or pipelines they are built in.

**Solution:** A centralized feature store allows organizations to build up a foundation of features that can be reused across projects. Teams are then able to utilize features developed by other teams, and as more features are added to the store it becomes easier and cheaper to build models.

## Serving features is hard

**Problem:** Serving up to date features at scale is hard. Raw data can come from a variety of sources, from data lakes, to even streams, to data warehouse, to simply flat files. Data scientists need the ability to produce massive datasets of features from this data in order to train their models offline. These models then need access to real-time feature data at low latency and high throughput when they are served in production.

**Solution:** Feast is built to be able to ingest data from a variety of sources, supporting both streaming and batch sources. Once data is loaded into Feast as features, they become available through both a batch serving API as well as an real-time \(online serving\) API. These APIs allows data scientists and ML engineers to easily retrieve feature data for their development, training, or in production. Feast also comes with a Java, Go, and Python SDK to make this experience easy.

## **Models need point-in-time correctness**

**Problem:** Most data sources are not built with ML use cases in mind and by extension don't provide point-in-time correct lookups of feature data. One of the reasons why features are often re-engineered is because ML practitioners need to ensure that their models are trained on a dataset that accurately models the state of the world when the model runs in production.

**Solution:** Feast allows end users to create point-in-time correct datasets across multiple entities. Feast ensures that there is no data leakage, that cross feature set joins are valid, and that models are not fed expired data.

## Definitions of features vary

**Problem:** Teams define features differently and there is no easy access to the documentation of a feature.

**Solution:** Feast becomes the single source of truth for all feature data for all models within an organization. Teams are able to capture documentation, metadata and metrics about features. This allows teams to communicate clearly about features, test features data, and determine if a feature is useful for a particular model.

## **Inconsistency between training and serving**

**Problem:** Training requires access to historical data, whereas models that serve predictions need the latest values. Inconsistencies arise when data is siloed into many independent systems requiring separate tooling. Often teams are using Python for creating batch features off line, but these features are redeveloped with different libraries and languages when moving to serving or streaming systems.

**Solution:** Feast provides consistency by managing and unifying the ingestion of data from batch and streaming sources into both the feature warehouse and feature serving stores. Feast becomes the bridge between your model and your data, both for training and serving. This ensures that there is a consistency in the feature data that your model receives.

\*\*\*\*

