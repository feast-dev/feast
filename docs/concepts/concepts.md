# Concepts

## Architecture

![Logical diagram of a typical Feast deployment](../.gitbook/assets/basic-architecture-diagram%20%282%29.svg)

The core components of a Feast deployment are

* **Feast Core:** Feast Core is a centralized service that acts as the authority on features within an organization. Typically there is only one "Core" deployment per organization, with all feature management happening through it.
* **Feast Ingestion Jobs:** Feast ingestion jobs retrieve feature data from user defined data sources and populate serving stores with this feature data. These jobs are managed by Feast Core. Data can either be sources from existing sources \(like [Kafka](https://kafka.apache.org/)\), or it can be loaded into Feast through its API.
* **Feast Serving:** Feast Serving is the data access layer through which end users and production systems retrieve feature data. Each Serving store is backed by one or more databases. These databases are updated by the Feast ingestion jobs. There are two types of stores: batch and online. Batch stores hold large volumes historical data, while online stores only hold the latest feature values. 

## Data Model

### Feature Set

User data is typically in the form of dataframes, tables in data warehouses, or events on a stream. These data sources are loaded into Feast in order to serve features for model training or serving.

Feature sets allow for groups of fields in these data sources to be ingested and stored together. This allows for efficient storage and logical namespacing of data.

When data is loaded from these sources, each field in the feature set must be found in every record of the data source. Fields from these data sources must be either a timestamp, an entity, or a feature.

{% hint style="info" %}
Feature sets are a grouping of feature sets based on how they are loaded into Feast. They ensure that data is efficiently stored during ingestion. Feature sets are not a grouping of features for retrieval of features. During retrieval it is possible to retrieve feature values from any number of feature sets.
{% endhint %}

#### Customer Transactions Example

Below is an example of a basic `customer transactions` feature set that has been exported to YAML:

{% tabs %}
{% tab title="customer\_transactions\_feature\_set.yaml" %}
```yaml
name: customer_transactions
kind: feature_set
entities:
- name: customer_id
  valueType: INT64
features:
- name: daily_transactions
  valueType: FLOAT
- name: total_transactions
  valueType: FLOAT
  maxAge: 3600s
```
{% endtab %}
{% endtabs %}

The dataframe below \(`customer_data.csv`\) contains the features and entities of the above feature set

| datetime | customer\_id | daily\_transactions | total\_tra**nsactions** |
| :--- | :--- | :--- | :--- |
| 2019-01-01 01:00:00 | 20001 | 5.0 | 14.0 |
| 2019-01-01 01:00:00 | 20002 | 2.6 | 43.0 |
| 2019-01-01 01:00:00 | 20003 | 4.1 | 154.0 |
| 2019-01-01 01:00:00 | 20004 | 3.4 | 74.0 |

In order to ingest feature data into Feast for this specific feature set:

```python
# Load dataframe
customer_df = pd.read_csv("customer_data.csv")

# Create feature set from YAML (using YAML is optional)
cust_trans_fs = FeatureSet.from_yaml("customer_transactions_feature_set.yaml")

# Load feature data into Feast for this specific feature set
client.ingest(cust_trans_fs, customer_data)
```

### Feature

A feature is an individual measurable property or characteristic of a phenomenon being observed. Features are the most important concepts within a feature store. Feature data is used both as input to models during training and when models are served in production.

In the context of Feast, features are values that are associated with either one or more entities over time. In Feast, these values are either primitives or lists of primitives. Each feature can also have additional information attached to it. For example whether it is a categorical feature or numerical.

{% hint style="info" %}
Features in Feast are defined within Feature Sets and are not treated as standalone concepts.
{% endhint %}

### Entity

An entity type is any object in an organization that needs to be modeled and on which information should be stored. Entity types are usually recognizable concepts, either concrete or abstract, such as persons, places, things, or events which have relevance to the modeled system.

An entity is an instance of an entity type.

* Examples of entity types in the context of ride-hailing and food delivery: `customer`, `order`, `driver`, `restaurant`, `dish`, `area`.
* A specific driver, for example a driver with ID `D011234` would be an entity of the entity type `driver`

An entity is the object on which features are observed. For example we could have a feature `total_trips_24h` on the driver `D01123` with a feature value of `11`.

In the context of Feast, entities are important because they are used as keys when looking up feature values. Entities are also used when joining feature values between different feature sets in order to build one large data set to train a model, or to serve a model.

{% hint style="info" %}
Entities in Feast are defined within Feature Sets and are not treated as standalone concepts.
{% endhint %}

### Types

Feast supports the following types for feature values

* BYTES
* STRING
* INT32
* INT64
* DOUBLE
* FLOAT
* BOOL
* BYTES\_LIST
* STRING\_LIST
* INT32\_LIST
* INT64\_LIST
* DOUBLE\_LIST
* FLOAT\_LIST
* BOOL\_LIST

## Glossary

| Term | Description |
| :--- | :--- |
| Feast deployment | A complete Feast system as it is deployed. Consists out of a single Feast Core deployment and one or more Feast Serving deployments. |
| Feast Core | The centralized service which acts as a registry and authority of features. Organizations should only deploy a single Feast Core instance. Feast Core also manages the ingestion of feature data and population of Feast Serving data stores. |
| Feast Serving | Feast Serving is a service used to access both online and batch feature data. Feast Serving deployments are backed by one or more databases. |

