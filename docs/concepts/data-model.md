# Data Model

### Concepts

The top-level namespace within Feast is a [project](data-model.md#project). Users define one or more [feature views](data-model.md#feature-view) within a project. Each feature view contains one or more [features](data-model.md#feature) that relate to a specific [entity](data-model.md#entity). A feature view must always have a [data source](data-model.md#data-source). This source is used during the generation of training [datasets](data-model.md#dataset) and when materializing feature values into the online store. 

![](../.gitbook/assets/image%20%287%29.png)

### Project

Projects provide complete isolation of feature stores at the infrastructure level. This is accomplished through resource namespacing, e.g., prefixing table names with the associated project. Each project should be considered a completely separate universe of entities and features. It is not possible to retrieve features from multiple projects in a single request. We recommend having a single project per environment \(`dev`, `staging`, `prod`\).

### Data Source

Feast uses a time-series data model to represent data. This data model is used to interpret feature data in data sources in order to build training datasets or when materializing features into an online store.

Below is an example data source with a single entity \(`driver`\) and two features \(`trips_today`, and `rating`\).

![Ride-hailing data source](../.gitbook/assets/image%20%2816%29.png)

### Entity

An entity is a collection of semantically related features. Users define entities to map to the domain of their use case. For example, a ride-hailing service could have customers and drivers as their entities, which group related features that correspond to these customers and drivers.

```python
driver = Entity(name='driver', value_type=ValueType.STRING, join_key='driver_id')
```

Entities are defined as part of feature views. Entities are used to identify the primary key on which feature values should be stored and retrieved. These keys are used during the lookup of feature values from the online store and the join process in point-in-time joins. It is possible to define composite entities \(more than one entity object\) in a feature view.

Entities should be reused across feature views.

### Feature

A feature is an individual measurable property observed on an entity. For example, a feature of a `customer` entity could be the number of transactions they have made on an average month. 

Features are defined as part of feature views. Since Feast does not transform data, a feature is essentially a schema that only contains a name and a type:

```python
trips_today = Feature(
    name="trips_today",
    dtype=ValueType.FLOAT
)
```

Together with [data sources](data-model.md#data-source), they indicate to Feast where to find your feature values, e.g., in a specific parquet file or BigQuery table. Feature definitions are also used when reading features from the feature store, using [feature references](data-model.md#feature-references).

Feature names must be unique within a [feature view](data-model.md#feature-view).

### Feature View

A feature view is an object that represents a logical group of time-series feature data as it is found in a data source. Feature views consist of one or more entities, features, and a data source. Feature views allow Feast to model your existing feature data in a consistent way in both an offline \(training\) and online \(serving\) environment.

{% tabs %}
{% tab title="driver\_trips\_feature\_view.py" %}
```python
driver_stats_fv = FeatureView(
    name="driver_activity",
    entities=["driver"],
    features=[
        Feature(name="trips_today", dtype=ValueType.INT64),
        Feature(name="rating", dtype=ValueType.FLOAT),
    ],
    input=BigQuerySource(
        table_ref="feast-oss.demo_data.driver_activity"
    )
)
```
{% endtab %}
{% endtabs %}

Feature views are used during

* The generation of training datasets by querying the data source of feature views in order to find historical feature values. A single training dataset may consist of features from multiple feature views.
* Loading of feature values into an online store. Feature views determine the storage schema in the online store.
* Retrieval of features from the online store. Feature views provide the schema definition to Feast in order to look up features from the online store.

{% hint style="info" %}
Feast does not generate feature values. It acts as the ingestion and serving system. The data sources described within feature views should reference feature values in their already computed form.
{% endhint %}

### Dataset

A dataset is a collection of rows that is produced by a historical retrieval from Feast in order to train a model. A dataset is produced by a join from one or more feature views onto an entity dataframe. Therefore, a dataset may consist of features from multiple feature views.

**Dataset vs Feature View:** Feature views contain the schema of data and a reference to where data can be found \(through its data source\). Datasets are the actual data manifestation of querying those data sources.

**Dataset vs Data Source:** Datasets are the output of historical retrieval, whereas data sources are the inputs. One or more data sources can be used in the creation of a dataset.

### Feature References

Feature references uniquely identify feature values in Feast. The structure of a feature reference in string form is as follows: `<feature_table>:<feature>`  

Feature references are used for the retrieval of features from Feast:

```python
online_features = fs.get_online_features(
    feature_refs=[
        'driver_locations:lon',
        'drivers_activity:trips_today'
    ],
    entities=[{'driver': 'driver_1001'}]
)
```

It is possible to retrieve features from multiple feature views with a single request, and Feast is able to join features from multiple tables in order to build a training dataset. However, It is not possible to reference \(or retrieve\) features from multiple projects at the same time.

### **Entity key**

Entity keys are one or more entity values that uniquely describe an entity. In the case of an entity \(like a `driver`\) that only has a single entity field, the entity _is_ an entity key. However, it is also possible for an entity key to consist of multiple entity values. For example, a feature view with the composite entity of \(customer, country\) might have an entity key of \(1001, 5\). 

![](../.gitbook/assets/image%20%2815%29.png)

Entity keys act as primary keys. They are used during the lookup of features from the online store, and they are also used to match feature rows across feature views during point-in-time joins.

### Event timestamp

The timestamp on which an event occurred, as found in a feature view's data source. The entity timestamp describes the event time at which a feature was observed or generated. 

Event timestamps are used during point-in-time joins to ensure that the latest feature values are joined from feature views onto entity rows. Event timestamps are also used to ensure that old feature values aren't served to models during online serving.

### Entity row

An entity key at a specific point in time.

![](../.gitbook/assets/image%20%2811%29.png)

### Entity dataframe

A collection of entity rows. Entity dataframes are the "left table" that is enriched with feature values when building training datasets. The entity dataframe is provided to Feast by users during historical retrieval:

```python
training_df = store.get_historical_features(
    entity_df=entity_df, 
    feature_refs = [
        'drivers_activity:trips_today'
        'drivers_activity:rating'
    ],
)
```

Example of an entity dataframe with feature values joined to it:

![](../.gitbook/assets/image%20%2817%29.png)

### **Online Store** 

The Feast online store is used for low-latency online feature value lookups. Feature values are loaded into the online store from data sources in feature views. The data model within the online store maps directly to that of the data source. One key difference with the online store is that only the latest feature values are stored per entity key. No historical values are stored.

Example batch data source

![](../.gitbook/assets/image%20%286%29.png)

Once the above data source is materialized into Feast \(using `feast materialize`\), the feature values will be represented as follows:

![](../.gitbook/assets/image%20%285%29.png)

\*\*\*\*

