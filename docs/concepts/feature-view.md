# Feature view

### Feature View

A feature view is an object that represents a logical group of time-series feature data as it is found in a [data source](feature-view.md#data-source). Feature views consist of one or more [entities](feature-view.md#entity), [features](feature-view.md#feature), and a [data source](feature-view.md#data-source). Feature views allow Feast to model your existing feature data in a consistent way in both an offline \(training\) and online \(serving\) environment.

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
    batch_source=BigQuerySource(
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

Together with [data sources](data-model-and-concepts.md#data-source), they indicate to Feast where to find your feature values, e.g., in a specific parquet file or BigQuery table. Feature definitions are also used when reading features from the feature store, using [feature references](data-model-and-concepts.md#feature-references).

Feature names must be unique within a [feature view](data-model-and-concepts.md#feature-view).

