# Feature view

## Feature View

A feature view is an object that represents a logical group of time-series feature data as it is found in a [data source](data-source.md). Feature views consist of one or more [entities](entity.md), [features](feature-view.md#feature), and a [data source](data-source.md). Feature views allow Feast to model your existing feature data in a consistent way in both an offline \(training\) and online \(serving\) environment.

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

## Feature

A feature is an individual measurable property observed on an entity. For example, a feature of a `customer` entity could be the number of transactions they have made on an average month.

Features are defined as part of feature views. Since Feast does not transform data, a feature is essentially a schema that only contains a name and a type:

```python
trips_today = Feature(
    name="trips_today",
    dtype=ValueType.FLOAT
)
```

Together with [data sources](data-source.md), they indicate to Feast where to find your feature values, e.g., in a specific parquet file or BigQuery table. Feature definitions are also used when reading features from the feature store, using [feature references](feature-retrieval.md#feature-references).

Feature names must be unique within a [feature view](feature-view.md#feature-view).

