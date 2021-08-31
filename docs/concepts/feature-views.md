# Feature Views

### Overview

Feature views are objects used to define and productionize logical groups of features for training and serving.

Feature views serve the following purposes:

* Feature views are a means for defining the location and properties of data sources that contain features.
* The data sources described within feature views allow Feast to find and materialize feature data into stores.
* Feature views ensure data is efficiently stored during materialization by providing a grouping mechanism of feature values that occur on the same event timestamp.
* Features are referenced relative to their feature view during the lookup of features, e.g., `driver_feature_view:driver_rating`.

{% hint style="info" %}
Feast does not yet apply feature transformations. Feast acts as the productionization layer for pre-existing features. The data sources described within feature views should reference feature values in their already transformed form.
{% endhint %}

Entities, features, and sources must be defined in order to define a feature view.

### Entity

Define an entity for the driver. Entities can be thought of as primary keys used to retrieve features. Entities are also used to join multiple tables/views during the construction of feature vectors.

```python
driver = Entity(
    # Name of the entity. Must be unique within a project
    name="driver",

    # The join key of an entity describes the storage level field/column on which
    # features can be looked up. The join key is also used to join feature 
    # tables/views when building feature vectors
    join_key="driver_id",

    # The storage level type for an entity
    value_type=ValueType.INT64
)
```

### Feature

A feature is an individual measurable property observed on an entity. For example, the amount of transactions \(feature\) a customer \(entity\) has completed. 

Features are defined as part of feature views. Since Feast does not transform data, a feature is essentially a schema that only contains a name and a type:

```python
conversion_rate = Feature(
    # Name of the feature. Used during lookup of feautres from the feature store
    # The name must be unique
    name="conv_rate",
 
    # The type used for storage of features (both at source and when materialized
    # into a store)
    dtype=ValueType.FLOAT
)
```

### Source

Indicates a data source from which feature values can be retrieved. Sources are queried when building training datasets or materializing features into an online store.

```python

driver_stats_source = BigQuerySource(
    # The BigQuery table where features can be found
    table_ref="feast-oss.demo_data.driver_stats",
    
    # The event timestamp is used for point-in-time joins and for ensuring only
    # features within the TTL are returned
    event_timestamp_column="datetime",
    
    # The (optional) created timestamp is used to ensure there are no duplicate
    # feature rows in the offline store or when building training datasets
    created_timestamp_column="created",
)
```

### Feature View

A Feature View is a 

{% tabs %}
{% tab title="driver\_trips\_feature\_table.py" %}
```python
driver_stats_fv = FeatureView(
    # The unique name of this feature view. Two feature views in a single
    # project cannot have the same name
    name="driver_stats",

    # The list of entities specifies the keys required for joining or looking
    # up features from this feature view. The reference provided in this field
    # correspond to the name of a defined entity (or entities)
    entities=["driver"],

    # The timedelta is the maximum age that each feature value may have
    # relative to its lookup time. For historical features (used in training),
    # TTL is relative to each timestamp provided in the entity dataframe.
    # TTL also allows for eviction of keys from online stores and limits the
    # amount of historical scanning required for historical feature values
    # during retrieval
    ttl=timedelta(weeks=1),

    # The list of features defined below act as a schema to both define features
    # for both materialization of features into a store, and are used as references
    # during retrieval for building a training dataset or serving features
    features=[
        Feature(name="conv_rate", dtype=ValueType.FLOAT),
        Feature(name="acc_rate", dtype=ValueType.FLOAT),
        Feature(name="avg_daily_trips", dtype=ValueType.INT64),
    ],

    # Batch sources are used to find feature values. In the case of this feature
    # view we will query a source table on BigQuery for driver statistics
    # features
    batch_source=driver_stats_source,

    # Tags are user defined key/value pairs that are attached to each
    # feature view
    tags={"team": "driver_performance"},
)
```
{% endtab %}
{% endtabs %}

