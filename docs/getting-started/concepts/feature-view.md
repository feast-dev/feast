# Feature view

## Feature views

A feature view is an object that represents a logical group of time-series feature data as it is found in a [data source](data-source.md). Feature views consist of zero or more [entities](entity.md), one or more [features](feature-view.md#feature), and a [data source](data-source.md). Feature views allow Feast to model your existing feature data in a consistent way in both an offline (training) and online (serving) environment. Feature views generally contain features that are properties of a specific object, in which case that object is defined as an entity and included in the feature view. If the features are not related to a specific object, the feature view might not have entities; see [feature views without entities](feature-view.md#feature-views-without-entities) below.

{% tabs %}
{% tab title="driver_trips_feature_view.py" %}
```python
from feast import BigQuerySource, FeatureView, Field
from feast.types import Float32, Int64

driver_stats_fv = FeatureView(
    name="driver_activity",
    entities=["driver"],
    schema=[
        Field(name="trips_today", dtype=Int64),
        Field(name="rating", dtype=Float32),
    ],
    source=BigQuerySource(
        table="feast-oss.demo_data.driver_activity"
    )
)
```
{% endtab %}
{% endtabs %}

Feature views are used during

* The generation of training datasets by querying the data source of feature views in order to find historical feature values. A single training dataset may consist of features from multiple feature views.
* Loading of feature values into an online store. Feature views determine the storage schema in the online store. Feature values can be loaded from batch sources or from [stream sources](../../reference/data-sources/push.md).
* Retrieval of features from the online store. Feature views provide the schema definition to Feast in order to look up features from the online store.

{% hint style="info" %}
Feast does not generate feature values. It acts as the ingestion and serving system. The data sources described within feature views should reference feature values in their already computed form.
{% endhint %}

## Feature views without entities

If a feature view contains features that are not related to a specific entity, the feature view can be defined without entities (only event timestamps are needed for this feature view).

{% tabs %}
{% tab title="global_stats.py" %}
```python
from feast import BigQuerySource, FeatureView, Field
from feast.types import Int64

global_stats_fv = FeatureView(
    name="global_stats",
    entities=[],
    schema=[
        Field(name="total_trips_today_by_all_drivers", dtype=Int64),
    ],
    source=BigQuerySource(
        table="feast-oss.demo_data.global_stats"
    )
)
```
{% endtab %}
{% endtabs %}

## Feature inferencing

If the `features` parameter is not specified in the feature view creation, Feast will infer the features during `feast apply` by creating a feature for each column in the
underlying data source except the columns corresponding to the entities of the feature view or the columns corresponding to the timestamp columns of the feature view's
data source. The names and value types of the inferred features will use the names and data types of the columns from which the features were inferred.

## Entity aliasing

"Entity aliases" can be specified to join `entity_dataframe` columns that do not match the column names in the source table of a FeatureView.

This could be used if a user has no control over these column names or if there are multiple entities are a subclass of a more general entity. For example, "spammer" and "reporter" could be aliases of a "user" entity, and "origin" and "destination" could be aliases of a "location" entity as shown below.

It is suggested that you dynamically specify the new FeatureView name using `.with_name` and `join_key_map` override using `.with_join_key_map` instead of needing to register each new copy.

{% tabs %}
{% tab title="location_stats_feature_view.py" %}
```python
from feast import BigQuerySource, Entity, FeatureView, Field, ValueType
from feast.types import Int32

location = Entity(name="location", join_keys=["location_id"], value_type=ValueType.INT64)

location_stats_fv= FeatureView(
    name="location_stats",
    entities=["location"],
    schema=[
        Field(name="temperature", dtype=Int32)
    ],
    source=BigQuerySource(
        table="feast-oss.demo_data.location_stats"
    ),
)
```
{% endtab %}

{% tab title="temperatures_feature_service.py" %}
```python
from location_stats_feature_view import location_stats_fv

temperatures_fs = FeatureService(
    name="temperatures",
    features=[
        location_stats_fv
            .with_name("origin_stats")
            .with_join_key_map(
                {"location_id": "origin_id"}
            ),
        location_stats_fv
            .with_name("destination_stats")
            .with_join_key_map(
                {"location_id": "destination_id"}
            ),
    ],
)
```
{% endtab %}
{% endtabs %}

## Feature

A feature is an individual measurable property. It is typically a property observed on a specific entity, but does not have to be associated with an entity. For example, a feature of a `customer` entity could be the number of transactions they have made on an average month, while a feature that is not observed on a specific entity could be the total number of posts made by all users in the last month.

Features are defined as part of feature views. Since Feast does not transform data, a feature is essentially a schema that only contains a name and a type:

```python
from feast import Field
from feast.types import Float32

trips_today = Field(
    name="trips_today",
    dtype=Float32
)
```

Together with [data sources](data-source.md), they indicate to Feast where to find your feature values, e.g., in a specific parquet file or BigQuery table. Feature definitions are also used when reading features from the feature store, using [feature references](feature-retrieval.md#feature-references).

Feature names must be unique within a [feature view](feature-view.md#feature-view).

## \[Alpha] On demand feature views

On demand feature views allows users to use existing features and request time data (features only available at request time) to transform and create new features. Users define python transformation logic which is executed in both historical retrieval and online retrieval paths:

```python
from feast import Field, RequestSource
from feast.types import Float64

# Define a request data source which encodes features / information only
# available at request time (e.g. part of the user initiated HTTP request)
input_request = RequestSource(
    name="vals_to_add",
    schema=[
        Field(name="val_to_add", dtype=PrimitiveFeastType.INT64),
        Field(name="val_to_add_2": dtype=PrimitiveFeastType.INT64),
    ]
)

# Use the input data and feature view features to create new features
@on_demand_feature_view(
   sources=[
       driver_hourly_stats_view,
       input_request
   ],
   schema=[
     Field(name='conv_rate_plus_val1', dtype=Float64),
     Field(name='conv_rate_plus_val2', dtype=Float64)
   ]
)
def transformed_conv_rate(features_df: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df['conv_rate_plus_val1'] = (features_df['conv_rate'] + features_df['val_to_add'])
    df['conv_rate_plus_val2'] = (features_df['conv_rate'] + features_df['val_to_add_2'])
    return df
```
