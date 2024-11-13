# Feature view

## Feature views

{% hint style="warning" %}
**Note**: Feature views do not work with non-timestamped data. A workaround is to insert dummy timestamps.
{% endhint %}

A **feature view** is defined as a *collection of features*. 

- In the online settings, this is a *stateful* collection of 
features that are read when the `get_online_features` method is called. 
- In the offline setting, this is a *stateless* collection of features that are created when the `get_historical_features` 
method is called. 

A feature view is an object representing a logical group of time-series feature data as it is found in a [data source](data-ingestion.md). Depending on the kind of feature view, it may contain some lightweight (experimental) feature transformations (see [\[Beta\] On demand feature views](../../reference/beta-on-demand-feature-view.md)).

Feature views consist of:

* a [data source](data-ingestion.md)
* zero or more [entities](entity.md)
  * If the features are not related to a specific object, the feature view might not have entities; see [feature views without entities](feature-view.md#feature-views-without-entities) below.
* a name to uniquely identify this feature view in the project.
* (optional, but recommended) a schema specifying one or more [features](feature-view.md#field) (without this, Feast will infer the schema by reading from the data source)
* (optional, but recommended) metadata (for example, description, or other free-form metadata via `tags`)
* (optional) a TTL, which limits how far back Feast will look when generating historical datasets

Feature views allow Feast to model your existing feature data in a consistent way in both an offline (training) and online (serving) environment. Feature views generally contain features that are properties of a specific object, in which case that object is defined as an entity and included in the feature view.

{% tabs %}
{% tab title="driver_trips_feature_view.py" %}
```python
from feast import BigQuerySource, Entity, FeatureView, Field
from feast.types import Float32, Int64

driver = Entity(name="driver", join_keys=["driver_id"])

driver_stats_fv = FeatureView(
    name="driver_activity",
    entities=[driver],
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

## Feature views without entities

If a feature view contains features that are not related to a specific entity, the feature view can be defined without entities (only timestamps are needed for this feature view).

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

If the `schema` parameter is not specified in the creation of the feature view, Feast will infer the features during `feast apply` by creating a `Field` for each column in the underlying data source except the columns corresponding to the entities of the feature view or the columns corresponding to the timestamp columns of the feature view's data source. The names and value types of the inferred features will use the names and data types of the columns from which the features were inferred.

## Entity aliasing

"Entity aliases" can be specified to join `entity_dataframe` columns that do not match the column names in the source table of a FeatureView.

This could be used if a user has no control over these column names or if there are multiple entities are a subclass of a more general entity. For example, "spammer" and "reporter" could be aliases of a "user" entity, and "origin" and "destination" could be aliases of a "location" entity as shown below.

It is suggested that you dynamically specify the new FeatureView name using `.with_name` and `join_key_map` override using `.with_join_key_map` instead of needing to register each new copy.

{% tabs %}
{% tab title="location_stats_feature_view.py" %}
```python
from feast import BigQuerySource, Entity, FeatureView, Field
from feast.types import Int32, Int64

location = Entity(name="location", join_keys=["location_id"])

location_stats_fv= FeatureView(
    name="location_stats",
    entities=[location],
    schema=[
        Field(name="temperature", dtype=Int32),
        Field(name="location_id", dtype=Int64),
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

## Field

A field or feature is an individual measurable property. It is typically a property observed on a specific entity, but does not have to be associated with an entity. For example, a feature of a `customer` entity could be the number of transactions they have made on an average month, while a feature that is not observed on a specific entity could be the total number of posts made by all users in the last month. Supported types for fields in Feast can be found in `sdk/python/feast/types.py`.

Fields are defined as part of feature views. Since Feast does not transform data, a field is essentially a schema that only contains a name and a type:

```python
from feast import Field
from feast.types import Float32

trips_today = Field(
    name="trips_today",
    dtype=Float32
)
```

Together with [data sources](data-ingestion.md), they indicate to Feast where to find your feature values, e.g., in a specific parquet file or BigQuery table. Feature definitions are also used when reading features from the feature store, using [feature references](feature-retrieval.md#feature-references).

Feature names must be unique within a [feature view](feature-view.md#feature-view).

Each field can have additional metadata associated with it, specified as key-value [tags](https://rtd.feast.dev/en/master/feast.html#feast.field.Field).

## \[Alpha] On demand feature views

On demand feature views allows data scientists to use existing features and request time data (features only available at request time) to transform and create new features. Users define python transformation logic which is executed in both the historical retrieval and online retrieval paths.

Currently, these transformations are executed locally. This is fine for online serving, but does not scale well to offline retrieval.

### Why use on demand feature views?

This enables data scientists to easily impact the online feature retrieval path. For example, a data scientist could

1. Call `get_historical_features` to generate a training dataframe
2. Iterate in notebook on feature engineering in Pandas
3. Copy transformation logic into on demand feature views and commit to a dev branch of the feature repository
4. Verify with `get_historical_features` (on a small dataset) that the transformation gives expected output over historical data
5. Verify with `get_online_features` on dev branch that the transformation correctly outputs online features
6. Submit a pull request to the staging / prod branches which impact production traffic

```python
from feast import Field, RequestSource
from feast.on_demand_feature_view import on_demand_feature_view
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

## \[Alpha] Stream feature views

A stream feature view is an extension of a normal feature view. The primary difference is that stream feature views have both stream and batch data sources, whereas a normal feature view only has a batch data source.

Stream feature views should be used instead of normal feature views when there are stream data sources (e.g. Kafka and Kinesis) available to provide fresh features in an online setting. Here is an example definition of a stream feature view with an attached transformation:

```python
from datetime import timedelta

from feast import Field, FileSource, KafkaSource, stream_feature_view
from feast.data_format import JsonFormat
from feast.types import Float32

driver_stats_batch_source = FileSource(
    name="driver_stats_source",
    path="data/driver_stats.parquet",
    timestamp_field="event_timestamp",
)

driver_stats_stream_source = KafkaSource(
    name="driver_stats_stream",
    kafka_bootstrap_servers="localhost:9092",
    topic="drivers",
    timestamp_field="event_timestamp",
    batch_source=driver_stats_batch_source,
    message_format=JsonFormat(
        schema_json="driver_id integer, event_timestamp timestamp, conv_rate double, acc_rate double, created timestamp"
    ),
    watermark_delay_threshold=timedelta(minutes=5),
)

@stream_feature_view(
    entities=[driver],
    ttl=timedelta(seconds=8640000000),
    mode="spark",
    schema=[
        Field(name="conv_percentage", dtype=Float32),
        Field(name="acc_percentage", dtype=Float32),
    ],
    timestamp_field="event_timestamp",
    online=True,
    source=driver_stats_stream_source,
)
def driver_hourly_stats_stream(df: DataFrame):
    from pyspark.sql.functions import col

    return (
        df.withColumn("conv_percentage", col("conv_rate") * 100.0)
        .withColumn("acc_percentage", col("acc_rate") * 100.0)
        .drop("conv_rate", "acc_rate")
    )
```

See [here](https://github.com/feast-dev/streaming-tutorial) for a example of how to use stream feature views to register your own streaming data pipelines in Feast.
