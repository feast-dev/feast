# Feature Tables

Feature tables are both a schema and a means of identifying data sources for features.

Data typically comes in the form of flat files, dataframes, tables in a database, or events on a stream. Thus, the data occurs with multiple columns/fields in multiple rows/events.

Feature tables are a way for defining the unique properties of these data sources, how Feast should interpret them, and how Feast should source them. Feature tables allow for groups of fields in these data sources to be [ingested](../user-guide/data-ingestion.md) and [stored](../user-guide/stores.md) together. Feature tables allow for efficient storage and logical namespacing of data within [stores](../user-guide/stores.md).

{% hint style="info" %}
Feature tables are a grouping of features based on how they are loaded into Feast. They ensure that data is efficiently stored during ingestion. Feature tables are not a grouping of features for retrieval of features. During retrieval it is possible to retrieve feature values from any number of feature tables.
{% endhint %}

## Features

A feature is an individual measurable property or characteristic of a phenomenon being observed. Features are the most important concepts within a feature store. Feature data is used both as input to models during training and when models are served in production.

In the context of Feast, features are values that are associated with either one or more entities over time. In Feast, these values are either primitives or lists of primitives. Each feature can also have additional information attached to it.

Defining a feature is as simple as providing a name and value type as shown below.

```python
avg_daily_ride = Feature("average_daily_rides", ValueType.FLOAT)
```

Please see the [FeatureSpec](https://api.docs.feast.dev/grpc/feast.core.pb.html#FeatureSpecV2) for the complete feature specification API.

## Driver Trips Example

Below is an example specification of a basic `driver trips` feature table created using the Python SDK:

{% tabs %}
{% tab title="driver\_feature\_table.py" %}
```python
from feast import BigQuerySource, FeatureTable, Feature, ValueType

# Create an empty feature table
driver_ft = FeatureTable(
    name="driver_trips",
    entities=["driver_id"],
    features=[
      Feature("average_daily_rides", ValueType.FLOAT),
      Feature("rating", ValueType.FLOAT)
    ],
    batch_source=BigQuerySource(
        table_ref="gcp_project:bq_dataset.bq_table",
        event_timestamp_column="datetime",
        created_timestamp_column="timestamp",
        field_mapping={
          "rating": "driver_rating"
        }
    )
)
```
{% endtab %}
{% endtabs %}

Shown in the YAML specifications above, the entity `driver_id` relates to an entity that has already been registered with Feast.

By default, Feast assumes that the features specified in the YAML specification is a 1-1 mapping to the fields found in the batch source. However, if fields in the batch source are named differently to the feature names, `field_mappings` can be utilised as shown above \(i.e `rating` field in batch source is to be mapped to `driver_rating` feature.

{% hint style="info" %}
When applying a Feature Table without specifying a project, Feast creates/updates the Feature Table in the`default` project. To create a Feature Table in another project, specify the project of choice in the`apply_feature_table` call.
{% endhint %}

