# Feature Tables

## Overview

Feature tables are both a schema and a means of identifying data sources for features.

Data typically comes in the form of flat files, dataframes, tables in a database, or events on a stream. Thus, the data occurs with multiple columns/fields in multiple rows/events.

Feature tables are a way for defining the unique properties of these data sources, how Feast should interpret them, and how Feast should source them. Feature tables allow for groups of fields in these data sources to be [ingested](../user-guide/data-ingestion.md) and [stored](../advanced/stores.md) together. Feature tables allow for efficient storage and logical namespacing of data within [stores](../advanced/stores.md).

#### Features

A feature is an individual measurable property or characteristic of a phenomenon being observed. Features are the most important concepts within a feature store. Feature data is used both as input to models during training and when models are served in production.

In the context of Feast, features are values that are associated with either one or more entities over time. In Feast, these values are either primitives or lists of primitives. Each feature can also have additional information attached to it.

Defining a feature is as simple as providing a name and value type as shown below.

```python
avg_daily_ride = Feature("average_daily_rides", ValueType.FLOAT)
```

Please see the [FeatureSpec](https://api.docs.feast.dev/grpc/feast.core.pb.html#FeatureSpecV2) for the complete feature specification API.

{% hint style="info" %}
Feature tables are a grouping of features based on how they are loaded into Feast. They ensure that data is efficiently stored during ingestion. Feature tables are not a grouping of features for retrieval of features. During retrieval it is possible to retrieve feature values from any number of feature tables.
{% endhint %}

## Structure of a Feature Table

Users are allowed to exclude a stream source when registering a Feature table, since stream sources might not necessarily exist. However, at minimal, a batch source must be specified since data needs to be available somewhere for ingestion into Feast. 

When creating a feature table specification, the following fields should be considered:

* **name:** Name of feature table. Must be unique.
* **entities:** List of names of entities to associate with the features defined in this feature table. See [Entities](entities.md) page for more details.
* **features:** List of feature specifications for each feature defined with this feature table.
* **labels:** User-defined metadata.
* **max\_age:** Max age is measured as the duration of time between the feature's event timestamp when when the feature is retrieved. Feature values outside max age will be returned as unset values and indicated to end user.
* **batch\_source:** Batch/Offline data source to source batch/offline feature data. See [Sources](sources.md) page for more details.
* **stream\_source:** Stream/Online data source to source stream/online feature data. See [Sources](sources.md) page for more details.

A valid feature table specification is shown below:

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
    max_age=14400,
    labels={
      "team": "driver_matching" 
    },
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

Shown in the feature table specification above, the entity `driver_id` relates to an entity that has already been registered with Feast.

By default, Feast assumes that the features specified in the feature table specification is a 1-1 mapping to the fields found in the batch source. However, if fields in the batch source are named differently to the feature names, `field_mappings` can be utilised as shown above \(i.e `rating` field in batch source is to be mapped to `driver_rating` feature.

{% hint style="info" %}
When applying a Feature Table without specifying a project, Feast creates/updates the Feature Table in the`default` project. To create a Feature Table in another project, specify the project of choice in the`apply_feature_table` call.
{% endhint %}

## Working with a Feature Table

There are various functionality that comes with a Feature Table.

#### Creating a Feature Table

```python
driver_ft = FeatureTable(..., max_age=14400, ...)
client.apply_feature_table(driver_ft)
```

#### Updating a Feature Table

In order to facilitate the need for feature table definitions to change over time, a limited set of changes can be made to existing feature tables.

```python
driver_ft = FeatureTable(..., max_age=7200, ...)
client.apply_feature_table(driver_ft)
```

Permitted changes include:

* Adding new features.
* Deleting existing features \(note that features are tombstoned and remain on record, rather than removed completely; as a result, new features will not be able to take the names of these deleted features\).
* Changing the feature table's source, max age and labels.

Note that the following are **not** allowed:

* Changes to project or name of feature table.
* Changes to entities related to the feature table.
* Changes to names and types of existing features.

#### Deleting a Feature Table

{% hint style="danger" %}
Not supported yet in v0.8.
{% endhint %}

#### Updating Features

```python
# Adding a new Feature
driver_ft = FeatureTable(
    ...,
    max_age=7200,
    features=[
        ...,
        Feature("new_feature", ValueType.STRING)
    ]
    ...
)
client.apply_feature_table(driver_ft)

# Adding a new Feature (using add_feature call)
driver_ft.add_feature(Feature(name="new_feature", dtype=ValueType.STRING))
client.apply_feature_table(driver_ft)

# Removing a Feature (eg. Remove new_feature)
driver_ft = FeatureTable(
    ...,
    max_age=7200,
    features=[
        ...
    ]
    ...
)
client.apply_feature_table(driver_ft)
```

