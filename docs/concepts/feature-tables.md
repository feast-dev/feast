# Feature Tables

## Overview

Feature tables are both a schema and a logical means of grouping features, data [sources](sources.md), and other related metadata.

Data typically comes in the form of flat files, dataframes, tables in a database, or events on a stream. Thus, the data is presented in multiple columns and/or fields in multiple rows and/or events.

Feature tables define the unique properties of these data [sources](sources.md), including how Feast interprets and sources them. Feature tables also enable Feast to[ ingest](../user-guide/data-ingestion.md) and [store](../advanced/stores.md) groups of fields from these data sources. Additionally, feature tables introduce efficient storage and logical namespacing to data within [stores](../advanced/stores.md).

### Features

A feature is an individual measurable property or characteristic of an observable phenomenon. For example, in a bank a feature could be `total_foreign_transactions_24h` for a specific class of credit cards the bank issues. Feature data is the input both for training models, and for models served in production. 

{% hint style="info" %}
Features are the most important concepts within a feature store.
{% endhint %}

In Feast, features are values that are associated with one or more [entities](entities.md). These values are either primitives or lists of primitives. Each feature can also have additional information attached to it.

You define a feature by providing a name and value type. In our example, we use a name and value type that might be used in a ride-hailing company:

```python
avg_daily_ride = Feature("average_daily_rides", ValueType.FLOAT)
```

Visit [FeatureSpec](https://api.docs.feast.dev/grpc/feast.core.pb.html#FeatureSpecV2) for the complete feature specification API.

{% hint style="warning" %}
Feature tables are **not** a source for retrieving features. Rather, you can retrieve feature values from feature tables.
{% endhint %}

## Structure of a Feature Table

When you register a feature table, at a minimum specify a batch source to populate the feature table. 

If your use case includes a stream source, specify the stream source when you register the feature table. However, because a stream source may not be available in all use cases, Feast does not require specifying one.

When you create a feature-table specification, include the following fields:

* **name:** Name of feature table. This name must be unique.
* **entities:** List of names of entities to associate with the features defined in this feature table. Visit [Entities](entities.md) to learn more about them.
* **features:** List of feature specifications for each feature defined in this feature table.
* **labels:** User-defined metadata.
* **max\_age:** Max age is measured as the duration of time between the feature's event timestamp and its retrieval. Feature values outside max age will be returned as unset values and indicated to you.
* **batch\_source:** The batch or offline data source from which you create batch source feature data. Visit [Sources](sources.md) to learn more about them.
* **stream\_source:** The stream or online data source from which you create stream or online feature data. See [Sources](sources.md) page for more details.

Here is an example of a valid feature-table specification. In our example, we again use a ride-hailing company:

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

By default, Feast assumes that features specified in the feature-table specification corresponds 1 - 1 to the fields found in the batch source. 

However, if the names of the fields in the batch source are different from the names of features, you can use `field_mappings` to ensure the names correspond. 

In the example feature-specification table above, we use `field_mappings` to ensure the field named `rating` in the batch source is mapped to the feature named `driver_rating`.  


{% hint style="info" %}
When applying a feature table without specifying a project, Feast either creates or updates the feature table in the `default` project. 

To create a feature table for another project, specify the project of choice in the `apply_feature_table` call.
{% endhint %}

## Working with a Feature Table

Feature tables include a rich set of functions.

#### Creating a Feature Table

```python
driver_ft = FeatureTable(..., max_age=14400, ...)
client.apply_feature_table(driver_ft)
```

#### Updating a Feature Table

Feature table definitions may need to change over time to reflect more accurately your use case. In our ride-hailing example below, we update the max age:

```python
driver_ft = FeatureTable(..., max_age=7200, ...)
client.apply_feature_table(driver_ft)
```

Feast currently supports the following:

* Adding new features.
* Deleting existing features
* Changing the feature table's source, max age, and labels.

{% hint style="warning" %}
Deleted features are archived, rather than removed completely. Importantly, new features **cannot** use the names of these deleted features.
{% endhint %}

Feast currently does **not** support the following:

* Changes to the project or name of a feature table.
* Changes to entities related to a feature table.
* Changes to names and types of existing features.

#### Deleting a Feature Table

{% hint style="danger" %}
Feast currently does not support deleting a feature table.
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

{% hint style="info" %}
Quick summary of Feature Tables:

* They are both a schema and a way of grouping features, data [sources](sources.md), and other related metadata.
* They define the unique properties of the data within data [sources](sources.md).
* They enable Feast to[ ingest](../user-guide/data-ingestion.md) and [store](../advanced/stores.md) groups of fields from these data sources
* They ensure data is efficiently stored during [ingestion](../user-guide/data-ingestion.md).
{% endhint %}

