# Feature Tables

## Overview

Feature tables are both a schema and a logical means of grouping features, data [sources](sources.md), and other related metadata.

Feature tables serve the following purposes:

* They are a means for defining the location and properties of data [sources](sources.md).
* They are used to create within Feast a database-level structure for the storage of feature values.
* The data sources described within feature tables enable Feast to ingest and store features within Feast.
* They ensure data is efficiently stored during [ingestion](../user-guide/loading-data-into-feast.md).

{% hint style="info" %}
Feast does not yet apply feature transformations. Transformations are currently expected to happen before data is ingested into Feast. The data sources described within feature tables should reference feature values in their already computed form.
{% endhint %}

### Features

A feature is an individual measurable property or characteristic of an observable phenomenon. For example, in a bank, a feature could be `total_foreign_transactions_24h` for a specific class of credit cards the bank issues. Feature data is the input both for training models, and for models served in production. 

{% hint style="info" %}
Features are the most important concepts within a feature store.
{% endhint %}

In Feast, features are values that are associated with one or more [entities](entities.md). These values are either primitives or lists of primitives. Each feature can also have additional information attached to it.

You define a feature by providing a name and value type. In our example, we use a name and value type that might be used in a ride-hailing company:

```python
avg_daily_ride = Feature("average_daily_rides", ValueType.FLOAT)
```

Features act purely as a schema within feature tables. Feature tables and features act as normal database tables and columns.

Visit [FeatureSpec](https://api.docs.feast.dev/grpc/feast.core.pb.html#FeatureSpecV2) for the complete feature specification API.

## Structure of a Feature Table

Feature tables contain the following fields:

* **Name:** Name of feature table. This name must be unique within a project.
* **Entities:** List of [entities](entities.md) to associate with the features defined in this feature table. Entities are used as lookup keys when retrieving features from a feature table.
* **Features:** List of features within this feature table.
* **Labels:** Labels are arbitrary key-value properties that can be defined by users.
* **Max age:** Max age affect the retrieval of features from a feature table. Age is measured as the duration of time between the event timestamp of a feature and the lookup time on an entity key used to retrieve the feature. Feature values outside max age will be returned as unset values. Max age allows for eviction of keys from online stores and limits the amount of scanning for historical feature values during retrieval.
* **Batch Source:** The batch data source from which you can ingest feature values into Feast. Visit [Sources](sources.md) to learn more about them.
* **Stream Source:** The streaming data source from which you can ingest streaming feature values into Feast. Visit [Sources](sources.md) to learn more about them.

Here is a ride-hailing example of a valid feature-table specification:

{% tabs %}
{% tab title="driver\_trips\_feature\_table.py" %}
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

When you register a feature table, at a minimum specify a batch source to populate the feature table. Stream sources are optional. They are used to stream feature values into online stores.

By default, Feast assumes that features specified in the feature-table specification corresponds one-to-one to the fields found in the sources. All features defined in a feature table should be available in the defined sources.

However, if the names of the fields in the batch source are different from the names of features, you can use `field_mappings` to ensure the names correspond. 

In the example feature-specification table above, we use `field_mappings` to ensure the field named `rating` in the batch source is mapped to the feature named `driver_rating`.

## Working with a Feature Table

#### Creating a Feature Table

```python
driver_ft = FeatureTable(...)
client.apply_feature_table(driver_ft)
```

#### Updating a Feature Table

Feature table definitions may need to change over time to reflect more accurately your use case. In our ride-hailing example below, we update the max age:

```python
driver_ft = FeatureTable()

client.apply_feature_table(driver_ft)

driver_ft.labels = {"team": "marketplace"}

client.apply_feature_table(driver_ft)
```

Feast currently supports the following changes to feature tables:

* Adding new features.
* Deleting existing features
* Changing the feature table's source, max age, and labels.

{% hint style="warning" %}
Deleted features are archived, rather than removed completely. Importantly, new features cannot use the names of these deleted features.
{% endhint %}

Feast currently does not support the following changes to feature tables:

* Changes to the project or name of a feature table.
* Changes to entities related to a feature table.
* Changes to names and types of existing features.

#### Deleting a Feature Table

{% hint style="danger" %}
Feast currently does not support the deletion of feature tables.
{% endhint %}

