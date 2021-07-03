# Feature Tables

## Overview

Feature tables are both a schema and a logical means of grouping features, data [sources](sources.md), and other related metadata.

Feature tables serve the following purposes:

* Feature tables are a means for defining the location and properties of data [sources](sources.md).
* Feature tables are used to create within Feast a database-level structure for the storage of feature values.
* The data sources described within feature tables allow Feast to find and ingest feature data into stores within Feast.
* Feature tables ensure data is efficiently stored during [ingestion](../user-guide/define-and-ingest-features.md) by providing a grouping mechanism of features values that occur on the same event timestamp.

{% hint style="info" %}
Feast does not yet apply feature transformations. Transformations are currently expected to happen before data is ingested into Feast. The data sources described within feature tables should reference feature values in their already transformed form.
{% endhint %}

### Features

A feature is an individual measurable property observed on an entity. For example the amount of transactions \(feature\) a customer \(entity\) has completed. Features are used for both model training and scoring \(batch, online\).

Features are defined as part of feature tables. Since Feast does not apply transformations, a feature is basically a schema that only contains a name and a type:

```python
avg_daily_ride = Feature("average_daily_rides", ValueType.FLOAT)
```

Visit [FeatureSpec](https://api.docs.feast.dev/grpc/feast.core.pb.html#FeatureSpecV2) for the complete feature specification API.

## Structure of a Feature Table

Feature tables contain the following fields:

* **Name:** Name of feature table. This name must be unique within a project.
* **Entities:** List of [entities](entities.md) to associate with the features defined in this feature table. Entities are used as lookup keys when retrieving features from a feature table.
* **Features:** List of features within a feature table.
* **Labels:** Labels are arbitrary key-value properties that can be defined by users.
* **Max age:** Max age affect the retrieval of features from a feature table. Age is measured as the duration of time between the event timestamp of a feature and the lookup time on an [entity key]() used to retrieve the feature. Feature values outside max age will be returned as unset values. Max age allows for eviction of keys from online stores and limits the amount of historical scanning required for historical feature values during retrieval.
* **Batch Source:** The batch data source from which Feast will ingest feature values into stores. This can either be used to back-fill stores before switching over to a streaming source, or it can be used as the primary source of data for a feature table. Visit [Sources](sources.md) to learn more about batch sources.
* **Stream Source:** The streaming data source from which you can ingest streaming feature values into Feast. Streaming sources must be paired with a batch source containing the same feature values. A streaming source is only used to populate online stores. The batch equivalent source that is paired with a streaming source is used during the generation of historical feature datasets. Visit [Sources](sources.md) to learn more about stream sources.

Here is a ride-hailing example of a valid feature table specification:

{% tabs %}
{% tab title="driver\_trips\_feature\_table.py" %}
```python
from feast import BigQuerySource, FeatureTable, Feature, ValueType
from google.protobuf.duration_pb2 import Duration

driver_ft = FeatureTable(
    name="driver_trips",
    entities=["driver_id"],
    features=[
      Feature("average_daily_rides", ValueType.FLOAT),
      Feature("rating", ValueType.FLOAT)
    ],
    max_age=Duration(seconds=3600),
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

By default, Feast assumes that features specified in the feature-table specification corresponds one-to-one to the fields found in the sources. All features defined in a feature table should be available in the defined sources.

Field mappings can be used to map features defined in Feast to fields as they occur in data sources.

In the example feature-specification table above, we use field mappings to ensure the feature named `rating` in the batch source is mapped to the field named `driver_rating`.

## Working with a Feature Table

#### Creating a Feature Table

```python
driver_ft = FeatureTable(...)
client.apply(driver_ft)
```

#### Updating a Feature Table

```python
driver_ft = FeatureTable()

client.apply(driver_ft)

driver_ft.labels = {"team": "marketplace"}

client.apply(driver_ft)
```

#### Feast currently supports the following changes to feature tables:

* Adding new features.
* Removing features.
* Updating source, max age, and labels.

{% hint style="warning" %}
Deleted features are archived, rather than removed completely. Importantly, new features cannot use the names of these deleted features.
{% endhint %}

#### Feast currently does not support the following changes to feature tables:

* Changes to the project or name of a feature table.
* Changes to entities related to a feature table.
* Changes to names and types of existing features.

#### Deleting a Feature Table

{% hint style="danger" %}
Feast currently does not support the deletion of feature tables.
{% endhint %}

