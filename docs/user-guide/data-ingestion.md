# Data ingestion

Users are required provide their external data sources in order to load data into Feast. Feast only allows users to exclude a stream source when registering a Feature table. However, at minimal, a batch source must be specified. A valid Feature table specification can be found [here](../concepts/feature-tables.md#customer-transactions-example).

If a user has provided a batch source with no data currently, Feast supports ingesting into the specified batch source.

### Ingesting data

#### _Writing data into offline store_

The following example demonstrates how data ingestion works. For a full tutorial please refer to [Tutorials](https://github.com/feast-dev/feast/tree/master/examples) section.

1. Connect to Feast Core and load/create a Pandas DataFrame.
2. Create and register a feature table. This is a one-off step that is required to initially register a feature table with Feast.
3. Ingest data into offline store.

The DataFrame below \(`driver_data.csv`\) contains the features and entities of the above feature table.

| datetime | driver\_id | average\_daily\_rides | driver\_rating |
| :--- | :--- | :--- | :--- |
| 2019-01-01 01:00:00 | 1001 | 5.0 | 14.0 |
| 2019-01-01 01:00:00 | 1002 | 2.6 | 43.0 |
| 2019-01-01 01:00:00 | 1003 | 4.1 | 154.0 |
| 2019-01-01 01:00:00 | 1004 | 3.4 | 74.0 |

```python
from feast import BigQuerySource, Client, FeatureTable, ValueType
import pandas as pd

client = Client(core_url="localhost:6565")

# 1) Create pandas dataframe
ft_df = pd.DataFrame(
            {
                "datetime": [pd.datetime.now()],
                "driver_id": [1001],
                "rating": [4.3],
            }
        )

# 2) Load pandas dataframe
ft_df = pd.read_csv("driver_data.csv")

# Create an empty feature table
driver_ft = FeatureTable(
    name="driver_trips",
    entities=["driver_id"],
    features=[Feature("rating", ValueType.FLOAT)],
    batch_source=BigQuerySource(
        table_ref="gcp_project:bq_dataset.bq_table",
        event_timestamp_column="datetime",
        created_timestamp_column="timestamp",
    )
)

# Register the feature table with Feast
client.apply_feature_table(driver_ft)
```

To ensure that the feature table was correctly registered with Feast, the user can retrieve it and print it out again.

```python
driver_trips_ft = client.get_feature_table("driver_trips")
print(driver_trips_ft)
```

```yaml
{
  "spec": {
    "name": "driver_trips",
    "entities": [
      {
        "name": "driver_id",
        "valueType": "INT64"
      }
    ],
    "features": [
      {
        "name": "average_daily_rides",
        "valueType": "FLOAT"
      },
      {
        "name": "rating",
        "valueType": "FLOAT"
      },
      {
        "name": "maximum_daily_rides",
        "valueType": "INT64"
      }
    ],
    "maxAge": "0s",
    "batchSource": {
      "type": "BATCH_BIGQUERY",
      "event_timestamp_column": "event_timestamp",
      "created_timestamp_column": "created_timestamp",
      "bigqueryOptions": {
        "table_ref": "gcp_project:bq_dataset.bq_table",
      }
    )
    }
    "streamSource": {
      "type": "STREAM_KAFKA",
      "kafkaOptions": {
        "bootstrapServers": "10.202.250.99:31190",
        "classPath": "path/to/generated/protobuf/class",
        "topic": "feast"
      }
    }
  },
  "meta": {
    "createdTimestamp": "2020-03-15T07:47:52Z",
  }
}
```

Once we are happy that the schema is correct, we can start to ingest the DataFrame into Feast.

```python
# Ingest into offline store
client.ingest(driver_ft, ft_df)
```

{% hint style="warning" %}
Feast ingestion maintains the order of data that is ingested. This means that data that is written later will replace those that are written prior in stores. This is important to note when ingesting data that will end in a production system.
{% endhint %}

