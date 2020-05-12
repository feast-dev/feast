# Data ingestion

Users don't necessarily have to provide their external data sources in order to load data into Feast. Feast also allows users to exclude the source when registering a feature set. The following is a valid feature set specification.

```python
feature_set = FeatureSet(
    name="stream_feature",    
    entities=[Entity("entity_id", ValueType.INT64)],
    features=[Feature("feature_value1", ValueType.STRING)],
)
```

If a user does not provide a source of data then they are required to publish data to Feast. This process is called ingestion. 

## Ingesting data

The following example demonstrates how data ingestion works. For a full tutorial please see the [Telco Customer Churn Prediction Notebook](https://github.com/gojek/feast/blob/master/examples/feast-xgboost-churn-prediction-tutorial/Telecom%20Customer%20Churn%20Prediction%20%28with%20Feast%20and%20XGBoost%29.ipynb).

1. Connect to Feast Core and load in a Pandas DataFrame.

```python
from feast import FeatureSet, Client, Entity
import pandas as pd

# Connect to Feast core
client = Client(core_url="feast-core.example.com")

# Load in customer data
df = pd.read_csv("customer.csv")
```

2. Create, infer, and register a feature set from the DataFrame. This is a once off step that is required to initially register a feature set with Feast

```python
# Create an empty feature set
customer_churn_fs = FeatureSet("customer_churn")

# Infer the schema of the feature set from the Pandas DataFrame
customer_churn_fs.infer_fields_from_df(
            df,
            entities=[Entity(name='customer_id',
                             dtype=ValueType.STRING)]
        )

# Register the feature set with Feast
client.apply(customer_churn_fs)
```

3. We can also test that the feature set was correctly registered with Feast by retrieving it again and printing it out

```text
customer_churn_fs = client.get_feature_set('customer_churn')
print(client.get_feature_set('customer_churn'))
```

```yaml
{
  "spec": {
    "name": "customer_churn",
    "version": 1,
    "entities": [
      {
        "name": "customer_id",
        "valueType": "STRING"
      }
    ],
    "features": [
      {
        "name": "churn",
        "valueType": "INT64"
      },
      {
        "name": "contract_month_to_month",
        "valueType": "INT64"
      },
      {
        "name": "streamingmovies",
        "valueType": "INT64"
      },
      {
        "name": "paperlessbilling",
        "valueType": "INT64"
      },
      {
        "name": "contract_two_year",
        "valueType": "INT64"
      },
      {
        "name": "partner",
        "valueType": "INT64"
      }
    ],
    "maxAge": "0s",
    "source": {
      "type": "KAFKA",
      "kafkaSourceConfig": {
        "bootstrapServers": "10.202.250.99:31190",
        "topic": "feast"
      }
    },
    "project": "default"
  },
  "meta": {
    "createdTimestamp": "2020-03-15T07:47:52Z",
    "status": "STATUS_READY"
  }
}
```

Once we are happy that the schema is correct, we can start to ingest the DataFrame into Feast.

```text
client.ingest(customer_churn_fs, telcom)
```

```text
100%|██████████| 7032/7032 [00:02<00:00, 2771.19rows/s]
Ingestion complete!

Ingestion statistics:
Success: 7032/7032A rows ingested
```

{% hint style="warning" %}
Feast ingestion maintains the order of data that is ingested. This means that data that is written later will replace those that are written prior in stores. This is important to note when ingesting data that will end in a production system.
{% endhint %}



