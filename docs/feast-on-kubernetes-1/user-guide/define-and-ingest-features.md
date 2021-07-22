# Define and ingest features

In order to retrieve features for both training and serving, Feast requires data being ingested into its offline and online stores.

Users are expected to already have either a batch or stream source with data stored in it, ready to be ingested into Feast. Once a feature table \(with the corresponding sources\) has been registered with Feast, it is possible to load data from this source into stores.

The following depicts an example ingestion flow from a data source to the online store.

## Batch Source to Online Store

```python
from feast import Client
from datetime import datetime, timedelta

client = Client(core_url="localhost:6565")
driver_ft = client.get_feature_table("driver_trips")

# Initialize date ranges
today = datetime.now()
yesterday = today - timedelta(1)

# Launches a short-lived job that ingests data over the provided date range.
client.start_offline_to_online_ingestion(
    driver_ft, yesterday, today
)
```

## Stream Source to Online Store

```python
from feast import Client
from datetime import datetime, timedelta

client = Client(core_url="localhost:6565")
driver_ft = client.get_feature_table("driver_trips")

# Launches a long running streaming ingestion job
client.start_stream_to_online_ingestion(driver_ft)
```

## Batch Source to Offline Store

{% hint style="danger" %}
Not supported in Feast 0.8
{% endhint %}

## Stream Source to Offline Store

{% hint style="danger" %}
Not supported in Feast 0.8
{% endhint %}

