# Getting data into Feast

In order to retrieve features for both training and serving, Feast requires data being ingested into the offline and online stores.

{% hint style="warning" %}
Offline storage support will not be available until v0.9. Only Online storage support exists currently.
{% endhint %}

Users are expected to already have either a batch or stream source with data materialized in it, ready to be ingested into Feast. Upon providing their external data sources in feature table specifications and registering them, users can now ingest data into Feast using Spark jobs.

The following depicts an example ingestion flow from the specified data source to online store.

### Batch Source to Online Store

```python
from feast import Client
from datetime import datetime, timedelta

client = Client(core_url="localhost:6565")
driver_ft = client.get_feature_table("driver_trips")

# Initialize date ranges
today = datetime.now()
yesterday = today - timedelta(1)

client.start_offline_to_online_ingestion(
    driver_ft, yesterday, today
)
```

### Stream Source to Online Store

```python
from feast import Client
from datetime import datetime, timedelta

client = Client(core_url="localhost:6565")
driver_ft = client.get_feature_table("driver_trips")

client.start_stream_to_online_ingestion(driver_ft)
```

