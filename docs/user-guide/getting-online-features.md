# Getting online features

Feast provides an API through which online feature values can be retrieved. This allows teams to look up feature values at low latency in production during model serving, in order to make online predictions.

{% hint style="info" %}
Online stores only maintain the current state of features, i.e latest feature values. No historical data is stored or served.
{% endhint %}

```python
from feast import Client

online_client = Client(
   core_url="localhost:6565",
   serving_url="localhost:6566",
)

entity_rows = [
   {"driver_id": 1001},
   {"driver_id": 1002},
]

# Features in <featuretable_name:feature_name> format
feature_refs = [
   "driver_trips:average_daily_rides",
   "driver_trips:maximum_daily_rides",
   "driver_trips:rating",
]

response = online_client.get_online_features(
   feature_refs=feature_refs, # Contains only feature references
   entity_rows=entity_rows, # Contains only entities (driver ids)
)

# Print features in dictionary format
response_dict = response.to_dict()
print(response_dict)
```

The online store must be populated through [ingestion jobs](define-and-ingest-features.md#batch-source-to-online-store) prior to being used for online serving.

Feast Serving provides a [gRPC API](https://api.docs.feast.dev/grpc/feast.serving.pb.html) that is backed by [Redis](https://redis.io/). We have native clients in [Python](https://api.docs.feast.dev/python/), [Go](https://godoc.org/github.com/gojek/feast/sdk/go), and [Java](https://javadoc.io/doc/dev.feast).

### Online Field Statuses

Feast also returns status codes when retrieving features from the Feast Serving API. These status code give useful insight into the quality of data being served. 

| Status | Meaning |
| :--- | :--- |
| NOT\_FOUND | The feature value was not found in the online store. This might mean that no feature value was ingested for this feature. |
| NULL\_VALUE | A entity key was successfully found but no feature values had been set. This status code should not occur during normal operation. |
| OUTSIDE\_MAX\_AGE | The age of the feature row in the online store \(in terms of its event timestamp\) has exceeded the maximum age defined within the feature table. |
| PRESENT | The feature values have been found and are within the maximum age. |
| UNKNOWN | Indicates a system failure. |

