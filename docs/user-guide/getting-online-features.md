# Getting online features

Feast provides an online retrieval interface for serving. Data ingested into the online store comes from both batch and stream sources. 

When data is ingested from a batch source, users can retrieve the same features used in training models from the low latency online store to be used in production. When data is ingested from a stream source, features that are retrieved by users are of the latest values, which are not yet used in training models.

Online feature retrieval works in much the same way as batch retrieval, with one important distinction: Online stores only maintain the current state of features, i.e latest feature values. No historical data is served.

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

{% hint style="info" %}
When no project is specified when retrieving features with get\_online\_feature\(\), Feast infers that the features specified belong to the default project. To retrieve from another project, specify the project parameter when retrieving features.
{% endhint %}

Feast Serving provides a [gRPC API](https://api.docs.feast.dev/grpc/feast.serving.pb.html) that is backed by [Redis](https://redis.io/). We also provide support for [Python](https://api.docs.feast.dev/python/), [Go](https://godoc.org/github.com/gojek/feast/sdk/go), and [Java](https://javadoc.io/doc/dev.feast) clients.

### Online Field Statuses

Online Serving also returns Online Field Statuses when retrieving features. These status values gives useful insight into situations where Online Serving returns unset values. It also allows better of handling of the different possible cases represented by each status:for feature in features:

```python
response_dict = response.to_dict()

for feature_ref in feature_refs:
    # field status can be obtained from the response's field values
    status = response_dict["field_values"]["statuses"][feature_ref]

    if status == GetOnlineFeaturesResponse.FieldStatus.NOT_FOUND:
       # handle case where feature value has not been ingested
    elif status == GetOnlineFeaturesResponse.FieldStatus.PRESENT:
       # feature value is present and can be used
       value = response_dict["field_values"]["statuses"][feature_ref]
```

| Status | Meaning |
| :--- | :--- |
| NOT\_FOUND | Unset values returned as the feature value was not found in the online store. This might mean that no feature value was ingested for this feature. |
| NULL\_VALUE | Unset values returned as the ingested feature value was also unset. |
| OUTSIDE\_MAX\_AGE | Unset values returned as the age of the feature value \(time since the value was ingested\) has exceeded the Feature Set's max age, which the feature was defined in. |
| PRESENT | Set values are returned for the requested feature. |
| UNKNOWN | Status signifies the field status is unset for the requested feature. Might mean that the Feast version does not support Field Statuses. |

