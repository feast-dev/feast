# Features

A feature is an individual measurable property or characteristic of a phenomenon being observed. Features are the most important concepts within a feature store. Feature data is used both as input to models during training and when models are served in production.

In the context of Feast, features are values that are associated with either one or more entities over time. In Feast, these values are either primitives or lists of primitives. Each feature can also have additional information attached to it. 

The following is a YAML representation of a feature specification. This specification would form part of a larger specification within a [feature set](feature-sets.md).

{% code title="total\_trips\_feature.yaml" %}
```yaml
# Entity name
name: total_trips_24h

# Entity value type
value_type: INT64
```
{% endcode %}

 Features can be created through the[ Feast SDK](../introduction/getting-started/feast-sdk.md) as follows

```python
from feast import Entity, Feature, ValueType, FeatureSet

# Create a driver entity
driver = Entity("driver_id", ValueType.INT64)

# Create a total trips 24h feature
total_trips_24h = Feature("total_trips_24h", ValueType.INT64)

# Create a feature set with a single entity and a single feature
driver_fs = FeatureSet("customer_fs", entities=[driver], features=[total_trips_24h])

# Register the feature set with Feast
client.apply(driver_fs)
```

Please see the [FeatureSpec](https://api.docs.feast.dev/grpc/feast.core.pb.html#FeatureSpec) for the complete feature specification API.



