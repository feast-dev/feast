# Sources

A `source` is a data source that can be used to find feature data. Users define sources as part of [feature sets](feature-sets.md). Once a feature set is registered with a source, Feast will automatically start to populate its stores with data from this source.

{% hint style="info" %}
Feast only supports [Kafka](https://kafka.apache.org/) as a source currently.
{% endhint %}

An example of a user provided source can be seen in the following code snippet

```python
feature_set = FeatureSet(
    name="stream_feature",
    entities=[
        Entity("entity_id", ValueType.INT64)
    ],
    features=[
        Feature("feature_value1", ValueType.STRING)
    ],
    source=KafkaSource(
        brokers="mybroker:9092",
        topic="my_feature_topic"
    )
)
```

Once this feature set is registered, Feast will start an ingestion job that retrieves data from this source and  starts to populate all [stores](stores.md) that subscribe to it.

In most cases a feature set \(and by extension its source\) will be used to populate both an online store and a historical store. This allows users to both train and serve their model with the same feature data.

Feast will ensure that the source complies with the schema of the feature set. The event data has to be [Protobuf](https://developers.google.com/protocol-buffers) encoded and must contain the necessary [FeatureRow](https://api.docs.feast.dev/grpc/feast.types.pb.html#FeatureRow) structure.

