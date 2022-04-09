# Push source

## Description

Push sources allow feature values to be pushed to the online store in real time. This allows fresh feature values to be made available to applications. Push sources supercede the 
[FeatureStore.write_to_online_store](https://rtd.feast.dev/en/latest/index.html#feast.feature_store.FeatureStore.write_to_online_store).

Push sources can be used by multiple feature views. When data is pushed to a push source, Feast propagates the feature values to all the consuming feature views.

Push sources must have a batch source specified, since that's the source used when retrieving historical features. 
When using a PushSource as a stream source in the definition of a feature view, a batch source doesn't need to be specified in the definition explicitly.

## Example
### Defining a push source

```python
from feast import PushSource, ValueType, BigQuerySource, FeatureView, Feature

push_source = PushSource(
    name="push_source",
    schema={"user_id": ValueType.INT64, "life_time_value": ValueType.INT64},
    batch_source=BigQuerySource(table="test.test"),
)

fv = FeatureView(
    name="feature view",
    entities=["user_id"],
    schema=[Field(name="life_time_value", dtype=Int64)],
    stream_source=push_source,
)
```

### Pushing data
```python
from feast import FeatureStore
import pandas as pd

fs = FeatureStore(...)
feature_data_frame = pd.DataFrame()
fs.push("push_source", feature_data_frame)
```

