# Push source

## Description

Push sources allow feature values to be pushed to the online store in real time. This allows fresh feature values to be made available to applications. Push sources supercede the 
[FeatureStore.write_to_online_store](https://rtd.feast.dev/en/latest/index.html#feast.feature_store.FeatureStore.write_to_online_store).

Push sources can be used by multiple feature views. When data is pushed to a push source, feast propagates the feature values to all the consuming feature views.

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
    entities=["location_id"],
    features=[Feature(name="life_time_value", dtype=ValueType.INT64)],
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

Configuration options are available [here](https://rtd.feast.dev/en/latest/index.html#feast.data_source.FileSource).

