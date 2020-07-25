import sys

import pandas as pd
import numpy as np
import pytz
from feast.wait import wait_retry_backoff

from feast import Client, Feature, FeatureSet, Entity, ValueType
from datetime import datetime

feast_core_url = sys.argv[1]
feast_online_serving_url = sys.argv[2]

number_of_entities = 100

df = pd.DataFrame(
    {
        "datetime": [
            datetime.utcnow().replace(tzinfo=pytz.utc)
            for _ in range(number_of_entities)
        ],
        "user_id": [1000 + entity_id for entity_id in range(number_of_entities)],
        "int32_feature": [np.int32(1) for _ in range(number_of_entities)],
        "int64_feature": [np.int64(1) for _ in range(number_of_entities)],
        "float_feature": [np.float(0.1) for _ in range(number_of_entities)],
        "double_feature": [np.float64(0.1) for _ in range(number_of_entities)],
        "string_feature": ["one" for _ in range(number_of_entities)],
        "bytes_feature": [b"one" for _ in range(number_of_entities)],
        "bool_feature": [True for _ in range(number_of_entities)],
        "int32_list_feature": [
            np.array([1, 2, 3, 4], dtype=np.int32) for _ in range(number_of_entities)
        ],
        "int64_list_feature": [
            np.array([1, 2, 3, 4], dtype=np.int64) for _ in range(number_of_entities)
        ],
        "float_list_feature": [
            np.array([1.1, 1.2, 1.3, 1.4], dtype=np.float32)
            for _ in range(number_of_entities)
        ],
        "double_list_feature": [
            np.array([1.1, 1.2, 1.3, 1.4], dtype=np.float64)
            for _ in range(number_of_entities)
        ],
        "string_list_feature": [
            np.array(["one", "two", "three"]) for _ in range(number_of_entities)
        ],
        "bytes_list_feature": [
            np.array([b"one", b"two", b"three"]) for _ in range(number_of_entities)
        ],
    }
)

all_types_fs_expected = FeatureSet(
    name="all_types",
    entities=[Entity(name="user_id", dtype=ValueType.INT64)],
    features=[
        Feature(name="float_feature", dtype=ValueType.FLOAT),
        Feature(name="int64_feature", dtype=ValueType.INT64),
        Feature(name="int32_feature", dtype=ValueType.INT32),
        Feature(name="string_feature", dtype=ValueType.STRING),
        Feature(name="bytes_feature", dtype=ValueType.BYTES),
        Feature(name="bool_feature", dtype=ValueType.BOOL),
        Feature(name="double_feature", dtype=ValueType.DOUBLE),
        Feature(name="double_list_feature", dtype=ValueType.DOUBLE_LIST),
        Feature(name="float_list_feature", dtype=ValueType.FLOAT_LIST),
        Feature(name="int64_list_feature", dtype=ValueType.INT64_LIST),
        Feature(name="int32_list_feature", dtype=ValueType.INT32_LIST),
        Feature(name="string_list_feature", dtype=ValueType.STRING_LIST),
        Feature(name="bytes_list_feature", dtype=ValueType.BYTES_LIST),
    ],
)

client = Client(core_url=feast_core_url, serving_url=feast_online_serving_url)

# Register feature set
client.apply(all_types_fs_expected)

df.info()
df.describe()
df.head()

# Ingest tdata
client.ingest(all_types_fs_expected, df)


# Wait for data to be available
def try_get_features():
    online_request_entity = [{"user_id": 1001}]
    online_request_features = ["float_feature"]

    response = client.get_online_features(
        entity_rows=online_request_entity, feature_refs=online_request_features
    )
    response_dict = response.to_dict()
    if response_dict['float_feature'] == df.iloc[0]['float_feature']:
        return response_dict, True
    return response_dict, False


online_features_actual = wait_retry_backoff(
    retry_fn=try_get_features,
    timeout_secs=90,
    timeout_msg="Timed out trying to get online feature values",
)
