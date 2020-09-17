from datetime import datetime

import numpy as np
import pandas as pd
import pytz

GOOD = pd.DataFrame(
    {
        "datetime": [datetime.utcnow().replace(tzinfo=pytz.utc) for _ in range(3)],
        "entity_id": [1001, 1002, 1004],
        "feature_1": [0.2, 0.4, 0.5],
        "feature_2": ["string1", "string2", "string3"],
        "feature_3": [1, 2, 5],
    }
)

GOOD_FIVE_FEATURES = pd.DataFrame(
    {
        "datetime": [datetime.utcnow().replace(tzinfo=pytz.utc) for _ in range(3)],
        "entity_id": [1001, 1002, 1004],
        "feature_1": [0.2, 0.4, 0.5],
        "feature_2": ["string1", "string2", "string3"],
        "feature_3": [1, 2, 5],
        "feature_4": [1, 2, 5],
        "feature_5": [1, 2, 5],
    }
)

GOOD_FIVE_FEATURES_TWO_ENTITIES = pd.DataFrame(
    {
        "datetime": [datetime.utcnow().replace(tzinfo=pytz.utc) for _ in range(3)],
        "entity_1_id": [1001, 1002, 1004],
        "entity_2_id": ["1001", "1002", "1003"],
        "feature_1": [0.2, 0.4, 0.5],
        "feature_2": ["string1", "string2", "string3"],
        "feature_3": [1, 2, 5],
        "feature_4": [1, 2, 5],
        "feature_5": [1.3, 1.3, 1.3],
    }
)


BAD_NO_ENTITY = pd.DataFrame(
    {
        "datetime": [datetime.utcnow().replace(tzinfo=pytz.utc) for _ in range(3)],
        "feature_1": [0.2, 0.4, 0.5],
        "feature_2": [0.3, 0.3, 0.34],
        "feature_3": [1, 2, 5],
    }
)

NO_FEATURES = pd.DataFrame(
    {
        "datetime": [datetime.utcnow().replace(tzinfo=pytz.utc) for _ in range(3)],
        "entity_id": [1001, 1002, 1004],
    }
)

BAD_NO_DATETIME = pd.DataFrame(
    {
        "entity_id": [1001, 1002, 1004],
        "feature_1": [0.2, 0.4, 0.5],
        "feature_2": [0.3, 0.3, 0.34],
        "feature_3": [1, 2, 5],
    }
)

BAD_INCORRECT_DATETIME_TYPE = pd.DataFrame(
    {
        "datetime": [1.23, 3.23, 1.23],
        "entity_id": [1001, 1002, 1004],
        "feature_1": [0.2, 0.4, 0.5],
        "feature_2": [0.3, 0.3, 0.34],
        "feature_3": [1, 2, 5],
    }
)

ALL_TYPES = pd.DataFrame(
    {
        "datetime": [datetime.utcnow().replace(tzinfo=pytz.utc) for _ in range(3)],
        "user_id": [1001, 1002, 1004],
        "int32_feature": [np.int32(1), np.int32(2), np.int32(3)],
        "int64_feature": [np.int64(1), np.int64(2), np.int64(3)],
        "float_feature": [np.float(0.1), np.float(0.2), np.float(0.3)],
        "double_feature": [np.float64(0.1), np.float64(0.2), np.float64(0.3)],
        "string_feature": ["one", "two", "three"],
        "bytes_feature": [b"one", b"two", b"three"],
        "bool_feature": [True, False, False],
        "int32_list_feature": [
            np.array([1, 2, 3, 4], dtype=np.int32),
            np.array([1, 2, 3, 4], dtype=np.int32),
            np.array([1, 2, 3, 4], dtype=np.int32),
        ],
        "int64_list_feature": [
            np.array([1, 2, 3, 4], dtype=np.int64),
            np.array([1, 2, 3, 4], dtype=np.int64),
            np.array([1, 2, 3, 4], dtype=np.int64),
        ],
        "float_list_feature": [
            np.array([1.1, 1.2, 1.3, 1.4], dtype=np.float32),
            np.array([1.1, 1.2, 1.3, 1.4], dtype=np.float32),
            np.array([1.1, 1.2, 1.3, 1.4], dtype=np.float32),
        ],
        "double_list_feature": [
            np.array([1.1, 1.2, 1.3, 1.4], dtype=np.float64),
            np.array([1.1, 1.2, 1.3, 1.4], dtype=np.float64),
            np.array([1.1, 1.2, 1.3, 1.4], dtype=np.float64),
        ],
        "string_list_feature": [
            np.array(["one", "two", "three"]),
            np.array(["one", "two", "three"]),
            np.array(["one", "two", "three"]),
        ],
        "bytes_list_feature": [
            np.array([b"one", b"two", b"three"]),
            np.array([b"one", b"two", b"three"]),
            np.array([b"one", b"two", b"three"]),
        ],
        "bool_list_feature": [
            [True, False, True],
            [True, False, True],
            [True, False, True],
        ],
    }
)
