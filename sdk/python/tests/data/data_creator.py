from datetime import datetime, timedelta
from typing import List

import pandas as pd
from pytz import timezone, utc

from feast.value_type import ValueType


def create_dataset(
    entity_type: ValueType = ValueType.INT32,
    feature_dtype: str = None,
    feature_is_list: bool = False,
) -> pd.DataFrame:
    now = datetime.now().replace(microsecond=0, second=0, minute=0)
    ts = pd.Timestamp(now).round("ms")
    data = {
        "driver_id": get_entities_for_value_type(entity_type),
        "value": get_feature_values_for_dtype(feature_dtype, feature_is_list),
        "ts_1": [
            ts - timedelta(hours=4),
            ts,
            ts - timedelta(hours=3),
            # Use different time zones to test tz-naive -> tz-aware conversion
            (ts - timedelta(hours=4))
            .replace(tzinfo=utc)
            .astimezone(tz=timezone("Europe/Berlin")),
            (ts - timedelta(hours=1))
            .replace(tzinfo=utc)
            .astimezone(tz=timezone("US/Pacific")),
        ],
        "created_ts": [ts, ts, ts, ts, ts],
    }
    return pd.DataFrame.from_dict(data)


def create_entityless_dataset() -> pd.DataFrame:
    now = datetime.utcnow()
    ts = pd.Timestamp(now).round("ms")
    data = {
        "entityless_value": [10, None, 30, 40, 50],
        "ts_1": [
            ts - timedelta(hours=4),
            ts,
            ts - timedelta(hours=3),
            # Use different time zones to test tz-naive -> tz-aware conversion
            (ts - timedelta(hours=4))
            .replace(tzinfo=utc)
            .astimezone(tz=timezone("Europe/Berlin")),
            (ts - timedelta(hours=1))
            .replace(tzinfo=utc)
            .astimezone(tz=timezone("US/Pacific")),
        ],
        "created_ts": [ts, ts, ts, ts, ts],
    }
    return pd.DataFrame.from_dict(data)


def get_entities_for_value_type(value_type: ValueType) -> List:
    value_type_map = {
        ValueType.INT32: [1, 2, 1, 3, 3],
        ValueType.INT64: [1, 2, 1, 3, 3],
        ValueType.FLOAT: [1.0, 2.0, 1.0, 3.0, 3.0],
        ValueType.STRING: ["1", "2", "1", "3", "3"],
    }
    return value_type_map[value_type]


def get_feature_values_for_dtype(dtype: str, is_list: bool) -> List:
    if dtype is None:
        return [0.1, None, 0.3, 4, 5]
    # TODO(adchia): for int columns, consider having a better error when dealing with None values (pandas int dfs can't
    #  have na)
    dtype_map = {
        "int32": [1, 2, 3, 4, 5],
        "int64": [1, 2, 3, 4, 5],
        "float": [1.0, None, 3.0, 4.0, 5.0],
        "string": ["1", None, "3", "4", "5"],
        "bool": [True, None, False, True, False],
    }
    non_list_val = dtype_map[dtype]
    # Duplicate the value once if this is a list
    if is_list:
        return [[n, n] if n is not None else None for n in non_list_val]
    else:
        return non_list_val
