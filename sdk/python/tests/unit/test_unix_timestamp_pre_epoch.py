from datetime import datetime, timezone

import numpy as np
from google.protobuf.timestamp_pb2 import Timestamp

from feast.type_map import python_values_to_proto_values
from feast.value_type import ValueType


def test_pre_epoch_datetime_matches_other_timestamp_representations():
    values = [
        datetime(1969, 12, 31, 23, 59, 59, 500000, tzinfo=timezone.utc),
        Timestamp(seconds=-1, nanos=500000000),
        np.datetime64("1969-12-31T23:59:59.500000"),
    ]
    protos = python_values_to_proto_values(values, ValueType.UNIX_TIMESTAMP)
    actual = [value.unix_timestamp_val for value in protos]
    expected = [-1, -1, -1]
    assert actual == expected, f"Expected the same pre-epoch second: {actual}"


def test_unix_timestamp_whole_second_and_null_controls():
    values = [
        datetime(1969, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
        datetime(1970, 1, 1, 0, 0, 0, 500000, tzinfo=timezone.utc),
        datetime(1970, 1, 1, tzinfo=timezone.utc),
        None,
    ]
    protos = python_values_to_proto_values(values, ValueType.UNIX_TIMESTAMP)
    actual = [
        value.unix_timestamp_val if value.WhichOneof("val") is not None else None
        for value in protos
    ]
    expected = [-1, 0, 0, None]
    assert actual == expected
