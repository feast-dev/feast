import numpy as np
import pytest

from feast.type_map import (
    feast_value_type_to_python_type,
    python_values_to_proto_values,
)
from feast.value_type import ValueType


def test_null_unix_timestamp():
    """Test that null UnixTimestamps get converted from proto correctly."""

    data = np.array(["NaT"], dtype="datetime64")
    protos = python_values_to_proto_values(data, ValueType.UNIX_TIMESTAMP)
    converted = feast_value_type_to_python_type(protos[0])

    assert converted is None


def test_null_unix_timestamp_list():
    """Test that UnixTimestamp lists with a null get converted from proto
    correctly."""

    data = np.array([["NaT"]], dtype="datetime64")
    protos = python_values_to_proto_values(data, ValueType.UNIX_TIMESTAMP_LIST)
    converted = feast_value_type_to_python_type(protos[0])

    assert converted[0] is None


@pytest.mark.parametrize(
    "values",
    (
        np.array([True]),
        np.array([False]),
        np.array([0]),
        np.array([1]),
        [True],
        [False],
        [0],
        [1],
    ),
)
def test_python_values_to_proto_values_bool(values):

    protos = python_values_to_proto_values(values, ValueType.BOOL)
    converted = feast_value_type_to_python_type(protos[0])

    assert converted is bool(values[0])


@pytest.mark.parametrize(
    "values, value_type, expected",
    (
        (np.array([b'[1,2,3]']), ValueType.INT64_LIST, [1, 2, 3]),
        (np.array([b'[1,2,3]']), ValueType.INT32_LIST, [1, 2, 3]),
        (np.array([b'[1.5,2.5,3.5]']), ValueType.FLOAT_LIST, [1.5, 2.5, 3.5]),
        (np.array([b'[1.5,2.5,3.5]']), ValueType.DOUBLE_LIST, [1.5, 2.5, 3.5]),
        (np.array([b'["a","b","c"]']), ValueType.STRING_LIST, ["a", "b", "c"]),
        (np.array([b'[true,false]']), ValueType.BOOL_LIST, [True, False]),
        (np.array([b'[1,0]']), ValueType.BOOL_LIST, [True, False]),
        (np.array([None]), ValueType.STRING_LIST, None),
        ([b'[1,2,3]'], ValueType.INT64_LIST, [1, 2, 3]),
        ([b'[1,2,3]'], ValueType.INT32_LIST, [1, 2, 3]),
        ([b'[1.5,2.5,3.5]'], ValueType.FLOAT_LIST, [1.5, 2.5, 3.5]),
        ([b'[1.5,2.5,3.5]'], ValueType.DOUBLE_LIST, [1.5, 2.5, 3.5]),
        ([b'["a","b","c"]'], ValueType.STRING_LIST, ["a", "b", "c"]),
        ([b'[true,false]'], ValueType.BOOL_LIST, [True, False]),
        ([b'[1,0]'], ValueType.BOOL_LIST, [True, False]),
        ([None], ValueType.STRING_LIST, None),
    ),
)
def test_python_values_to_proto_values_bytes_to_list(values, value_type, expected):
    protos = python_values_to_proto_values(values, value_type)
    converted = feast_value_type_to_python_type(protos[0])
    assert converted == expected


def test_python_values_to_proto_values_bytes_to_list_not_supported():
    with pytest.raises(TypeError):
        _ = python_values_to_proto_values([b"[]"], ValueType.BYTES_LIST)
