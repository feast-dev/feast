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
