import pytest

from feast.protos.feast.types.Value_pb2 import ValueType as ValueTypeProto
from feast.types import Array, Float32, String, from_value_type


def test_primitive_feast_type():
    assert String.to_value_type() == ValueTypeProto.Enum.Value("STRING")
    assert from_value_type(String.to_value_type()) == String
    assert Float32.to_value_type() == ValueTypeProto.Enum.Value("FLOAT")
    assert from_value_type(Float32.to_value_type()) == Float32


def test_array_feast_type():
    array_float_32 = Array(Float32)
    assert array_float_32.to_value_type() == ValueTypeProto.Enum.Value("FLOAT_LIST")
    assert from_value_type(array_float_32.to_value_type()) == array_float_32

    array_string = Array(String)
    assert array_string.to_value_type() == ValueTypeProto.Enum.Value("STRING_LIST")
    assert from_value_type(array_string.to_value_type()) == array_string

    with pytest.raises(ValueError):
        _ = Array(Array)

    with pytest.raises(ValueError):
        _ = Array(Array(String))


def test_all_value_types():
    values = ValueTypeProto.Enum.values()
    for value in values:
        # We do not support the NULL type.
        if value != ValueTypeProto.Enum.Value("NULL"):
            assert from_value_type(value).to_value_type() == value
