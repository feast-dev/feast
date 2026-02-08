import pytest

from feast.types import Array, Float32, Set, String, TimeUuid, Uuid, from_value_type
from feast.value_type import ValueType


def test_primitive_feast_type():
    assert String.to_value_type() == ValueType.STRING
    assert from_value_type(String.to_value_type()) == String
    assert Float32.to_value_type() == ValueType.FLOAT
    assert from_value_type(Float32.to_value_type()) == Float32


def test_array_feast_type():
    array_string = Array(String)
    assert array_string.to_value_type() == ValueType.STRING_LIST
    assert from_value_type(array_string.to_value_type()) == array_string

    array_float_32 = Array(Float32)
    assert array_float_32.to_value_type() == ValueType.FLOAT_LIST
    assert from_value_type(array_float_32.to_value_type()) == array_float_32

    with pytest.raises(ValueError):
        _ = Array(Array)

    with pytest.raises(ValueError):
        _ = Array(Array(String))


def test_set_feast_type():
    set_string = Set(String)
    assert set_string.to_value_type() == ValueType.STRING_SET
    assert from_value_type(set_string.to_value_type()) == set_string

    set_float_32 = Set(Float32)
    assert set_float_32.to_value_type() == ValueType.FLOAT_SET
    assert from_value_type(set_float_32.to_value_type()) == set_float_32

    with pytest.raises(ValueError):
        _ = Set(Set)

    with pytest.raises(ValueError):
        _ = Set(Set(String))


def test_uuid_feast_type():
    assert Uuid.to_value_type() == ValueType.UUID
    assert from_value_type(ValueType.UUID) == Uuid
    assert TimeUuid.to_value_type() == ValueType.TIME_UUID
    assert from_value_type(ValueType.TIME_UUID) == TimeUuid


def test_uuid_array_feast_type():
    array_uuid = Array(Uuid)
    assert array_uuid.to_value_type() == ValueType.UUID_LIST
    assert from_value_type(array_uuid.to_value_type()) == array_uuid

    array_time_uuid = Array(TimeUuid)
    assert array_time_uuid.to_value_type() == ValueType.TIME_UUID_LIST
    assert from_value_type(array_time_uuid.to_value_type()) == array_time_uuid


def test_all_value_types():
    for value in ValueType:
        # We do not support the NULL type.
        if value != ValueType.NULL:
            assert from_value_type(value).to_value_type() == value
