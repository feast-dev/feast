import pyarrow
import pytest

from feast.field import Field
from feast.types import (
    Array,
    Bool,
    Float32,
    Float64,
    Int32,
    Int64,
    Set,
    String,
    TimeUuid,
    Uuid,
    from_feast_to_pyarrow_type,
    from_feast_type,
    from_value_type,
)
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


def test_set_feast_type():
    set_string = Set(String)
    assert set_string.to_value_type() == ValueType.STRING_SET
    assert from_value_type(set_string.to_value_type()) == set_string

    set_float_32 = Set(Float32)
    assert set_float_32.to_value_type() == ValueType.FLOAT_SET
    assert from_value_type(set_float_32.to_value_type()) == set_float_32

    with pytest.raises(ValueError):
        _ = Set(Set)


def test_nested_array_array():
    """Array(Array(T)) should produce LIST_LIST."""
    t = Array(Array(String))
    assert t.to_value_type() == ValueType.LIST_LIST
    assert from_feast_type(t) == ValueType.LIST_LIST

    t2 = Array(Array(Int32))
    assert t2.to_value_type() == ValueType.LIST_LIST


def test_nested_array_set():
    """Array(Set(T)) should produce LIST_SET."""
    t = Array(Set(String))
    assert t.to_value_type() == ValueType.LIST_SET
    assert from_feast_type(t) == ValueType.LIST_SET


def test_nested_set_array():
    """Set(Array(T)) should produce SET_LIST."""
    t = Set(Array(String))
    assert t.to_value_type() == ValueType.SET_LIST
    assert from_feast_type(t) == ValueType.SET_LIST


def test_nested_set_set():
    """Set(Set(T)) should produce SET_SET."""
    t = Set(Set(String))
    assert t.to_value_type() == ValueType.SET_SET
    assert from_feast_type(t) == ValueType.SET_SET


def test_nested_depth_limit():
    """3 levels of nesting should raise ValueError."""
    with pytest.raises(ValueError, match="too deeply nested"):
        Array(Array(Array(String)))

    with pytest.raises(ValueError, match="too deeply nested"):
        Array(Set(Array(String)))

    with pytest.raises(ValueError, match="too deeply nested"):
        Set(Array(Array(String)))

    with pytest.raises(ValueError, match="too deeply nested"):
        Set(Set(Set(String)))


def test_nested_from_value_type_roundtrip():
    """from_value_type should return a placeholder for nested types."""
    for vt in (
        ValueType.LIST_LIST,
        ValueType.LIST_SET,
        ValueType.SET_LIST,
        ValueType.SET_SET,
    ):
        ft = from_value_type(vt)
        assert ft.to_value_type() == vt


def test_nested_pyarrow_conversion():
    """Nested collection types should convert to pyarrow list(list(...))."""
    # Array(Array(String)) -> list(list(string))
    pa_type = from_feast_to_pyarrow_type(Array(Array(String)))
    assert pa_type == pyarrow.list_(pyarrow.list_(pyarrow.string()))

    # Array(Set(Int64)) -> list(list(int64))
    pa_type = from_feast_to_pyarrow_type(Array(Set(Int64)))
    assert pa_type == pyarrow.list_(pyarrow.list_(pyarrow.int64()))

    # Set(Array(Float64)) -> list(list(float64))
    pa_type = from_feast_to_pyarrow_type(Set(Array(Float64)))
    assert pa_type == pyarrow.list_(pyarrow.list_(pyarrow.float64()))

    # Set(Set(Bool)) -> list(list(bool))
    pa_type = from_feast_to_pyarrow_type(Set(Set(Bool)))
    assert pa_type == pyarrow.list_(pyarrow.list_(pyarrow.bool_()))


def test_nested_field_roundtrip():
    """Field with nested collection type should survive to_proto -> from_proto."""
    test_cases = [
        ("aa", Array(Array(String))),
        ("as", Array(Set(Int32))),
        ("sa", Set(Array(Float64))),
        ("ss", Set(Set(Bool))),
    ]
    for name, dtype in test_cases:
        field = Field(name=name, dtype=dtype, tags={"user_tag": "value"})
        proto = field.to_proto()
        restored = Field.from_proto(proto)
        assert restored.name == name, f"Name mismatch for {dtype}"
        assert restored.dtype.to_value_type() == dtype.to_value_type(), (
            f"dtype mismatch for {name}: {restored.dtype} vs {dtype}"
        )
        assert restored.tags == {"user_tag": "value"}, (
            f"Tags should not contain internal tags for {name}"
        )


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


def test_uuid_set_feast_type():
    set_uuid = Set(Uuid)
    assert set_uuid.to_value_type() == ValueType.UUID_SET
    assert from_value_type(set_uuid.to_value_type()) == set_uuid

    set_time_uuid = Set(TimeUuid)
    assert set_time_uuid.to_value_type() == ValueType.TIME_UUID_SET
    assert from_value_type(set_time_uuid.to_value_type()) == set_time_uuid


def test_all_value_types():
    for value in ValueType:
        # We do not support the NULL type.
        if value != ValueType.NULL:
            assert from_value_type(value).to_value_type() == value
