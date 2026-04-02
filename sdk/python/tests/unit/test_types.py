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
    """Array(Array(T)) should produce VALUE_LIST."""
    t = Array(Array(String))
    assert t.to_value_type() == ValueType.VALUE_LIST
    assert from_feast_type(t) == ValueType.VALUE_LIST

    t2 = Array(Array(Int32))
    assert t2.to_value_type() == ValueType.VALUE_LIST


def test_nested_array_set():
    """Array(Set(T)) should produce VALUE_LIST."""
    t = Array(Set(String))
    assert t.to_value_type() == ValueType.VALUE_LIST
    assert from_feast_type(t) == ValueType.VALUE_LIST


def test_nested_set_array():
    """Set(Array(T)) should produce VALUE_SET."""
    t = Set(Array(String))
    assert t.to_value_type() == ValueType.VALUE_SET
    assert from_feast_type(t) == ValueType.VALUE_SET


def test_nested_set_set():
    """Set(Set(T)) should produce VALUE_SET."""
    t = Set(Set(String))
    assert t.to_value_type() == ValueType.VALUE_SET
    assert from_feast_type(t) == ValueType.VALUE_SET


def test_nested_unbounded_depth():
    """Nesting depth should be unbounded."""
    # 3-level
    t3 = Array(Array(Array(String)))
    assert t3.to_value_type() == ValueType.VALUE_LIST

    t3_mixed = Array(Set(Array(String)))
    assert t3_mixed.to_value_type() == ValueType.VALUE_LIST

    t3_set = Set(Array(Array(String)))
    assert t3_set.to_value_type() == ValueType.VALUE_SET

    t3_set2 = Set(Set(Set(String)))
    assert t3_set2.to_value_type() == ValueType.VALUE_SET

    # 4-level
    t4 = Array(Array(Array(Array(Int32))))
    assert t4.to_value_type() == ValueType.VALUE_LIST


def test_nested_from_value_type_roundtrip():
    """from_value_type should return a placeholder for nested types."""
    for vt in (
        ValueType.VALUE_LIST,
        ValueType.VALUE_SET,
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

    # 3-level: Array(Array(Array(Int32))) -> list(list(list(int32)))
    pa_type = from_feast_to_pyarrow_type(Array(Array(Array(Int32))))
    assert pa_type == pyarrow.list_(pyarrow.list_(pyarrow.list_(pyarrow.int32())))


def test_nested_field_roundtrip():
    """Field with nested collection type should survive to_proto -> from_proto."""
    test_cases = [
        ("aa", Array(Array(String))),
        ("as_field", Array(Set(Int32))),
        ("sa", Set(Array(Float64))),
        ("ss", Set(Set(Bool))),
        # 3-level nesting
        ("aaa", Array(Array(Array(Int32)))),
        ("asa", Array(Set(Array(String)))),
        # 4-level nesting
        ("aaaa", Array(Array(Array(Array(Float64))))),
    ]
    for name, dtype in test_cases:
        field = Field(name=name, dtype=dtype, tags={"user_tag": "value"})
        proto = field.to_proto()
        restored = Field.from_proto(proto)
        assert restored.name == name, f"Name mismatch for {dtype}"
        assert restored.dtype.to_value_type() == dtype.to_value_type(), (
            f"dtype mismatch for {name}: {restored.dtype} vs {dtype}"
        )
        # Verify inner type is preserved (not just ValueType equality)
        assert str(restored.dtype) == str(dtype), (
            f"Inner type lost for {name}: got {restored.dtype}, expected {dtype}"
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


def test_feast_type_str_roundtrip():
    """_feast_type_to_str and _str_to_feast_type should roundtrip for nested types."""
    from feast.field import _feast_type_to_str, _str_to_feast_type

    test_cases = [
        Array(Array(String)),
        Array(Array(Int32)),
        Array(Array(Float64)),
        Array(Set(Int64)),
        Array(Set(Bool)),
        Set(Array(String)),
        Set(Array(Float32)),
        Set(Set(Int32)),
        Set(Set(Float64)),
        # 3+ level nesting
        Array(Array(Array(String))),
        Array(Set(Array(Int32))),
        Set(Set(Set(Float64))),
    ]
    for dtype in test_cases:
        s = _feast_type_to_str(dtype)
        restored = _str_to_feast_type(s)
        assert str(restored) == str(dtype), (
            f"Roundtrip failed: {dtype} -> '{s}' -> {restored}"
        )


def test_str_to_feast_type_invalid():
    """_str_to_feast_type should raise ValueError on unrecognized type names."""
    from feast.field import _str_to_feast_type

    with pytest.raises(ValueError, match="Unknown FeastType"):
        _str_to_feast_type("INVALID_TYPE")

    with pytest.raises(ValueError, match="Unknown FeastType"):
        _str_to_feast_type("Strig")


def test_all_value_types():
    for value in ValueType:
        # We do not support the NULL type.
        if value != ValueType.NULL:
            assert from_value_type(value).to_value_type() == value
