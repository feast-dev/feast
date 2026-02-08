import uuid

import numpy as np
import pandas as pd
import pytest

from feast.protos.feast.types.Value_pb2 import Map, MapList
from feast.type_map import (
    _python_dict_to_map_proto,
    _python_list_to_map_list_proto,
    feast_value_type_to_python_type,
    pg_type_to_feast_value_type,
    python_type_to_feast_value_type,
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
        (np.array([b"[1,2,3]"]), ValueType.INT64_LIST, [1, 2, 3]),
        (np.array([b"[1,2,3]"]), ValueType.INT32_LIST, [1, 2, 3]),
        (np.array([b"[1.5,2.5,3.5]"]), ValueType.FLOAT_LIST, [1.5, 2.5, 3.5]),
        (np.array([b"[1.5,2.5,3.5]"]), ValueType.DOUBLE_LIST, [1.5, 2.5, 3.5]),
        (np.array([b'["a","b","c"]']), ValueType.STRING_LIST, ["a", "b", "c"]),
        (np.array([b"[true,false]"]), ValueType.BOOL_LIST, [True, False]),
        (np.array([b"[1,0]"]), ValueType.BOOL_LIST, [True, False]),
        (np.array([None]), ValueType.INT32_LIST, None),
        (np.array([None]), ValueType.INT64_LIST, None),
        (np.array([None]), ValueType.FLOAT_LIST, None),
        (np.array([None]), ValueType.DOUBLE_LIST, None),
        (np.array([None]), ValueType.BOOL_LIST, None),
        (np.array([None]), ValueType.BYTES_LIST, None),
        (np.array([None]), ValueType.STRING_LIST, None),
        (np.array([None]), ValueType.UNIX_TIMESTAMP_LIST, None),
        ([b"[1,2,3]"], ValueType.INT64_LIST, [1, 2, 3]),
        ([b"[1,2,3]"], ValueType.INT32_LIST, [1, 2, 3]),
        ([b"[1.5,2.5,3.5]"], ValueType.FLOAT_LIST, [1.5, 2.5, 3.5]),
        ([b"[1.5,2.5,3.5]"], ValueType.DOUBLE_LIST, [1.5, 2.5, 3.5]),
        ([b'["a","b","c"]'], ValueType.STRING_LIST, ["a", "b", "c"]),
        ([b"[true,false]"], ValueType.BOOL_LIST, [True, False]),
        ([b"[1,0]"], ValueType.BOOL_LIST, [True, False]),
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


def test_python_values_to_proto_values_int_list_with_null_not_supported():
    df = pd.DataFrame({"column": [1, 2, None]})
    arr = df["column"].to_numpy()
    with pytest.raises(TypeError):
        _ = python_values_to_proto_values(arr, ValueType.INT32_LIST)


class TestMapTypes:
    """Test cases for MAP and MAP_LIST value types."""

    def test_simple_map_conversion(self):
        """Test basic MAP type conversion from Python dict to proto and back."""
        test_dict = {"key1": "value1", "key2": "value2", "key3": 123}

        protos = python_values_to_proto_values([test_dict], ValueType.MAP)
        converted = feast_value_type_to_python_type(protos[0])

        assert isinstance(converted, dict)
        assert converted["key1"] == "value1"
        assert converted["key2"] == "value2"
        assert converted["key3"] == 123

    def test_nested_map_conversion(self):
        """Test nested MAP type conversion."""
        test_dict = {
            "level1": {
                "level2": {"key": "deep_value", "number": 42},
                "simple": "value",
            },
            "top_level": "top_value",
        }

        protos = python_values_to_proto_values([test_dict], ValueType.MAP)
        converted = feast_value_type_to_python_type(protos[0])

        assert isinstance(converted, dict)
        assert converted["level1"]["level2"]["key"] == "deep_value"
        assert converted["level1"]["level2"]["number"] == 42
        assert converted["level1"]["simple"] == "value"
        assert converted["top_level"] == "top_value"

    def test_map_with_different_value_types(self):
        """Test MAP with various value types."""
        test_dict = {
            "string_val": "hello",
            "int_val": 42,
            "float_val": 3.14,
            "bool_val": True,
            "list_val": [1, 2, 3],
            "string_list_val": ["a", "b", "c"],
        }

        protos = python_values_to_proto_values([test_dict], ValueType.MAP)
        converted = feast_value_type_to_python_type(protos[0])

        assert converted["string_val"] == "hello"
        assert converted["int_val"] == 42
        assert converted["float_val"] == 3.14
        assert converted["bool_val"] is True
        assert converted["list_val"] == [1, 2, 3]
        assert converted["string_list_val"] == ["a", "b", "c"]

    def test_map_with_none_values(self):
        """Test MAP with None values."""
        test_dict = {"key1": "value1", "key2": None, "key3": "value3"}

        protos = python_values_to_proto_values([test_dict], ValueType.MAP)
        converted = feast_value_type_to_python_type(protos[0])

        assert converted["key1"] == "value1"
        assert converted["key2"] is None
        assert converted["key3"] == "value3"

    def test_empty_map(self):
        """Test empty MAP conversion."""
        test_dict = {}

        protos = python_values_to_proto_values([test_dict], ValueType.MAP)
        converted = feast_value_type_to_python_type(protos[0])

        assert isinstance(converted, dict)
        assert len(converted) == 0

    def test_null_map(self):
        """Test None MAP conversion."""
        protos = python_values_to_proto_values([None], ValueType.MAP)
        converted = feast_value_type_to_python_type(protos[0])

        assert converted is None

    def test_map_list_conversion(self):
        """Test basic MAP_LIST type conversion."""
        test_list = [
            {"name": "John", "age": 30},
            {"name": "Jane", "age": 25},
            {"name": "Bob", "score": 85.5},
        ]

        protos = python_values_to_proto_values([test_list], ValueType.MAP_LIST)
        converted = feast_value_type_to_python_type(protos[0])

        assert isinstance(converted, list)
        assert len(converted) == 3
        assert converted[0]["name"] == "John"
        assert converted[0]["age"] == 30
        assert converted[1]["name"] == "Jane"
        assert converted[1]["age"] == 25
        assert converted[2]["name"] == "Bob"
        assert converted[2]["score"] == 85.5

    def test_map_list_with_nested_maps(self):
        """Test MAP_LIST with nested maps."""
        test_list = [
            {"user": {"name": "John", "details": {"city": "NYC"}}, "score": 100},
            {"user": {"name": "Jane", "details": {"city": "SF"}}, "score": 95},
        ]

        protos = python_values_to_proto_values([test_list], ValueType.MAP_LIST)
        converted = feast_value_type_to_python_type(protos[0])

        assert len(converted) == 2
        assert converted[0]["user"]["name"] == "John"
        assert converted[0]["user"]["details"]["city"] == "NYC"
        assert converted[1]["user"]["name"] == "Jane"
        assert converted[1]["user"]["details"]["city"] == "SF"

    def test_map_list_with_lists_in_maps(self):
        """Test MAP_LIST where maps contain lists."""
        test_list = [
            {"name": "John", "hobbies": ["reading", "swimming"]},
            {"name": "Jane", "scores": [95, 87, 92]},
        ]

        protos = python_values_to_proto_values([test_list], ValueType.MAP_LIST)
        converted = feast_value_type_to_python_type(protos[0])

        assert converted[0]["name"] == "John"
        assert converted[0]["hobbies"] == ["reading", "swimming"]
        assert converted[1]["name"] == "Jane"
        assert converted[1]["scores"] == [95, 87, 92]

    def test_empty_map_list(self):
        """Test empty MAP_LIST conversion."""
        test_list = []

        protos = python_values_to_proto_values([test_list], ValueType.MAP_LIST)
        converted = feast_value_type_to_python_type(protos[0])

        assert isinstance(converted, list)
        assert len(converted) == 0

    def test_null_map_list(self):
        """Test None MAP_LIST conversion."""
        protos = python_values_to_proto_values([None], ValueType.MAP_LIST)
        converted = feast_value_type_to_python_type(protos[0])

        assert converted is None

    def test_map_list_with_empty_maps(self):
        """Test MAP_LIST containing empty maps."""
        test_list = [{}, {"key": "value"}, {}]

        protos = python_values_to_proto_values([test_list], ValueType.MAP_LIST)
        converted = feast_value_type_to_python_type(protos[0])

        assert len(converted) == 3
        assert len(converted[0]) == 0
        assert converted[1]["key"] == "value"
        assert len(converted[2]) == 0

    def test_python_type_inference_for_map(self):
        """Test that dictionaries are correctly inferred as MAP type."""
        test_dict = {"key1": "value1", "key2": 123}

        inferred_type = python_type_to_feast_value_type("test_field", test_dict)

        assert inferred_type == ValueType.MAP

    def test_python_type_inference_for_map_list(self):
        """Test that list of dictionaries is correctly inferred as MAP_LIST type."""
        test_list = [{"key1": "value1"}, {"key2": "value2"}]

        inferred_type = python_type_to_feast_value_type("test_field", test_list)

        assert inferred_type == ValueType.MAP_LIST

    def test_multiple_map_values(self):
        """Test conversion of multiple MAP values."""
        test_dicts = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "city": "NYC"},
        ]

        protos = python_values_to_proto_values(test_dicts, ValueType.MAP)
        converted_values = [feast_value_type_to_python_type(proto) for proto in protos]

        assert len(converted_values) == 3
        assert converted_values[0]["name"] == "Alice"
        assert converted_values[1]["name"] == "Bob"
        assert converted_values[2]["city"] == "NYC"

    def test_multiple_map_list_values(self):
        """Test conversion of multiple MAP_LIST values."""
        test_lists = [[{"id": 1}, {"id": 2}], [{"id": 3}, {"id": 4}], []]

        protos = python_values_to_proto_values(test_lists, ValueType.MAP_LIST)
        converted_values = [feast_value_type_to_python_type(proto) for proto in protos]

        assert len(converted_values) == 3
        assert len(converted_values[0]) == 2
        assert converted_values[0][0]["id"] == 1
        assert len(converted_values[2]) == 0

    def test_map_with_map_list_value(self):
        """Test MAP containing MAP_LIST as a value."""
        test_dict = {
            "metadata": {"version": "1.0"},
            "items": [{"name": "item1", "count": 5}, {"name": "item2", "count": 3}],
        }

        protos = python_values_to_proto_values([test_dict], ValueType.MAP)
        converted = feast_value_type_to_python_type(protos[0])

        assert converted["metadata"]["version"] == "1.0"
        assert len(converted["items"]) == 2
        assert converted["items"][0]["name"] == "item1"
        assert converted["items"][1]["count"] == 3

    @pytest.mark.parametrize(
        "invalid_value",
        [
            [{"key": "value"}, "not_a_dict", {"another": "dict"}],
            ["string1", "string2"],
            [1, 2, 3],
        ],
    )
    def test_map_list_with_invalid_items(self, invalid_value):
        """Test that MAP_LIST with non-dict items raises appropriate errors."""
        with pytest.raises((ValueError, TypeError)):
            python_values_to_proto_values([invalid_value], ValueType.MAP_LIST)

    def test_direct_proto_construction(self):
        """Test direct construction of Map and MapList proto messages."""
        # Test Map proto construction
        test_dict = {"key1": "value1", "key2": 42}
        map_proto = _python_dict_to_map_proto(test_dict)

        assert isinstance(map_proto, Map)
        assert len(map_proto.val) == 2

        # Test MapList proto construction
        test_list = [{"a": 1}, {"b": 2}]
        map_list_proto = _python_list_to_map_list_proto(test_list)

        assert isinstance(map_list_proto, MapList)
        assert len(map_list_proto.val) == 2

    def test_roundtrip_conversion_consistency(self):
        """Test that roundtrip conversion maintains data integrity."""
        original_map = {
            "string": "hello",
            "integer": 42,
            "float": 3.14159,
            "boolean": True,
            "nested": {"inner_string": "world", "inner_list": [1, 2, 3]},
            "list_of_maps": [{"item": "first"}, {"item": "second"}],
        }

        # Convert to proto and back
        protos = python_values_to_proto_values([original_map], ValueType.MAP)
        converted = feast_value_type_to_python_type(protos[0])

        # Verify all data is preserved
        assert converted["string"] == original_map["string"]
        assert converted["integer"] == original_map["integer"]
        assert converted["float"] == original_map["float"]
        assert converted["boolean"] == original_map["boolean"]
        assert (
            converted["nested"]["inner_string"]
            == original_map["nested"]["inner_string"]
        )
        assert converted["nested"]["inner_list"] == original_map["nested"]["inner_list"]
        assert len(converted["list_of_maps"]) == len(original_map["list_of_maps"])
        assert converted["list_of_maps"][0]["item"] == "first"
        assert converted["list_of_maps"][1]["item"] == "second"


class TestSetTypes:
    """Test cases for SET value types."""

    def test_simple_set_conversion(self):
        """Test basic SET type conversion from Python set to proto and back."""
        test_set = {1, 2, 3, 4, 5}

        protos = python_values_to_proto_values([test_set], ValueType.INT32_SET)
        converted = feast_value_type_to_python_type(protos[0])

        assert isinstance(converted, set)
        assert converted == test_set

    def test_string_set_conversion(self):
        """Test STRING_SET type conversion."""
        test_set = {"apple", "banana", "cherry"}

        protos = python_values_to_proto_values([test_set], ValueType.STRING_SET)
        converted = feast_value_type_to_python_type(protos[0])

        assert isinstance(converted, set)
        assert converted == test_set

    def test_float_set_conversion(self):
        """Test FLOAT_SET type conversion."""
        test_set = {1.5, 2.5, 3.5}

        protos = python_values_to_proto_values([test_set], ValueType.FLOAT_SET)
        converted = feast_value_type_to_python_type(protos[0])

        assert isinstance(converted, set)
        assert converted == test_set

    def test_bool_set_conversion(self):
        """Test BOOL_SET type conversion."""
        test_set = {True, False}

        protos = python_values_to_proto_values([test_set], ValueType.BOOL_SET)
        converted = feast_value_type_to_python_type(protos[0])

        assert isinstance(converted, set)
        assert converted == test_set

    def test_set_from_list_with_duplicates(self):
        """Test that duplicate values in lists are removed when converted to sets."""
        test_list = [1, 2, 2, 3, 3, 3, 4, 5, 5]

        protos = python_values_to_proto_values([test_list], ValueType.INT32_SET)
        converted = feast_value_type_to_python_type(protos[0])

        assert isinstance(converted, set)
        assert converted == {1, 2, 3, 4, 5}

    def test_empty_set(self):
        """Test empty SET conversion."""
        test_set = set()

        protos = python_values_to_proto_values([test_set], ValueType.STRING_SET)
        converted = feast_value_type_to_python_type(protos[0])

        assert isinstance(converted, set)
        assert len(converted) == 0

    def test_null_set(self):
        """Test None SET conversion."""
        protos = python_values_to_proto_values([None], ValueType.INT32_SET)
        converted = feast_value_type_to_python_type(protos[0])

        assert converted is None

    def test_multiple_set_values(self):
        """Test conversion of multiple set values."""
        test_sets = [{1, 2, 3}, {4, 5}, {6}]

        protos = python_values_to_proto_values(test_sets, ValueType.INT32_SET)

        assert len(protos) == 3
        assert feast_value_type_to_python_type(protos[0]) == {1, 2, 3}
        assert feast_value_type_to_python_type(protos[1]) == {4, 5}
        assert feast_value_type_to_python_type(protos[2]) == {6}


class TestUuidTypes:
    """Test cases for UUID and TIME_UUID value types."""

    def test_uuid_string_roundtrip(self):
        """UUID string -> proto -> uuid.UUID object roundtrip."""
        test_uuid = uuid.uuid4()
        protos = python_values_to_proto_values([str(test_uuid)], ValueType.UUID)
        assert protos[0].uuid_val == str(test_uuid)

        result = feast_value_type_to_python_type(protos[0])
        assert isinstance(result, uuid.UUID)
        assert result == test_uuid

    def test_uuid_object_serialization(self):
        """uuid.UUID object -> proto serialization (str conversion automatic)."""
        test_uuid = uuid.uuid4()
        protos = python_values_to_proto_values([test_uuid], ValueType.UUID)
        assert protos[0].uuid_val == str(test_uuid)

    def test_time_uuid_roundtrip(self):
        """TIME_UUID string -> proto -> uuid.UUID object roundtrip."""
        test_uuid = uuid.uuid1()
        protos = python_values_to_proto_values([str(test_uuid)], ValueType.TIME_UUID)
        assert protos[0].time_uuid_val == str(test_uuid)

        result = feast_value_type_to_python_type(protos[0])
        assert isinstance(result, uuid.UUID)
        assert result == test_uuid

    def test_uuid_without_feature_type_returns_uuid(self):
        """With dedicated uuid_val proto field, UUID is identified without feature_type hint."""
        test_uuid = uuid.uuid4()
        protos = python_values_to_proto_values([str(test_uuid)], ValueType.UUID)

        # No feature_type hint needed — uuid_val field identifies the type
        result = feast_value_type_to_python_type(protos[0])
        assert isinstance(result, uuid.UUID)
        assert result == test_uuid

    def test_uuid_backward_compat_string_val(self):
        """UUIDs stored as string_val (old format) still work with feature_type hint."""
        from feast.protos.feast.types.Value_pb2 import Value as ProtoValue

        test_uuid = uuid.uuid4()
        # Simulate old-format proto with string_val
        proto = ProtoValue(string_val=str(test_uuid))

        # Without feature_type, returns plain string
        result = feast_value_type_to_python_type(proto)
        assert isinstance(result, str)

        # With feature_type hint, returns uuid.UUID
        result = feast_value_type_to_python_type(proto, ValueType.UUID)
        assert isinstance(result, uuid.UUID)
        assert result == test_uuid

    def test_uuid_list_roundtrip(self):
        """UUID list -> proto -> list of uuid.UUID objects roundtrip."""
        test_uuids = [uuid.uuid4(), uuid.uuid4()]
        test_uuid_strs = [str(u) for u in test_uuids]
        protos = python_values_to_proto_values([test_uuid_strs], ValueType.UUID_LIST)
        result = feast_value_type_to_python_type(protos[0])
        assert all(isinstance(r, uuid.UUID) for r in result)
        assert result == test_uuids

    def test_pg_uuid_type_mapping(self):
        """PostgreSQL uuid type maps to ValueType.UUID."""
        assert pg_type_to_feast_value_type("uuid") == ValueType.UUID
        assert pg_type_to_feast_value_type("uuid[]") == ValueType.UUID_LIST
