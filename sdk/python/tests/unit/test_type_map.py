import numpy as np
import pandas as pd
import pyarrow
import pytest

from feast.protos.feast.types.Value_pb2 import Map, MapList
from feast.type_map import (
    _convert_value_type_str_to_value_type,
    _python_dict_to_map_proto,
    _python_list_to_map_list_proto,
    arrow_to_pg_type,
    feast_value_type_to_pa,
    feast_value_type_to_python_type,
    pa_to_feast_value_type,
    pa_to_redshift_value_type,
    pg_type_to_feast_value_type,
    python_type_to_feast_value_type,
    python_values_to_proto_values,
    redshift_to_feast_value_type,
    snowflake_type_to_feast_value_type,
    spark_to_feast_value_type,
)
from feast.types import Array, from_feast_to_pyarrow_type
from feast.types import Map as FeastMap
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


class TestMapArrowTypeSupport:
    """Test cases for MAP and MAP_LIST Arrow type conversions."""

    def test_feast_value_type_to_pa_map(self):
        """Test that ValueType.MAP converts to a PyArrow map type."""
        pa_type = feast_value_type_to_pa(ValueType.MAP)
        assert isinstance(pa_type, pyarrow.MapType)
        assert pa_type.key_type == pyarrow.string()

    def test_feast_value_type_to_pa_map_list(self):
        """Test that ValueType.MAP_LIST converts to a PyArrow list of maps."""
        pa_type = feast_value_type_to_pa(ValueType.MAP_LIST)
        assert isinstance(pa_type, pyarrow.ListType)
        assert isinstance(pa_type.value_type, pyarrow.MapType)

    def test_pa_to_feast_value_type_map(self):
        """Test that PyArrow map type string converts to ValueType.MAP."""
        result = pa_to_feast_value_type("map<string, string>")
        assert result == ValueType.MAP

    def test_pa_to_feast_value_type_map_various_value_types(self):
        """Test that various PyArrow map type strings all convert to MAP."""
        assert pa_to_feast_value_type("map<string, int64>") == ValueType.MAP
        assert pa_to_feast_value_type("map<string, double>") == ValueType.MAP
        assert pa_to_feast_value_type("map<string, binary>") == ValueType.MAP

    def test_from_feast_to_pyarrow_type_map(self):
        """Test that Feast Map type converts to PyArrow map type."""
        pa_type = from_feast_to_pyarrow_type(FeastMap)
        assert isinstance(pa_type, pyarrow.MapType)

    def test_from_feast_to_pyarrow_type_array_map(self):
        """Test that Feast Array(Map) converts to PyArrow list of maps."""
        pa_type = from_feast_to_pyarrow_type(Array(FeastMap))
        assert isinstance(pa_type, pyarrow.ListType)
        assert isinstance(pa_type.value_type, pyarrow.MapType)

    def test_convert_value_type_str_map(self):
        """Test that 'MAP' string converts to ValueType.MAP."""
        assert _convert_value_type_str_to_value_type("MAP") == ValueType.MAP

    def test_convert_value_type_str_map_list(self):
        """Test that 'MAP_LIST' string converts to ValueType.MAP_LIST."""
        assert _convert_value_type_str_to_value_type("MAP_LIST") == ValueType.MAP_LIST

    def test_arrow_to_pg_type_map(self):
        """Test that Arrow map type converts to Postgres jsonb."""
        assert arrow_to_pg_type("map<string, string>") == "jsonb"
        assert arrow_to_pg_type("map<string, int64>") == "jsonb"

    def test_pg_type_to_feast_value_type_json(self):
        """Test that Postgres json/jsonb types convert to ValueType.MAP."""
        assert pg_type_to_feast_value_type("json") == ValueType.MAP
        assert pg_type_to_feast_value_type("jsonb") == ValueType.MAP

    def test_pg_type_to_feast_value_type_json_array(self):
        """Test that Postgres json[]/jsonb[] types convert to ValueType.MAP_LIST."""
        assert pg_type_to_feast_value_type("json[]") == ValueType.MAP_LIST
        assert pg_type_to_feast_value_type("jsonb[]") == ValueType.MAP_LIST

    def test_snowflake_variant_to_map(self):
        """Test that Snowflake VARIANT/OBJECT types convert to ValueType.MAP."""
        assert snowflake_type_to_feast_value_type("VARIANT") == ValueType.MAP
        assert snowflake_type_to_feast_value_type("OBJECT") == ValueType.MAP

    def test_redshift_super_to_map(self):
        """Test that Redshift super type converts to ValueType.MAP."""
        assert redshift_to_feast_value_type("super") == ValueType.MAP

    def test_map_roundtrip_proto_to_arrow_type(self):
        """Test that MAP type survives a full conversion roundtrip."""
        pa_type = feast_value_type_to_pa(ValueType.MAP)
        pa_type_str = str(pa_type)
        roundtrip = pa_to_feast_value_type(pa_type_str)
        assert roundtrip == ValueType.MAP

    def test_spark_map_to_feast(self):
        """Test that Spark map types convert to ValueType.MAP."""
        assert spark_to_feast_value_type("map<string,string>") == ValueType.MAP
        assert spark_to_feast_value_type("map<string,int>") == ValueType.MAP
        assert spark_to_feast_value_type("MAP<STRING,DOUBLE>") == ValueType.MAP

    def test_spark_array_map_to_feast(self):
        """Test that Spark array<map<...>> types convert to ValueType.MAP_LIST."""
        assert (
            spark_to_feast_value_type("array<map<string,string>>") == ValueType.MAP_LIST
        )

    def test_spark_unknown_still_returns_null(self):
        """Test that unrecognized Spark types still return NULL."""
        assert spark_to_feast_value_type("interval") == ValueType.NULL

    def test_spark_struct_to_feast_struct(self):
        """Test that Spark struct types now convert to ValueType.STRUCT."""
        assert spark_to_feast_value_type("struct<a:int>") == ValueType.STRUCT


class TestEnableValidationOnFeatureView:
    """Test that enable_validation is a real parameter on FeatureView."""

    def test_feature_view_has_enable_validation_default_false(self):
        """Test that FeatureView has enable_validation defaulting to False."""
        import inspect

        from feast.feature_view import FeatureView

        sig = inspect.signature(FeatureView.__init__)
        assert "enable_validation" in sig.parameters
        assert sig.parameters["enable_validation"].default is False

    def test_batch_feature_view_has_enable_validation(self):
        """Test that BatchFeatureView has enable_validation parameter."""
        import inspect

        from feast.batch_feature_view import BatchFeatureView

        sig = inspect.signature(BatchFeatureView.__init__)
        assert "enable_validation" in sig.parameters
        assert sig.parameters["enable_validation"].default is False

    def test_stream_feature_view_has_enable_validation(self):
        """Test that StreamFeatureView has enable_validation parameter."""
        import inspect

        from feast.stream_feature_view import StreamFeatureView

        sig = inspect.signature(StreamFeatureView.__init__)
        assert "enable_validation" in sig.parameters
        assert sig.parameters["enable_validation"].default is False


class TestRedshiftDynamoDBMapSupport:
    """Test cases for DynamoDB + Redshift map type round-trips."""

    def test_pa_to_redshift_value_type_map(self):
        """Test that Arrow map type maps to Redshift 'super' type."""
        pa_type = feast_value_type_to_pa(ValueType.MAP)
        assert pa_to_redshift_value_type(pa_type) == "super"

    def test_pa_to_redshift_value_type_map_list(self):
        """Test that Arrow list-of-map type maps to Redshift 'super' type."""
        pa_type = feast_value_type_to_pa(ValueType.MAP_LIST)
        assert pa_to_redshift_value_type(pa_type) == "super"

    def test_json_string_to_map_proto(self):
        """Test that JSON strings are parsed to MAP protos during materialization."""
        json_str = '{"key1": "value1", "key2": "value2"}'
        protos = python_values_to_proto_values([json_str], ValueType.MAP)
        converted = feast_value_type_to_python_type(protos[0])
        assert isinstance(converted, dict)
        assert converted["key1"] == "value1"
        assert converted["key2"] == "value2"

    def test_json_string_to_map_list_proto(self):
        """Test that JSON strings are parsed to MAP_LIST protos during materialization."""
        json_str = '[{"a": "1"}, {"b": "2"}]'
        protos = python_values_to_proto_values([json_str], ValueType.MAP_LIST)
        converted = feast_value_type_to_python_type(protos[0])
        assert isinstance(converted, list)
        assert len(converted) == 2
        assert converted[0]["a"] == "1"

    def test_dict_still_works_for_map(self):
        """Test that regular Python dicts still work for MAP (no regression)."""
        test_dict = {"x": "y", "a": 1}
        protos = python_values_to_proto_values([test_dict], ValueType.MAP)
        converted = feast_value_type_to_python_type(protos[0])
        assert isinstance(converted, dict)
        assert converted["x"] == "y"

    def test_none_map_still_works(self):
        """Test that None MAP values still produce empty proto (no regression)."""
        protos = python_values_to_proto_values([None], ValueType.MAP)
        converted = feast_value_type_to_python_type(protos[0])
        assert converted is None

    def test_redshift_super_roundtrip(self):
        """Test full type conversion roundtrip: Redshift super → Feast MAP → Arrow → Redshift super."""
        feast_type = redshift_to_feast_value_type("super")
        assert feast_type == ValueType.MAP
        pa_type = feast_value_type_to_pa(feast_type)
        redshift_type = pa_to_redshift_value_type(pa_type)
        assert redshift_type == "super"


class TestJsonTypeSupport:
    """Test cases for JSON value type."""

    def test_simple_json_conversion(self):
        """Test basic JSON type conversion: Python dict -> proto (json_val) -> Python."""
        test_data = {"name": "Alice", "age": 30, "active": True}
        protos = python_values_to_proto_values([test_data], ValueType.JSON)
        converted = feast_value_type_to_python_type(protos[0])

        assert isinstance(converted, dict)
        assert converted["name"] == "Alice"
        assert converted["age"] == 30
        assert converted["active"] is True

    def test_json_string_passthrough(self):
        """Test that a raw JSON string is stored and returned correctly."""
        json_str = '{"key": "value", "count": 42}'
        protos = python_values_to_proto_values([json_str], ValueType.JSON)
        converted = feast_value_type_to_python_type(protos[0])

        assert isinstance(converted, dict)
        assert converted["key"] == "value"
        assert converted["count"] == 42

    def test_json_array_value(self):
        """Test JSON type with an array as the top-level value."""
        test_data = [1, 2, 3, "four"]
        protos = python_values_to_proto_values([test_data], ValueType.JSON)
        converted = feast_value_type_to_python_type(protos[0])

        assert isinstance(converted, list)
        assert converted == [1, 2, 3, "four"]

    def test_json_nested(self):
        """Test deeply nested JSON structures."""
        test_data = {
            "level1": {"level2": {"level3": {"value": "deep"}}},
            "array": [{"a": 1}, {"b": 2}],
        }
        protos = python_values_to_proto_values([test_data], ValueType.JSON)
        converted = feast_value_type_to_python_type(protos[0])

        assert converted["level1"]["level2"]["level3"]["value"] == "deep"
        assert converted["array"][0]["a"] == 1

    def test_null_json(self):
        """Test None JSON conversion."""
        protos = python_values_to_proto_values([None], ValueType.JSON)
        converted = feast_value_type_to_python_type(protos[0])
        assert converted is None

    def test_json_list_conversion(self):
        """Test JSON_LIST type conversion."""
        test_data = [
            {"name": "Alice"},
            '{"name": "Bob"}',
            {"count": 42},
        ]
        protos = python_values_to_proto_values([test_data], ValueType.JSON_LIST)
        converted = feast_value_type_to_python_type(protos[0])

        assert isinstance(converted, list)
        assert len(converted) == 3
        assert converted[0] == {"name": "Alice"}
        assert converted[1] == {"name": "Bob"}
        assert converted[2] == {"count": 42}

    def test_null_json_list(self):
        """Test None JSON_LIST conversion."""
        protos = python_values_to_proto_values([None], ValueType.JSON_LIST)
        converted = feast_value_type_to_python_type(protos[0])
        assert converted is None

    def test_multiple_json_values(self):
        """Test conversion of multiple JSON values."""
        test_values = [
            {"x": 1},
            {"y": 2},
            None,
            {"z": 3},
        ]
        protos = python_values_to_proto_values(test_values, ValueType.JSON)
        converted = [feast_value_type_to_python_type(p) for p in protos]

        assert converted[0] == {"x": 1}
        assert converted[1] == {"y": 2}
        assert converted[2] is None
        assert converted[3] == {"z": 3}

    def test_feast_value_type_to_pa_json(self):
        """Test that ValueType.JSON converts to PyArrow large_string."""
        pa_type = feast_value_type_to_pa(ValueType.JSON)
        assert pa_type == pyarrow.large_string()

    def test_feast_value_type_to_pa_json_list(self):
        """Test that ValueType.JSON_LIST converts to PyArrow list of large_string."""
        pa_type = feast_value_type_to_pa(ValueType.JSON_LIST)
        assert isinstance(pa_type, pyarrow.ListType)
        assert pa_type.value_type == pyarrow.large_string()

    def test_convert_value_type_str_json(self):
        """Test that 'JSON' string converts to ValueType.JSON."""
        assert _convert_value_type_str_to_value_type("JSON") == ValueType.JSON
        assert _convert_value_type_str_to_value_type("JSON_LIST") == ValueType.JSON_LIST

    def test_arrow_to_pg_type_json(self):
        """Test that Arrow large_string converts to Postgres jsonb."""
        assert arrow_to_pg_type("large_string") == "jsonb"

    def test_bq_json_to_feast(self):
        """Test that BigQuery JSON type converts to ValueType.JSON."""
        from feast.type_map import bq_to_feast_value_type

        assert bq_to_feast_value_type("JSON") == ValueType.JSON

    def test_spark_struct_not_json(self):
        """Test that Spark struct types map to STRUCT not JSON."""
        assert spark_to_feast_value_type("struct<a:int,b:string>") == ValueType.STRUCT

    def test_snowflake_json_to_feast(self):
        """Test that Snowflake JSON type converts to ValueType.JSON."""
        assert snowflake_type_to_feast_value_type("JSON") == ValueType.JSON

    def test_json_feast_type_aliases(self):
        """Test Json FeastType alias and conversions."""
        from feast.types import Json, from_feast_to_pyarrow_type

        pa_type = from_feast_to_pyarrow_type(Json)
        assert pa_type == pyarrow.large_string()

    def test_json_value_types_mapping(self):
        """Test JSON types in VALUE_TYPES_TO_FEAST_TYPES."""
        from feast.types import VALUE_TYPES_TO_FEAST_TYPES, Json

        assert VALUE_TYPES_TO_FEAST_TYPES[ValueType.JSON] == Json

    def test_pa_to_feast_value_type_large_string(self):
        """Test that large_string arrow type converts to ValueType.JSON."""
        result = pa_to_feast_value_type("large_string")
        assert result == ValueType.JSON


class TestStructTypeSupport:
    """Test cases for STRUCT value type."""

    def test_simple_struct_conversion(self):
        """Test basic STRUCT type conversion: Python dict -> proto (struct_val) -> Python dict."""
        test_data = {"name": "Alice", "age": 30}
        protos = python_values_to_proto_values([test_data], ValueType.STRUCT)
        converted = feast_value_type_to_python_type(protos[0])

        assert isinstance(converted, dict)
        assert converted["name"] == "Alice"
        assert converted["age"] == 30

    def test_nested_struct_conversion(self):
        """Test nested STRUCT type conversion."""
        test_data = {
            "address": {"street": "123 Main St", "city": "NYC"},
            "name": "Alice",
        }
        protos = python_values_to_proto_values([test_data], ValueType.STRUCT)
        converted = feast_value_type_to_python_type(protos[0])

        assert converted["address"]["street"] == "123 Main St"
        assert converted["address"]["city"] == "NYC"
        assert converted["name"] == "Alice"

    def test_null_struct(self):
        """Test None STRUCT conversion."""
        protos = python_values_to_proto_values([None], ValueType.STRUCT)
        converted = feast_value_type_to_python_type(protos[0])
        assert converted is None

    def test_struct_list_conversion(self):
        """Test STRUCT_LIST type conversion."""
        test_data = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ]
        protos = python_values_to_proto_values([test_data], ValueType.STRUCT_LIST)
        converted = feast_value_type_to_python_type(protos[0])

        assert isinstance(converted, list)
        assert len(converted) == 2
        assert converted[0]["name"] == "Alice"
        assert converted[1]["age"] == 25

    def test_null_struct_list(self):
        """Test None STRUCT_LIST conversion."""
        protos = python_values_to_proto_values([None], ValueType.STRUCT_LIST)
        converted = feast_value_type_to_python_type(protos[0])
        assert converted is None

    def test_multiple_struct_values(self):
        """Test conversion of multiple STRUCT values."""
        test_values = [
            {"x": 1},
            None,
            {"y": 2, "z": 3},
        ]
        protos = python_values_to_proto_values(test_values, ValueType.STRUCT)
        converted = [feast_value_type_to_python_type(p) for p in protos]

        assert converted[0] == {"x": 1}
        assert converted[1] is None
        assert converted[2] == {"y": 2, "z": 3}

    def test_struct_class_creation(self):
        """Test Struct FeastType creation and validation."""
        from feast.types import Int32, String, Struct

        struct_type = Struct({"name": String, "age": Int32})
        assert struct_type.to_value_type() == ValueType.STRUCT
        assert "name" in struct_type.fields
        assert struct_type.fields["name"] == String
        assert struct_type.fields["age"] == Int32

    def test_struct_empty_raises(self):
        """Test that empty Struct raises ValueError."""
        from feast.types import Struct

        with pytest.raises(ValueError, match="at least one field"):
            Struct({})

    def test_struct_to_pyarrow(self):
        """Test Struct type converts to PyArrow struct."""
        from feast.types import Int32, String, Struct

        struct_type = Struct({"name": String, "age": Int32})
        pa_type = struct_type.to_pyarrow_type()

        assert pyarrow.types.is_struct(pa_type)
        assert pa_type.get_field_index("name") >= 0
        assert pa_type.get_field_index("age") >= 0

    def test_struct_from_feast_to_pyarrow(self):
        """Test from_feast_to_pyarrow_type handles Struct."""
        from feast.types import Int32, String, Struct

        struct_type = Struct({"name": String, "age": Int32})
        pa_type = from_feast_to_pyarrow_type(struct_type)

        assert pyarrow.types.is_struct(pa_type)

    def test_array_of_struct(self):
        """Test Array(Struct(...)) works."""
        from feast.types import Array, Int32, String, Struct

        struct_type = Struct({"name": String, "value": Int32})
        array_type = Array(struct_type)

        assert array_type.to_value_type() == ValueType.STRUCT_LIST
        pa_type = from_feast_to_pyarrow_type(array_type)
        assert isinstance(pa_type, pyarrow.ListType)
        assert pyarrow.types.is_struct(pa_type.value_type)

    def test_feast_value_type_to_pa_struct(self):
        """Test that ValueType.STRUCT converts to PyArrow struct (empty default)."""
        pa_type = feast_value_type_to_pa(ValueType.STRUCT)
        assert pyarrow.types.is_struct(pa_type)

    def test_feast_value_type_to_pa_struct_list(self):
        """Test that ValueType.STRUCT_LIST converts to PyArrow list of struct."""
        pa_type = feast_value_type_to_pa(ValueType.STRUCT_LIST)
        assert isinstance(pa_type, pyarrow.ListType)
        assert pyarrow.types.is_struct(pa_type.value_type)

    def test_convert_value_type_str_struct(self):
        """Test that 'STRUCT' string converts to ValueType.STRUCT."""
        assert _convert_value_type_str_to_value_type("STRUCT") == ValueType.STRUCT
        assert (
            _convert_value_type_str_to_value_type("STRUCT_LIST")
            == ValueType.STRUCT_LIST
        )

    def test_spark_struct_to_feast(self):
        """Test that Spark struct types convert to ValueType.STRUCT."""
        assert spark_to_feast_value_type("struct<a:int,b:string>") == ValueType.STRUCT
        assert spark_to_feast_value_type("STRUCT<name:string>") == ValueType.STRUCT

    def test_spark_array_struct_to_feast(self):
        """Test that Spark array<struct<...>> types convert to STRUCT_LIST."""
        assert (
            spark_to_feast_value_type("array<struct<a:int>>") == ValueType.STRUCT_LIST
        )

    def test_bq_struct_to_feast(self):
        """Test that BigQuery STRUCT/RECORD types convert to ValueType.STRUCT."""
        from feast.type_map import bq_to_feast_value_type

        assert bq_to_feast_value_type("STRUCT") == ValueType.STRUCT
        assert bq_to_feast_value_type("RECORD") == ValueType.STRUCT

    def test_pa_to_feast_value_type_struct(self):
        """Test that struct arrow type string converts to ValueType.STRUCT."""
        result = pa_to_feast_value_type("struct<name: string, age: int32>")
        assert result == ValueType.STRUCT

    def test_struct_schema_persistence(self):
        """Test that Struct schema is preserved through Field serialization/deserialization."""
        from feast.field import Field
        from feast.types import Int32, String, Struct

        struct_type = Struct({"street": String, "zip": Int32})
        field = Field(name="address", dtype=struct_type)

        proto = field.to_proto()
        restored = Field.from_proto(proto)

        assert isinstance(restored.dtype, Struct)
        assert "street" in restored.dtype.fields
        assert "zip" in restored.dtype.fields
        assert restored.dtype.fields["street"] == String
        assert restored.dtype.fields["zip"] == Int32

    def test_struct_json_string_parsing(self):
        """Test that JSON string input is parsed for STRUCT type."""
        json_str = '{"name": "Alice", "score": 95}'
        protos = python_values_to_proto_values([json_str], ValueType.STRUCT)
        converted = feast_value_type_to_python_type(protos[0])

        assert isinstance(converted, dict)
        assert converted["name"] == "Alice"
        assert converted["score"] == 95

    def test_struct_equality(self):
        """Test Struct type equality."""
        from feast.types import Int32, String, Struct

        s1 = Struct({"name": String, "age": Int32})
        s2 = Struct({"name": String, "age": Int32})
        s3 = Struct({"name": String})

        assert s1 == s2
        assert s1 != s3

    def test_from_feast_type_struct(self):
        """Test from_feast_type works for Struct."""
        from feast.types import Int32, String, Struct, from_feast_type

        struct_type = Struct({"name": String, "age": Int32})
        value_type = from_feast_type(struct_type)
        assert value_type == ValueType.STRUCT

    def test_from_value_type_struct(self):
        """Test from_value_type works for STRUCT (returns placeholder)."""
        from feast.types import Struct, from_value_type

        feast_type = from_value_type(ValueType.STRUCT)
        assert isinstance(feast_type, Struct)

    def test_from_value_type_struct_list(self):
        """Test from_value_type works for STRUCT_LIST (returns placeholder Array(Struct))."""
        from feast.types import Array, Struct, from_value_type

        feast_type = from_value_type(ValueType.STRUCT_LIST)
        assert isinstance(feast_type, Array)
        assert isinstance(feast_type.base_type, Struct)


class TestJsonValidation:
    """Test JSON well-formedness validation."""

    def test_proto_conversion_valid_json_string(self):
        """Valid JSON strings should convert without error."""
        valid_json = '{"key": "value", "num": 42}'
        protos = python_values_to_proto_values([valid_json], ValueType.JSON)
        assert protos[0].json_val == valid_json

    def test_proto_conversion_invalid_json_string_raises(self):
        """Invalid JSON strings should raise ValueError during proto conversion."""
        import pytest

        invalid_json = "this is not json {{"
        with pytest.raises(ValueError, match="Invalid JSON string for JSON type"):
            python_values_to_proto_values([invalid_json], ValueType.JSON)

    def test_proto_conversion_dict_no_validation_needed(self):
        """Python dicts are valid by definition and should not raise."""
        data = {"name": "Alice", "items": [1, 2, 3]}
        protos = python_values_to_proto_values([data], ValueType.JSON)
        converted = feast_value_type_to_python_type(protos[0])
        assert converted == data

    def test_proto_conversion_list_no_validation_needed(self):
        """Python lists are valid by definition and should not raise."""
        data = [1, "two", {"three": 3}]
        protos = python_values_to_proto_values([data], ValueType.JSON)
        converted = feast_value_type_to_python_type(protos[0])
        assert converted == data

    def test_proto_conversion_none_passes(self):
        """None values should pass through without validation."""
        protos = python_values_to_proto_values([None], ValueType.JSON)
        converted = feast_value_type_to_python_type(protos[0])
        assert converted is None

    def test_proto_conversion_json_list_invalid_string_raises(self):
        """Invalid JSON strings in JSON_LIST should raise ValueError."""
        import pytest

        data = ['{"valid": true}', "not-json"]
        with pytest.raises(ValueError, match="Invalid JSON string in JSON_LIST"):
            python_values_to_proto_values([data], ValueType.JSON_LIST)

    def test_proto_conversion_json_list_valid_mixed(self):
        """JSON_LIST with valid strings and dicts should succeed."""
        data = ['{"a": 1}', {"b": 2}]
        protos = python_values_to_proto_values([data], ValueType.JSON_LIST)
        converted = feast_value_type_to_python_type(protos[0])
        assert len(converted) == 2
        assert converted[0] == {"a": 1}
        assert converted[1] == {"b": 2}

    def test_proto_conversion_json_scalar_string(self):
        """JSON scalar values like numbers-as-strings should validate."""
        protos = python_values_to_proto_values(["42"], ValueType.JSON)
        converted = feast_value_type_to_python_type(protos[0])
        assert converted == 42

    def test_proto_conversion_json_null_string(self):
        """The JSON string 'null' is valid JSON."""
        protos = python_values_to_proto_values(["null"], ValueType.JSON)
        converted = feast_value_type_to_python_type(protos[0])
        assert converted is None

    def test_proto_conversion_json_empty_string_raises(self):
        """An empty string is not valid JSON."""
        import pytest

        with pytest.raises(ValueError, match="Invalid JSON string for JSON type"):
            python_values_to_proto_values([""], ValueType.JSON)

    def test_local_validation_node_valid_json(self):
        """LocalValidationNode should accept valid JSON strings."""
        from feast.infra.compute_engines.local.nodes import LocalValidationNode

        table = pyarrow.table(
            {"config": ['{"a": 1}', '{"b": 2}', "null"]},
            schema=pyarrow.schema([pyarrow.field("config", pyarrow.string())]),
        )

        node = LocalValidationNode(
            name="test_validate",
            validation_config={
                "columns": {"config": pyarrow.large_string()},
                "json_columns": {"config"},
            },
            backend=None,
        )
        # Should not raise
        node._validate_schema(table)

    def test_local_validation_node_invalid_json(self):
        """LocalValidationNode should reject invalid JSON strings."""
        import pytest

        from feast.infra.compute_engines.local.nodes import LocalValidationNode

        table = pyarrow.table(
            {"config": ['{"valid": true}', "not-json-at-all", '{"ok": 1}']},
            schema=pyarrow.schema([pyarrow.field("config", pyarrow.string())]),
        )

        node = LocalValidationNode(
            name="test_validate",
            validation_config={
                "columns": {"config": pyarrow.large_string()},
                "json_columns": {"config"},
            },
            backend=None,
        )
        with pytest.raises(ValueError, match="invalid JSON value"):
            node._validate_schema(table)

    def test_local_validation_node_skips_nulls(self):
        """LocalValidationNode should skip null values in JSON columns."""
        from feast.infra.compute_engines.local.nodes import LocalValidationNode

        table = pyarrow.table(
            {"config": ['{"a": 1}', None, '{"b": 2}']},
            schema=pyarrow.schema([pyarrow.field("config", pyarrow.string())]),
        )

        node = LocalValidationNode(
            name="test_validate",
            validation_config={
                "columns": {"config": pyarrow.large_string()},
                "json_columns": {"config"},
            },
            backend=None,
        )
        # Should not raise
        node._validate_schema(table)

    def test_local_validation_node_no_json_columns(self):
        """LocalValidationNode should skip JSON validation if no json_columns."""
        from feast.infra.compute_engines.local.nodes import LocalValidationNode

        table = pyarrow.table(
            {"data": ["not-json"]},
            schema=pyarrow.schema([pyarrow.field("data", pyarrow.string())]),
        )

        node = LocalValidationNode(
            name="test_validate",
            validation_config={
                "columns": {"data": pyarrow.string()},
            },
            backend=None,
        )
        # Should not raise — no json_columns configured
        node._validate_schema(table)

    def test_local_validation_node_error_message_shows_row_and_detail(self):
        """Error message should include the row number and parse error."""
        import pytest

        from feast.infra.compute_engines.local.nodes import LocalValidationNode

        table = pyarrow.table(
            {"config": ['{"ok": 1}', '{"ok": 2}', "{bad}"]},
            schema=pyarrow.schema([pyarrow.field("config", pyarrow.string())]),
        )

        node = LocalValidationNode(
            name="test_validate",
            validation_config={
                "columns": {"config": pyarrow.large_string()},
                "json_columns": {"config"},
            },
            backend=None,
        )
        with pytest.raises(ValueError, match="row 2"):
            node._validate_schema(table)


class TestSparkNativeTypeValidation:
    """Test Spark-native type mapping and compatibility checking."""

    def test_feast_string_to_spark_string(self):
        from pyspark.sql.types import StringType

        from feast.infra.compute_engines.spark.nodes import from_feast_to_spark_type
        from feast.types import String

        assert from_feast_to_spark_type(String) == StringType()

    def test_feast_int32_to_spark_integer(self):
        from pyspark.sql.types import IntegerType

        from feast.infra.compute_engines.spark.nodes import from_feast_to_spark_type
        from feast.types import Int32

        assert from_feast_to_spark_type(Int32) == IntegerType()

    def test_feast_int64_to_spark_long(self):
        from pyspark.sql.types import LongType

        from feast.infra.compute_engines.spark.nodes import from_feast_to_spark_type
        from feast.types import Int64

        assert from_feast_to_spark_type(Int64) == LongType()

    def test_feast_float32_to_spark_float(self):
        from pyspark.sql.types import FloatType

        from feast.infra.compute_engines.spark.nodes import from_feast_to_spark_type
        from feast.types import Float32

        assert from_feast_to_spark_type(Float32) == FloatType()

    def test_feast_float64_to_spark_double(self):
        from pyspark.sql.types import DoubleType

        from feast.infra.compute_engines.spark.nodes import from_feast_to_spark_type
        from feast.types import Float64

        assert from_feast_to_spark_type(Float64) == DoubleType()

    def test_feast_bool_to_spark_boolean(self):
        from pyspark.sql.types import BooleanType

        from feast.infra.compute_engines.spark.nodes import from_feast_to_spark_type
        from feast.types import Bool

        assert from_feast_to_spark_type(Bool) == BooleanType()

    def test_feast_bytes_to_spark_binary(self):
        from pyspark.sql.types import BinaryType

        from feast.infra.compute_engines.spark.nodes import from_feast_to_spark_type
        from feast.types import Bytes

        assert from_feast_to_spark_type(Bytes) == BinaryType()

    def test_feast_timestamp_to_spark_timestamp(self):
        from pyspark.sql.types import TimestampType

        from feast.infra.compute_engines.spark.nodes import from_feast_to_spark_type
        from feast.types import UnixTimestamp

        assert from_feast_to_spark_type(UnixTimestamp) == TimestampType()

    def test_feast_map_to_spark_map(self):
        from pyspark.sql.types import MapType, StringType

        from feast.infra.compute_engines.spark.nodes import from_feast_to_spark_type
        from feast.types import Map

        assert from_feast_to_spark_type(Map) == MapType(StringType(), StringType())

    def test_feast_json_to_spark_string(self):
        from pyspark.sql.types import StringType

        from feast.infra.compute_engines.spark.nodes import from_feast_to_spark_type
        from feast.types import Json

        assert from_feast_to_spark_type(Json) == StringType()

    def test_feast_array_int_to_spark_array(self):
        from pyspark.sql.types import ArrayType, IntegerType

        from feast.infra.compute_engines.spark.nodes import from_feast_to_spark_type
        from feast.types import Array, Int32

        assert from_feast_to_spark_type(Array(Int32)) == ArrayType(IntegerType())

    def test_feast_array_map_to_spark_array(self):
        from pyspark.sql.types import ArrayType, MapType, StringType

        from feast.infra.compute_engines.spark.nodes import from_feast_to_spark_type
        from feast.types import Array, Map

        assert from_feast_to_spark_type(Array(Map)) == ArrayType(
            MapType(StringType(), StringType())
        )

    def test_feast_struct_to_spark_struct(self):
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        from feast.infra.compute_engines.spark.nodes import from_feast_to_spark_type
        from feast.types import Int32, String, Struct

        struct = Struct({"name": String, "age": Int32})
        expected = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )
        assert from_feast_to_spark_type(struct) == expected

    def test_feast_array_struct_to_spark_array_struct(self):
        from pyspark.sql.types import (
            ArrayType,
            IntegerType,
            StringType,
            StructField,
            StructType,
        )

        from feast.infra.compute_engines.spark.nodes import from_feast_to_spark_type
        from feast.types import Array, Int32, String, Struct

        struct = Struct({"name": String, "age": Int32})
        expected = ArrayType(
            StructType(
                [
                    StructField("name", StringType(), True),
                    StructField("age", IntegerType(), True),
                ]
            )
        )
        assert from_feast_to_spark_type(Array(struct)) == expected

    def test_unsupported_type_returns_none(self):
        from feast.infra.compute_engines.spark.nodes import from_feast_to_spark_type
        from feast.types import Invalid

        assert from_feast_to_spark_type(Invalid) is None

    # Compatibility tests

    def test_exact_match_compatible(self):
        from pyspark.sql.types import StringType

        from feast.infra.compute_engines.spark.nodes import _spark_types_compatible

        assert _spark_types_compatible(StringType(), StringType())

    def test_map_struct_compatible(self):
        from pyspark.sql.types import MapType, StringType, StructType

        from feast.infra.compute_engines.spark.nodes import _spark_types_compatible

        assert _spark_types_compatible(
            MapType(StringType(), StringType()), StructType([])
        )

    def test_struct_map_compatible(self):
        from pyspark.sql.types import MapType, StringType, StructType

        from feast.infra.compute_engines.spark.nodes import _spark_types_compatible

        assert _spark_types_compatible(
            StructType([]), MapType(StringType(), StringType())
        )

    def test_integer_long_widening_compatible(self):
        from pyspark.sql.types import IntegerType, LongType

        from feast.infra.compute_engines.spark.nodes import _spark_types_compatible

        assert _spark_types_compatible(IntegerType(), LongType())
        assert _spark_types_compatible(LongType(), IntegerType())

    def test_float_double_widening_compatible(self):
        from pyspark.sql.types import DoubleType, FloatType

        from feast.infra.compute_engines.spark.nodes import _spark_types_compatible

        assert _spark_types_compatible(FloatType(), DoubleType())
        assert _spark_types_compatible(DoubleType(), FloatType())

    def test_string_vs_integer_incompatible(self):
        from pyspark.sql.types import IntegerType, StringType

        from feast.infra.compute_engines.spark.nodes import _spark_types_compatible

        assert not _spark_types_compatible(StringType(), IntegerType())

    def test_bool_vs_double_incompatible(self):
        from pyspark.sql.types import BooleanType, DoubleType

        from feast.infra.compute_engines.spark.nodes import _spark_types_compatible

        assert not _spark_types_compatible(BooleanType(), DoubleType())

    def test_array_element_compatibility(self):
        from pyspark.sql.types import ArrayType, IntegerType, LongType

        from feast.infra.compute_engines.spark.nodes import _spark_types_compatible

        assert _spark_types_compatible(ArrayType(IntegerType()), ArrayType(LongType()))

    def test_array_element_incompatibility(self):
        from pyspark.sql.types import ArrayType, IntegerType, StringType

        from feast.infra.compute_engines.spark.nodes import _spark_types_compatible

        assert not _spark_types_compatible(
            ArrayType(StringType()), ArrayType(IntegerType())
        )
