import pyarrow as pa
import pytest

from feast import ValueType
from feast.infra.offline_stores.contrib.trino_offline_store.trino_type_map import (
    _trino_array_item_type,
    pa_to_trino_value_type,
    trino_to_feast_value_type,
    trino_to_pa_value_type,
)


class TestTrinoArrayItemType:
    def test_simple_type(self) -> None:
        assert _trino_array_item_type("array(bigint)") == "bigint"

    def test_parameterized_type(self) -> None:
        assert _trino_array_item_type("array(varchar(10))") == "varchar(10)"

    def test_parameterized_with_comma(self) -> None:
        assert _trino_array_item_type("array(decimal(10, 2))") == "decimal(10, 2)"

    def test_nested_array(self) -> None:
        assert _trino_array_item_type("array(array(varchar))") == "array(varchar)"

    def test_complex_row(self) -> None:
        assert (
            _trino_array_item_type("array(row(x bigint, y varchar(10)))")
            == "row(x bigint, y varchar(10))"
        )

    def test_not_an_array(self) -> None:
        assert _trino_array_item_type("varchar") is None

    def test_partial_prefix(self) -> None:
        assert _trino_array_item_type("array") is None
        assert _trino_array_item_type("array(") is None


class TestTrinoToFeastValueType:
    def test_simple_types(self) -> None:
        assert trino_to_feast_value_type("boolean") == ValueType.BOOL
        assert trino_to_feast_value_type("bigint") == ValueType.INT64
        assert trino_to_feast_value_type("integer") == ValueType.INT32
        assert trino_to_feast_value_type("int") == ValueType.INT32
        assert trino_to_feast_value_type("double") == ValueType.DOUBLE
        assert trino_to_feast_value_type("real") == ValueType.FLOAT
        assert trino_to_feast_value_type("date") == ValueType.STRING
        assert trino_to_feast_value_type("tinyint") == ValueType.INT32
        assert trino_to_feast_value_type("smallint") == ValueType.INT32

    def test_parameterized_varchar(self) -> None:
        assert trino_to_feast_value_type("varchar(10)") == ValueType.STRING

    def test_parameterized_char(self) -> None:
        assert trino_to_feast_value_type("char(10)") == ValueType.STRING
        assert trino_to_feast_value_type("char") == ValueType.STRING

    def test_timestamp_with_precision(self) -> None:
        assert trino_to_feast_value_type("timestamp(3)") == ValueType.UNIX_TIMESTAMP
        assert trino_to_feast_value_type("timestamp") == ValueType.UNIX_TIMESTAMP

    def test_decimal_with_precision(self) -> None:
        assert trino_to_feast_value_type("decimal(10, 2)") == ValueType.FLOAT
        assert trino_to_feast_value_type("decimal(38, 2)") == ValueType.DOUBLE
        assert trino_to_feast_value_type("decimal(32)") == ValueType.FLOAT
        assert trino_to_feast_value_type("decimal(33)") == ValueType.DOUBLE

    def test_bare_decimal(self) -> None:
        assert trino_to_feast_value_type("decimal") == ValueType.DOUBLE

    def test_binary_types(self) -> None:
        assert trino_to_feast_value_type("binary") == ValueType.STRING
        assert trino_to_feast_value_type("varbinary") == ValueType.STRING

    def test_json(self) -> None:
        assert trino_to_feast_value_type("json") == ValueType.STRING

    def test_unsupported_type(self) -> None:
        with pytest.raises(ValueError, match="Trino type not supported"):
            trino_to_feast_value_type("unknown_type")


class TestTrinoToPaValueType:
    def test_simple_types(self) -> None:
        assert trino_to_pa_value_type("boolean") == pa.bool_()
        assert trino_to_pa_value_type("bigint") == pa.int64()
        assert trino_to_pa_value_type("integer") == pa.int32()
        assert trino_to_pa_value_type("int") == pa.int32()
        assert trino_to_pa_value_type("double") == pa.float64()
        assert trino_to_pa_value_type("real") == pa.float32()
        assert trino_to_pa_value_type("date") == pa.date32()
        assert trino_to_pa_value_type("tinyint") == pa.int8()
        assert trino_to_pa_value_type("smallint") == pa.int16()

    def test_parameterized_varchar(self) -> None:
        assert trino_to_pa_value_type("varchar(10)") == pa.string()

    def test_parameterized_char(self) -> None:
        assert trino_to_pa_value_type("char(10)") == pa.string()
        assert trino_to_pa_value_type("char") == pa.string()

    def test_binary_types(self) -> None:
        assert trino_to_pa_value_type("binary") == pa.binary()
        assert trino_to_pa_value_type("varbinary") == pa.binary()

    def test_json(self) -> None:
        assert trino_to_pa_value_type("json") == pa.string()

    def test_timestamp(self) -> None:
        assert trino_to_pa_value_type("timestamp") == pa.timestamp("us")
        assert trino_to_pa_value_type("timestamp(3)") == pa.timestamp("us")

    def test_decimal_bare(self) -> None:
        assert trino_to_pa_value_type("decimal") == pa.float64()

    def test_decimal_with_precision(self) -> None:
        assert trino_to_pa_value_type("decimal(10, 2)") == pa.float32()
        assert trino_to_pa_value_type("decimal(38, 2)") == pa.float64()
        assert trino_to_pa_value_type("decimal(32)") == pa.float32()
        assert trino_to_pa_value_type("decimal(33)") == pa.float64()

    def test_array_simple(self) -> None:
        assert trino_to_pa_value_type("array(bigint)") == pa.list_(pa.int64())

    def test_array_parameterized_varchar(self) -> None:
        assert trino_to_pa_value_type("array(varchar(10))") == pa.list_(pa.string())

    def test_array_parameterized_decimal(self) -> None:
        assert trino_to_pa_value_type("array(decimal(10, 2))") == pa.list_(pa.float32())

    def test_array_nested(self) -> None:
        assert trino_to_pa_value_type("array(array(bigint))") == pa.list_(
            pa.list_(pa.int64())
        )

    def test_row_type(self) -> None:
        assert trino_to_pa_value_type("row(x bigint)") == pa.string()
        assert trino_to_pa_value_type("row(x bigint, y varchar)") == pa.string()

    def test_map_type(self) -> None:
        assert trino_to_pa_value_type("map(varchar, bigint)") == pa.string()

    def test_array_of_row(self) -> None:
        assert trino_to_pa_value_type(
            "array(row(x bigint, y varchar(10)))"
        ) == pa.list_(pa.string())

    def test_unsupported_type(self) -> None:
        with pytest.raises(KeyError):
            trino_to_pa_value_type("unknown_type")


class TestPaToTrinoValueType:
    def test_simple_types(self) -> None:
        assert pa_to_trino_value_type(str(pa.bool_())) == "boolean"
        assert pa_to_trino_value_type(str(pa.int8())) == "tinyint"
        assert pa_to_trino_value_type(str(pa.int16())) == "smallint"
        assert pa_to_trino_value_type(str(pa.int32())) == "int"
        assert pa_to_trino_value_type(str(pa.int64())) == "bigint"
        assert pa_to_trino_value_type(str(pa.float32())) == "double"
        assert pa_to_trino_value_type(str(pa.float64())) == "double"
        assert pa_to_trino_value_type(str(pa.binary())) == "binary"

    def test_string(self) -> None:
        assert pa_to_trino_value_type(str(pa.string())) == "varchar"
        assert pa_to_trino_value_type("large_string") == "varchar"
        assert pa_to_trino_value_type("char") == "varchar"

    def test_varbinary(self) -> None:
        assert pa_to_trino_value_type("varbinary") == "binary"

    def test_date(self) -> None:
        assert pa_to_trino_value_type(str(pa.date32())) == "date"

    def test_timestamp(self) -> None:
        assert pa_to_trino_value_type(str(pa.timestamp("us"))) == "timestamp"
        assert (
            pa_to_trino_value_type(str(pa.timestamp("us", tz="UTC")))
            == "timestamp with time zone"
        )

    def test_decimal128(self) -> None:
        assert pa_to_trino_value_type(str(pa.decimal128(10, 2))) == "decimal(10, 2)"

    def test_decimal256(self) -> None:
        assert pa_to_trino_value_type(str(pa.decimal256(10, 2))) == "decimal(10, 2)"

    def test_list(self) -> None:
        assert pa_to_trino_value_type(str(pa.list_(pa.int64()))) == "array<bigint>"

    def test_list_of_string(self) -> None:
        assert pa_to_trino_value_type(str(pa.list_(pa.string()))) == "array<varchar>"

    def test_map_degrades_to_varchar(self) -> None:
        type_str = str(pa.map_(pa.string(), pa.int64()))
        assert pa_to_trino_value_type(type_str) == "varchar"

    def test_struct_degrades_to_varchar(self) -> None:
        type_str = str(pa.struct([("x", pa.int64())]))
        assert pa_to_trino_value_type(type_str) == "varchar"

    def test_null(self) -> None:
        assert pa_to_trino_value_type(str(pa.null())) == "null"
