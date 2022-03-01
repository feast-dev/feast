from typing import Dict, List, Tuple, Iterator

from feast import ValueType
from collections import defaultdict
from numpy import dtype


def spark_to_feast_value_type(spark_type_as_str: str) -> ValueType:
    # TODO not all spark types are convertible
    type_map: Dict[str, ValueType] = {
        "null": ValueType.UNKNOWN,
        "byte": ValueType.BYTES,
        "string": ValueType.STRING,
        "int": ValueType.INT32,
        "bigint": ValueType.INT64,
        "long": ValueType.INT64,
        "double": ValueType.DOUBLE,
        "float": ValueType.FLOAT,
        "boolean": ValueType.BOOL,
        "timestamp": ValueType.UNIX_TIMESTAMP,
        "array<byte>": ValueType.BYTES_LIST,
        "array<string>": ValueType.STRING_LIST,
        "array<int>": ValueType.INT32_LIST,
        "array<bigint>": ValueType.INT64_LIST,
        "array<double>": ValueType.DOUBLE_LIST,
        "array<float>": ValueType.FLOAT_LIST,
        "array<boolean>": ValueType.BOOL_LIST,
        "array<timestamp>": ValueType.UNIX_TIMESTAMP_LIST,
    }
    #TODO: this is just incorrect fix
    if(type(spark_type_as_str) != str or spark_type_as_str not in type_map):
        return ValueType.NULL
    return type_map[spark_type_as_str.lower()]


def spark_schema_to_np_dtypes(dtypes: List[Tuple[str, str]]) -> Iterator[dtype]:
    # TODO recheck all typing (also tz for timestamp)
    # https://spark.apache.org/docs/latest/api/python/user_guide/arrow_pandas.html#timestamp-with-time-zone-semantics

    type_map = defaultdict(
        lambda: dtype("O"),
        {
            "boolean": dtype("bool"),
            "double": dtype("float64"),
            "float": dtype("float64"),
            "int": dtype("int64"),
            "bigint": dtype("int64"),
            "smallint": dtype("int64"),
            "timestamp": dtype("datetime64[ns]"),
        },
    )

    return (type_map[t] for _, t in dtypes)