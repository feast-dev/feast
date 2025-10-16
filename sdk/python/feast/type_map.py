# Copyright 2019 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import decimal
import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Sized,
    Tuple,
    Type,
    Union,
    cast,
)

import numpy as np
import pandas as pd
from google.protobuf.timestamp_pb2 import Timestamp

from feast.protos.feast.types.Value_pb2 import (
    BoolList,
    BytesList,
    DoubleList,
    FloatList,
    Int32List,
    Int64List,
    StringList,
)
from feast.protos.feast.types.Value_pb2 import Value as ProtoValue
from feast.value_type import ListType, ValueType

if TYPE_CHECKING:
    import pyarrow

# null timestamps get converted to -9223372036854775808
NULL_TIMESTAMP_INT_VALUE: int = np.datetime64("NaT").astype(int)

logger = logging.getLogger(__name__)


def feast_value_type_to_python_type(field_value_proto: ProtoValue) -> Any:
    """
    Converts field value Proto to Dict and returns each field's Feast Value Type value
    in their respective Python value.

    Args:
        field_value_proto: Field value Proto

    Returns:
        Python native type representation/version of the given field_value_proto
    """
    val_attr = field_value_proto.WhichOneof("val")
    if val_attr is None:
        return None
    val = getattr(field_value_proto, val_attr)

    # If it's a _LIST type extract the list.
    if hasattr(val, "val"):
        val = list(val.val)

    # Convert UNIX_TIMESTAMP values to `datetime`
    if val_attr == "unix_timestamp_list_val":
        val = [
            (
                datetime.fromtimestamp(v, tz=timezone.utc)
                if v != NULL_TIMESTAMP_INT_VALUE
                else None
            )
            for v in val
        ]
    elif val_attr == "unix_timestamp_val":
        val = (
            datetime.fromtimestamp(val, tz=timezone.utc)
            if val != NULL_TIMESTAMP_INT_VALUE
            else None
        )

    return val


def feast_value_type_to_pandas_type(value_type: ValueType) -> Any:
    value_type_to_pandas_type: Dict[ValueType, str] = {
        ValueType.FLOAT: "float",
        ValueType.INT32: "int",
        ValueType.INT64: "int",
        ValueType.STRING: "str",
        ValueType.DOUBLE: "float",
        ValueType.BYTES: "bytes",
        ValueType.BOOL: "bool",
        ValueType.UNIX_TIMESTAMP: "datetime64[ns]",
    }
    if value_type.name.endswith("_LIST"):
        return "object"
    if value_type in value_type_to_pandas_type:
        return value_type_to_pandas_type[value_type]
    raise TypeError(
        f"Casting to pandas type for type {value_type} failed. "
        f"Type {value_type} not found"
    )


def python_type_to_feast_value_type(
    name: str,
    value: Optional[Any] = None,
    recurse: bool = True,
    type_name: Optional[str] = None,
) -> ValueType:
    """
    Finds the equivalent Feast Value Type for a Python value. Both native
    and Pandas types are supported. This function will recursively look
    for nested types when arrays are detected. All types must be homogenous.

    Args:
        name: Name of the value or field
        value: Value that will be inspected
        recurse: Whether to recursively look for nested types in arrays

    Returns:
        Feast Value Type
    """
    type_name = (type_name or type(value).__name__).lower()

    type_map = {
        "int": ValueType.INT64,
        "str": ValueType.STRING,
        "string": ValueType.STRING,  # pandas.StringDtype
        "float": ValueType.DOUBLE,
        "bytes": ValueType.BYTES,
        "float64": ValueType.DOUBLE,
        "float32": ValueType.FLOAT,
        "int64": ValueType.INT64,
        "uint64": ValueType.INT64,
        "int32": ValueType.INT32,
        "uint32": ValueType.INT32,
        "int16": ValueType.INT32,
        "uint16": ValueType.INT32,
        "uint8": ValueType.INT32,
        "int8": ValueType.INT32,
        "bool_": ValueType.BOOL,  # np.bool_
        "bool": ValueType.BOOL,
        "boolean": ValueType.BOOL,
        "timedelta": ValueType.UNIX_TIMESTAMP,
        "timestamp": ValueType.UNIX_TIMESTAMP,
        "datetime": ValueType.UNIX_TIMESTAMP,
        "datetime64[ns]": ValueType.UNIX_TIMESTAMP,
        "datetime64[ns, tz]": ValueType.UNIX_TIMESTAMP,  # special dtype of pandas
        "datetime64[ns, utc]": ValueType.UNIX_TIMESTAMP,
        "date": ValueType.UNIX_TIMESTAMP,
        "category": ValueType.STRING,
    }

    if type_name in type_map:
        return type_map[type_name]

    # Handle pandas "object" dtype by inspecting the actual value
    if type_name == "object" and value is not None:
        # Check the actual type of the value
        actual_type = type(value).__name__.lower()
        if actual_type == "str":
            return ValueType.STRING
        # If it's a different type wrapped in object, try to infer from the value
        elif actual_type in type_map:
            return type_map[actual_type]

    if isinstance(value, np.ndarray) and str(value.dtype) in type_map:
        item_type = type_map[str(value.dtype)]
        return ValueType[item_type.name + "_LIST"]

    if isinstance(value, (list, np.ndarray)):
        # if the value's type is "ndarray" and we couldn't infer from "value.dtype"
        # this is most probably array of "object",
        # so we need to iterate over objects and try to infer type of each item
        if not recurse:
            raise ValueError(
                f"Value type for field {name} is {type(value)} but "
                f"recursion is not allowed. Array types can only be one level "
                f"deep."
            )

        # This is the final type which we infer from the list
        common_item_value_type = None
        for item in value:
            if isinstance(item, ProtoValue):
                current_item_value_type: ValueType = _proto_value_to_value_type(item)
            else:
                # Get the type from the current item, only one level deep
                current_item_value_type = python_type_to_feast_value_type(
                    name=name, value=item, recurse=False
                )
            # Validate whether the type stays consistent
            if (
                common_item_value_type
                and not common_item_value_type == current_item_value_type
            ):
                raise ValueError(
                    f"List value type for field {name} is inconsistent. "
                    f"{common_item_value_type} different from "
                    f"{current_item_value_type}."
                )
            common_item_value_type = current_item_value_type
        if common_item_value_type is None:
            return ValueType.UNKNOWN
        return ValueType[common_item_value_type.name + "_LIST"]

    raise ValueError(
        f"Value with native type {type_name} cannot be converted into Feast value type"
    )


def python_values_to_feast_value_type(
    name: str, values: Any, recurse: bool = True
) -> ValueType:
    inferred_dtype = ValueType.UNKNOWN
    for row in values:
        current_dtype = python_type_to_feast_value_type(
            name, value=row, recurse=recurse
        )

        if inferred_dtype is ValueType.UNKNOWN:
            inferred_dtype = current_dtype
        else:
            if current_dtype != inferred_dtype and current_dtype not in (
                ValueType.UNKNOWN,
                ValueType.NULL,
            ):
                raise TypeError(
                    f"Input entity {name} has mixed types, {current_dtype} and {inferred_dtype}. That is not allowed. "
                )
    if inferred_dtype in (ValueType.UNKNOWN, ValueType.NULL):
        raise ValueError(
            f"field {name} cannot have all null values for type inference."
        )

    return inferred_dtype


def _convert_value_type_str_to_value_type(type_str: str) -> ValueType:
    type_map = {
        "UNKNOWN": ValueType.UNKNOWN,
        "BYTES": ValueType.BYTES,
        "STRING": ValueType.STRING,
        "INT32": ValueType.INT32,
        "INT64": ValueType.INT64,
        "DOUBLE": ValueType.DOUBLE,
        "FLOAT": ValueType.FLOAT,
        "FLOAT32": ValueType.FLOAT,
        "BOOL": ValueType.BOOL,
        "NULL": ValueType.NULL,
        "UNIX_TIMESTAMP": ValueType.UNIX_TIMESTAMP,
        "BYTES_LIST": ValueType.BYTES_LIST,
        "STRING_LIST": ValueType.STRING_LIST,
        "INT32_LIST ": ValueType.INT32_LIST,
        "INT64_LIST": ValueType.INT64_LIST,
        "DOUBLE_LIST": ValueType.DOUBLE_LIST,
        "FLOAT_LIST": ValueType.FLOAT_LIST,
        "BOOL_LIST": ValueType.BOOL_LIST,
        "UNIX_TIMESTAMP_LIST": ValueType.UNIX_TIMESTAMP_LIST,
    }
    return type_map.get(type_str, ValueType.STRING)


def _type_err(item, dtype):
    raise TypeError(f'Value "{item}" is of type {type(item)} not of type {dtype}')


PYTHON_LIST_VALUE_TYPE_TO_PROTO_VALUE: Dict[
    ValueType, Tuple[ListType, str, List[Type]]
] = {
    ValueType.FLOAT_LIST: (
        FloatList,
        "float_list_val",
        [np.float32, np.float64, float],
    ),
    ValueType.DOUBLE_LIST: (
        DoubleList,
        "double_list_val",
        [np.float64, np.float32, float],
    ),
    ValueType.INT32_LIST: (Int32List, "int32_list_val", [np.int64, np.int32, int]),
    ValueType.INT64_LIST: (Int64List, "int64_list_val", [np.int64, np.int32, int]),
    ValueType.UNIX_TIMESTAMP_LIST: (
        Int64List,
        "int64_list_val",
        [np.datetime64, np.int64, np.int32, int, datetime, Timestamp],
    ),
    ValueType.STRING_LIST: (StringList, "string_list_val", [np.str_, str]),
    ValueType.BOOL_LIST: (BoolList, "bool_list_val", [np.bool_, bool]),
    ValueType.BYTES_LIST: (BytesList, "bytes_list_val", [np.bytes_, bytes]),
}

PYTHON_SCALAR_VALUE_TYPE_TO_PROTO_VALUE: Dict[
    ValueType, Tuple[str, Any, Optional[Set[Type]]]
] = {
    ValueType.INT32: ("int32_val", lambda x: int(x), None),
    ValueType.INT64: (
        "int64_val",
        lambda x: (
            int(x.timestamp())
            if isinstance(x, pd._libs.tslibs.timestamps.Timestamp)
            else int(x)
        ),
        None,
    ),
    ValueType.FLOAT: ("float_val", lambda x: float(x), None),
    ValueType.DOUBLE: (
        "double_val",
        lambda x: x,
        {float, np.float64, int, np.int_, decimal.Decimal},
    ),
    ValueType.STRING: ("string_val", lambda x: str(x), None),
    ValueType.BYTES: ("bytes_val", lambda x: x, {bytes}),
    ValueType.IMAGE_BYTES: ("bytes_val", lambda x: x, {bytes}),
    ValueType.BOOL: ("bool_val", lambda x: x, {bool, np.bool_, int, np.int_}),
}


def _python_datetime_to_int_timestamp(
    values: Sequence[Any],
) -> Sequence[Union[int, np.int_]]:
    # Fast path for Numpy array.
    if isinstance(values, np.ndarray) and isinstance(values.dtype, np.datetime64):
        if values.ndim != 1:
            raise ValueError("Only 1 dimensional arrays are supported.")
        return cast(Sequence[np.int_], values.astype("datetime64[s]").astype(np.int_))

    int_timestamps = []
    for value in values:
        if isinstance(value, datetime):
            int_timestamps.append(int(value.timestamp()))
        elif isinstance(value, Timestamp):
            int_timestamps.append(int(value.ToSeconds()))
        elif isinstance(value, np.datetime64):
            int_timestamps.append(value.astype("datetime64[s]").astype(np.int_))  # type: ignore[attr-defined]
        elif isinstance(value, type(np.nan)):
            int_timestamps.append(NULL_TIMESTAMP_INT_VALUE)
        else:
            int_timestamps.append(int(value))
    return int_timestamps


def _python_value_to_proto_value(
    feast_value_type: ValueType, values: List[Any]
) -> List[ProtoValue]:
    """
    Converts a Python (native, pandas) value to a Feast Proto Value based
    on a provided value type

    Args:
        feast_value_type: The target value type
        values: List of Values that will be converted

    Returns:
        List of Feast Value Proto
    """
    # ToDo: make a better sample for type checks (more than one element)
    sample = next(filter(_non_empty_value, values), None)  # first not empty value

    # Detect list type and handle separately
    if "list" in feast_value_type.name.lower():
        # Feature can be list but None is still valid
        if feast_value_type in PYTHON_LIST_VALUE_TYPE_TO_PROTO_VALUE:
            proto_type, field_name, valid_types = PYTHON_LIST_VALUE_TYPE_TO_PROTO_VALUE[
                feast_value_type
            ]

            # Bytes to array type conversion
            if isinstance(sample, (bytes, bytearray)):
                # Bytes of an array containing elements of bytes not supported
                if feast_value_type == ValueType.BYTES_LIST:
                    raise _type_err(sample, ValueType.BYTES_LIST)

                json_sample = json.loads(sample)
                if isinstance(json_sample, list):
                    json_values = [json.loads(value) for value in values]
                    if feast_value_type == ValueType.BOOL_LIST:
                        json_values = [
                            [bool(item) for item in list_item]
                            for list_item in json_values
                        ]
                    return [
                        ProtoValue(**{field_name: proto_type(val=v)})  # type: ignore
                        for v in json_values
                    ]
                raise _type_err(sample, valid_types[0])

            if sample is not None and not all(
                type(item) in valid_types for item in sample
            ):
                # to_numpy() in utils._convert_arrow_to_proto() upcasts values of type Array of INT32 or INT64 with NULL values to Float64 automatically.
                for item in sample:
                    if type(item) not in valid_types:
                        if feast_value_type in [
                            ValueType.INT32_LIST,
                            ValueType.INT64_LIST,
                        ]:
                            if not any(np.isnan(item) for item in sample):
                                logger.error(
                                    "Array of Int32 or Int64 type has NULL values. to_numpy() upcasts to Float64 automatically."
                                )
                        raise _type_err(item, valid_types[0])

            if feast_value_type == ValueType.UNIX_TIMESTAMP_LIST:
                return [
                    (
                        # ProtoValue does actually accept `np.int_` but the typing complains.
                        ProtoValue(
                            unix_timestamp_list_val=Int64List(
                                val=_python_datetime_to_int_timestamp(value)  # type: ignore
                            )
                        )
                        if value is not None
                        else ProtoValue()
                    )
                    for value in values
                ]
            if feast_value_type == ValueType.BOOL_LIST:
                # ProtoValue does not support conversion of np.bool_ so we need to convert it to support np.bool_.
                return [
                    (
                        ProtoValue(
                            **{field_name: proto_type(val=[bool(e) for e in value])}  # type: ignore
                        )
                        if value is not None
                        else ProtoValue()
                    )
                    for value in values
                ]
            return [
                (
                    ProtoValue(**{field_name: proto_type(val=value)})  # type: ignore
                    if value is not None
                    else ProtoValue()
                )
                for value in values
            ]

    # Handle scalar types below
    else:
        if sample is None:
            # all input values are None
            return [ProtoValue()] * len(values)

        if feast_value_type == ValueType.UNIX_TIMESTAMP:
            int_timestamps = _python_datetime_to_int_timestamp(values)
            # ProtoValue does actually accept `np.int_` but the typing complains.
            return [ProtoValue(unix_timestamp_val=ts) for ts in int_timestamps]  # type: ignore

        (
            field_name,
            func,
            valid_scalar_types,
        ) = PYTHON_SCALAR_VALUE_TYPE_TO_PROTO_VALUE[feast_value_type]
        if valid_scalar_types:
            if (sample == 0 or sample == 0.0) and feast_value_type != ValueType.BOOL:
                # Numpy convert 0 to int. However, in the feature view definition, the type of column may be a float.
                # So, if value is 0, type validation must pass if scalar_types are either int or float.
                allowed_types = {np.int64, int, np.float64, float, decimal.Decimal}
                assert type(sample) in allowed_types, (
                    f"Type `{type(sample)}` not in {allowed_types}"
                )
            else:
                assert type(sample) in valid_scalar_types, (
                    f"Type `{type(sample)}` not in {valid_scalar_types}"
                )
        if feast_value_type == ValueType.BOOL:
            # ProtoValue does not support conversion of np.bool_ so we need to convert it to support np.bool_.
            return [
                (
                    ProtoValue(
                        **{
                            field_name: func(
                                bool(value) if type(value) is np.bool_ else value  # type: ignore
                            )
                        }
                    )
                    if not pd.isnull(value)
                    else ProtoValue()
                )
                for value in values
            ]
        if feast_value_type in PYTHON_SCALAR_VALUE_TYPE_TO_PROTO_VALUE:
            out = []
            for value in values:
                if isinstance(value, ProtoValue):
                    out.append(value)
                elif not pd.isnull(value):
                    out.append(ProtoValue(**{field_name: func(value)}))
                else:
                    out.append(ProtoValue())
            return out

    raise Exception(f"Unsupported data type: ${str(type(values[0]))}")


def python_values_to_proto_values(
    values: List[Any], feature_type: ValueType = ValueType.UNKNOWN
) -> List[ProtoValue]:
    value_type = feature_type
    sample = next(filter(_non_empty_value, values), None)  # first not empty value
    if sample is not None and feature_type == ValueType.UNKNOWN:
        if isinstance(sample, (list, np.ndarray)):
            value_type = (
                feature_type
                if len(sample) == 0
                else python_type_to_feast_value_type("", sample)
            )
        else:
            value_type = python_type_to_feast_value_type("", sample)

    if value_type == ValueType.UNKNOWN:
        raise TypeError("Couldn't infer value type from empty value")

    proto_values = _python_value_to_proto_value(value_type, values)

    if len(proto_values) != len(values):
        raise ValueError(
            f"Number of proto values {len(proto_values)} does not match number of values {len(values)}"
        )

    return proto_values


PROTO_VALUE_TO_VALUE_TYPE_MAP: Dict[str, ValueType] = {
    "int32_val": ValueType.INT32,
    "int64_val": ValueType.INT64,
    "double_val": ValueType.DOUBLE,
    "float_val": ValueType.FLOAT,
    "string_val": ValueType.STRING,
    "bytes_val": ValueType.BYTES,
    "bool_val": ValueType.BOOL,
    "int32_list_val": ValueType.INT32_LIST,
    "int64_list_val": ValueType.INT64_LIST,
    "double_list_val": ValueType.DOUBLE_LIST,
    "float_list_val": ValueType.FLOAT_LIST,
    "string_list_val": ValueType.STRING_LIST,
    "bytes_list_val": ValueType.BYTES_LIST,
    "bool_list_val": ValueType.BOOL_LIST,
}

VALUE_TYPE_TO_PROTO_VALUE_MAP: Dict[ValueType, str] = {
    v: k for k, v in PROTO_VALUE_TO_VALUE_TYPE_MAP.items()
}


def _proto_value_to_value_type(proto_value: ProtoValue) -> ValueType:
    """
    Returns Feast ValueType given Feast ValueType string.

    Args:
        proto_str: str

    Returns:
        A variant of ValueType.
    """
    proto_str = proto_value.WhichOneof("val")
    if proto_str is None:
        return ValueType.UNKNOWN
    return PROTO_VALUE_TO_VALUE_TYPE_MAP[proto_str]


def pa_to_feast_value_type(pa_type_as_str: str) -> ValueType:
    is_list = False
    if pa_type_as_str.startswith("list<item: "):
        is_list = True
        pa_type_as_str = pa_type_as_str.replace("list<item: ", "").replace(">", "")

    if pa_type_as_str.startswith("timestamp"):
        value_type = ValueType.UNIX_TIMESTAMP
    else:
        type_map = {
            "int32": ValueType.INT32,
            "int64": ValueType.INT64,
            "double": ValueType.DOUBLE,
            "float": ValueType.FLOAT,
            "string": ValueType.STRING,
            "binary": ValueType.BYTES,
            "bool": ValueType.BOOL,
            "null": ValueType.NULL,
            "list<element: double>": ValueType.DOUBLE_LIST,
            "list<element: int64>": ValueType.INT64_LIST,
            "list<element: int32>": ValueType.INT32_LIST,
            "list<element: str>": ValueType.STRING_LIST,
            "list<element: bool>": ValueType.BOOL_LIST,
            "list<element: bytes>": ValueType.BYTES_LIST,
            "list<element: float>": ValueType.FLOAT_LIST,
        }
        value_type = type_map[pa_type_as_str]

    if is_list:
        value_type = ValueType[value_type.name + "_LIST"]

    return value_type


def bq_to_feast_value_type(bq_type_as_str: str) -> ValueType:
    is_list = False
    if bq_type_as_str.startswith("ARRAY<"):
        is_list = True
        bq_type_as_str = bq_type_as_str[6:-1]

    type_map: Dict[str, ValueType] = {
        "DATETIME": ValueType.UNIX_TIMESTAMP,
        "TIMESTAMP": ValueType.UNIX_TIMESTAMP,
        "INTEGER": ValueType.INT64,
        "NUMERIC": ValueType.INT64,
        "INT64": ValueType.INT64,
        "STRING": ValueType.STRING,
        "FLOAT": ValueType.DOUBLE,
        "FLOAT64": ValueType.DOUBLE,
        "BYTES": ValueType.BYTES,
        "BOOL": ValueType.BOOL,
        "BOOLEAN": ValueType.BOOL,  # legacy sql data type
        "NULL": ValueType.NULL,
    }

    value_type = type_map.get(bq_type_as_str, ValueType.STRING)

    if is_list:
        value_type = ValueType[value_type.name + "_LIST"]

    return value_type


def mssql_to_feast_value_type(mssql_type_as_str: str) -> ValueType:
    type_map = {
        "bigint": ValueType.FLOAT,
        "binary": ValueType.BYTES,
        "bit": ValueType.BOOL,
        "char": ValueType.STRING,
        "date": ValueType.UNIX_TIMESTAMP,
        "datetime": ValueType.UNIX_TIMESTAMP,
        "datetimeoffset": ValueType.UNIX_TIMESTAMP,
        "float": ValueType.FLOAT,
        "int": ValueType.INT32,
        "nchar": ValueType.STRING,
        "nvarchar": ValueType.STRING,
        "nvarchar(max)": ValueType.STRING,
        "real": ValueType.FLOAT,
        "smallint": ValueType.INT32,
        "tinyint": ValueType.INT32,
        "varbinary": ValueType.BYTES,
        "varchar": ValueType.STRING,
        "None": ValueType.NULL,
        # skip date, geometry, hllsketch, time, timetz
    }
    if mssql_type_as_str.lower() not in type_map:
        raise ValueError(f"Mssql type not supported by feast {mssql_type_as_str}")
    return type_map[mssql_type_as_str.lower()]


def pa_to_mssql_type(pa_type: "pyarrow.DataType") -> str:
    # PyArrow types: https://arrow.apache.org/docs/python/api/datatypes.html
    # MS Sql types: https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver16
    pa_type_as_str = str(pa_type).lower()
    if pa_type_as_str.startswith("timestamp"):
        if "tz=" in pa_type_as_str:
            return "datetime2"
        else:
            return "datetime"

    if pa_type_as_str.startswith("date"):
        return "date"

    if pa_type_as_str.startswith("decimal"):
        return pa_type_as_str

    # We have to take into account how arrow types map to parquet types as well.
    # For example, null type maps to int32 in parquet, so we have to use int4 in Redshift.
    # Other mappings have also been adjusted accordingly.
    type_map = {
        "null": "None",
        "bool": "bit",
        "int8": "tinyint",
        "int16": "smallint",
        "int32": "int",
        "int64": "bigint",
        "uint8": "tinyint",
        "uint16": "smallint",
        "uint32": "int",
        "uint64": "bigint",
        "float": "float",
        "double": "real",
        "binary": "binary",
        "string": "varchar",
    }

    if pa_type_as_str.lower() not in type_map:
        raise ValueError(f"MS SQL Server type not supported by feast {pa_type_as_str}")

    return type_map[pa_type_as_str]


def redshift_to_feast_value_type(redshift_type_as_str: str) -> ValueType:
    # Type names from https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html
    type_map = {
        "int2": ValueType.INT32,
        "int4": ValueType.INT32,
        "int8": ValueType.INT64,
        "numeric": ValueType.DOUBLE,
        "float4": ValueType.FLOAT,
        "float8": ValueType.DOUBLE,
        "bool": ValueType.BOOL,
        "character": ValueType.STRING,
        "varchar": ValueType.STRING,
        "timestamp": ValueType.UNIX_TIMESTAMP,
        "timestamptz": ValueType.UNIX_TIMESTAMP,
        "super": ValueType.BYTES,
        # skip date, geometry, hllsketch, time, timetz
    }

    return type_map[redshift_type_as_str.lower()]


def snowflake_type_to_feast_value_type(snowflake_type: str) -> ValueType:
    type_map = {
        "BINARY": ValueType.BYTES,
        "VARCHAR": ValueType.STRING,
        "NUMBER32": ValueType.INT32,
        "NUMBER64": ValueType.INT64,
        "NUMBERwSCALE": ValueType.DOUBLE,
        "DOUBLE": ValueType.DOUBLE,
        "BOOLEAN": ValueType.BOOL,
        "DATE": ValueType.UNIX_TIMESTAMP,
        "TIMESTAMP": ValueType.UNIX_TIMESTAMP,
        "TIMESTAMP_TZ": ValueType.UNIX_TIMESTAMP,
        "TIMESTAMP_LTZ": ValueType.UNIX_TIMESTAMP,
        "TIMESTAMP_NTZ": ValueType.UNIX_TIMESTAMP,
    }
    return type_map[snowflake_type]


def _convert_value_name_to_snowflake_udf(value_name: str, project_name: str) -> str:
    name_map = {
        "BYTES": f"feast_{project_name}_snowflake_binary_to_bytes_proto",
        "STRING": f"feast_{project_name}_snowflake_varchar_to_string_proto",
        "INT32": f"feast_{project_name}_snowflake_number_to_int32_proto",
        "INT64": f"feast_{project_name}_snowflake_number_to_int64_proto",
        "DOUBLE": f"feast_{project_name}_snowflake_float_to_double_proto",
        "FLOAT": f"feast_{project_name}_snowflake_float_to_double_proto",
        "BOOL": f"feast_{project_name}_snowflake_boolean_to_bool_proto",
        "UNIX_TIMESTAMP": f"feast_{project_name}_snowflake_timestamp_to_unix_timestamp_proto",
        "BYTES_LIST": f"feast_{project_name}_snowflake_array_bytes_to_list_bytes_proto",
        "STRING_LIST": f"feast_{project_name}_snowflake_array_varchar_to_list_string_proto",
        "INT32_LIST": f"feast_{project_name}_snowflake_array_number_to_list_int32_proto",
        "INT64_LIST": f"feast_{project_name}_snowflake_array_number_to_list_int64_proto",
        "DOUBLE_LIST": f"feast_{project_name}_snowflake_array_float_to_list_double_proto",
        "FLOAT_LIST": f"feast_{project_name}_snowflake_array_float_to_list_double_proto",
        "BOOL_LIST": f"feast_{project_name}_snowflake_array_boolean_to_list_bool_proto",
        "UNIX_TIMESTAMP_LIST": f"feast_{project_name}_snowflake_array_timestamp_to_list_unix_timestamp_proto",
    }
    return name_map[value_name].upper()


def pa_to_redshift_value_type(pa_type: "pyarrow.DataType") -> str:
    # PyArrow types: https://arrow.apache.org/docs/python/api/datatypes.html
    # Redshift type: https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html
    pa_type_as_str = str(pa_type).lower()
    if pa_type_as_str.startswith("timestamp"):
        if "tz=" in pa_type_as_str:
            return "timestamptz"
        else:
            return "timestamp"

    if pa_type_as_str.startswith("date"):
        return "date"

    if pa_type_as_str.startswith("decimal"):
        # PyArrow decimal types (e.g. "decimal(38,37)") luckily directly map to the Redshift type.
        return pa_type_as_str

    if pa_type_as_str.startswith("list"):
        return "super"

    # We have to take into account how arrow types map to parquet types as well.
    # For example, null type maps to int32 in parquet, so we have to use int4 in Redshift.
    # Other mappings have also been adjusted accordingly.
    type_map = {
        "null": "int4",
        "bool": "bool",
        "int8": "int4",
        "int16": "int4",
        "int32": "int4",
        "int64": "int8",
        "uint8": "int4",
        "uint16": "int4",
        "uint32": "int8",
        "uint64": "int8",
        "float": "float4",
        "double": "float8",
        "binary": "varchar",
        "string": "varchar",
    }

    return type_map[pa_type_as_str]


def _non_empty_value(value: Any) -> bool:
    """
    Check that there's enough data we can use for type inference.
    If primitive type - just checking that it's not None
    If iterable - checking that there's some elements (len > 0)
    String is special case: "" - empty string is considered non empty
    """
    return value is not None and (
        not isinstance(value, Sized) or len(value) > 0 or isinstance(value, str)
    )


def spark_to_feast_value_type(spark_type_as_str: str) -> ValueType:
    # TODO not all spark types are convertible
    # Current non-convertible types: interval, map, struct, structfield, binary
    type_map: Dict[str, ValueType] = {
        "null": ValueType.UNKNOWN,
        "byte": ValueType.BYTES,
        "string": ValueType.STRING,
        "int": ValueType.INT32,
        "short": ValueType.INT32,
        "bigint": ValueType.INT64,
        "long": ValueType.INT64,
        "double": ValueType.DOUBLE,
        "decimal": ValueType.DOUBLE,
        "float": ValueType.FLOAT,
        "boolean": ValueType.BOOL,
        "timestamp": ValueType.UNIX_TIMESTAMP,
        "date": ValueType.UNIX_TIMESTAMP,
        "array<byte>": ValueType.BYTES_LIST,
        "array<string>": ValueType.STRING_LIST,
        "array<int>": ValueType.INT32_LIST,
        "array<bigint>": ValueType.INT64_LIST,
        "array<double>": ValueType.DOUBLE_LIST,
        "array<decimal>": ValueType.DOUBLE_LIST,
        "array<float>": ValueType.FLOAT_LIST,
        "array<boolean>": ValueType.BOOL_LIST,
        "array<timestamp>": ValueType.UNIX_TIMESTAMP_LIST,
        "array<date>": ValueType.UNIX_TIMESTAMP_LIST,
    }
    if spark_type_as_str.startswith("decimal"):
        spark_type_as_str = "decimal"
    if spark_type_as_str.startswith("array<decimal"):
        spark_type_as_str = "array<decimal>"
    # TODO: Find better way of doing this.
    if not isinstance(spark_type_as_str, str) or spark_type_as_str not in type_map:
        return ValueType.NULL
    return type_map[spark_type_as_str.lower()]


def spark_schema_to_np_dtypes(dtypes: List[Tuple[str, str]]) -> Iterator[np.dtype]:
    # TODO recheck all typing (also tz for timestamp)
    # https://spark.apache.org/docs/latest/api/python/user_guide/arrow_pandas.html#timestamp-with-time-zone-semantics

    type_map = defaultdict(
        lambda: np.dtype("O"),
        {
            "boolean": np.dtype("bool"),
            "double": np.dtype("float64"),
            "float": np.dtype("float64"),
            "int": np.dtype("int64"),
            "bigint": np.dtype("int64"),
            "smallint": np.dtype("int64"),
            "timestamp": np.dtype("datetime64[ns]"),
        },
    )

    return (type_map[t] for _, t in dtypes)


def arrow_to_pg_type(t_str: str) -> str:
    try:
        if t_str.startswith("timestamp") or t_str.startswith("datetime"):
            return "timestamptz" if "tz=" in t_str else "timestamp"
        return {
            "null": "null",
            "bool": "boolean",
            "int8": "tinyint",
            "int16": "smallint",
            "int32": "int",
            "int64": "bigint",
            "list<item: int32>": "int[]",
            "list<item: int64>": "bigint[]",
            "list<item: bool>": "boolean[]",
            "list<item: double>": "double precision[]",
            "list<item: timestamp[us]>": "timestamp[]",
            "uint8": "smallint",
            "uint16": "int",
            "uint32": "bigint",
            "uint64": "bigint",
            "float": "float",
            "double": "double precision",
            "binary": "binary",
            "string": "text",
        }[t_str]
    except KeyError:
        raise ValueError(f"Unsupported type: {t_str}")


def pg_type_to_feast_value_type(type_str: str) -> ValueType:
    type_map: Dict[str, ValueType] = {
        "boolean": ValueType.BOOL,
        "bytea": ValueType.BYTES,
        "char": ValueType.STRING,
        "bigint": ValueType.INT64,
        "smallint": ValueType.INT32,
        "integer": ValueType.INT32,
        "real": ValueType.DOUBLE,
        "double precision": ValueType.DOUBLE,
        "boolean[]": ValueType.BOOL_LIST,
        "bytea[]": ValueType.BYTES_LIST,
        "char[]": ValueType.STRING_LIST,
        "smallint[]": ValueType.INT32_LIST,
        "integer[]": ValueType.INT32_LIST,
        "text": ValueType.STRING,
        "text[]": ValueType.STRING_LIST,
        "character[]": ValueType.STRING_LIST,
        "bigint[]": ValueType.INT64_LIST,
        "real[]": ValueType.DOUBLE_LIST,
        "double precision[]": ValueType.DOUBLE_LIST,
        "character": ValueType.STRING,
        "character varying": ValueType.STRING,
        "date": ValueType.UNIX_TIMESTAMP,
        "time without time zone": ValueType.UNIX_TIMESTAMP,
        "timestamp without time zone": ValueType.UNIX_TIMESTAMP,
        "timestamp without time zone[]": ValueType.UNIX_TIMESTAMP_LIST,
        "date[]": ValueType.UNIX_TIMESTAMP_LIST,
        "time without time zone[]": ValueType.UNIX_TIMESTAMP_LIST,
        "timestamp with time zone": ValueType.UNIX_TIMESTAMP,
        "timestamp with time zone[]": ValueType.UNIX_TIMESTAMP_LIST,
        "numeric[]": ValueType.DOUBLE_LIST,
        "numeric": ValueType.DOUBLE,
        "uuid": ValueType.STRING,
        "uuid[]": ValueType.STRING_LIST,
    }
    value = (
        type_map[type_str.lower()]
        if type_str.lower() in type_map
        else ValueType.UNKNOWN
    )
    if value == ValueType.UNKNOWN:
        print("unknown type:", type_str)
    return value


def feast_value_type_to_pa(
    feast_type: ValueType, timestamp_unit: str = "us"
) -> "pyarrow.DataType":
    import pyarrow

    type_map = {
        ValueType.INT32: pyarrow.int32(),
        ValueType.INT64: pyarrow.int64(),
        ValueType.DOUBLE: pyarrow.float64(),
        ValueType.FLOAT: pyarrow.float32(),
        ValueType.STRING: pyarrow.string(),
        ValueType.BYTES: pyarrow.binary(),
        ValueType.BOOL: pyarrow.bool_(),
        ValueType.UNIX_TIMESTAMP: pyarrow.timestamp(timestamp_unit),
        ValueType.INT32_LIST: pyarrow.list_(pyarrow.int32()),
        ValueType.INT64_LIST: pyarrow.list_(pyarrow.int64()),
        ValueType.DOUBLE_LIST: pyarrow.list_(pyarrow.float64()),
        ValueType.FLOAT_LIST: pyarrow.list_(pyarrow.float32()),
        ValueType.STRING_LIST: pyarrow.list_(pyarrow.string()),
        ValueType.BYTES_LIST: pyarrow.list_(pyarrow.binary()),
        ValueType.BOOL_LIST: pyarrow.list_(pyarrow.bool_()),
        ValueType.UNIX_TIMESTAMP_LIST: pyarrow.list_(pyarrow.timestamp(timestamp_unit)),
        ValueType.NULL: pyarrow.null(),
    }
    return type_map[feast_type]


def pg_type_code_to_pg_type(code: int) -> str:
    """Map the postgres type code a Feast type string

    Rather than raise an exception on an unknown type, we return the
    string representation of the type code. This way rather than raising
    an exception on unknown types, Feast will just skip the problem columns.

    Note that json and jsonb are not supported but this shows up in the
    log as a warning. Since postgres allows custom types we return an unknown for those cases.

    See: https://jdbc.postgresql.org/documentation/publicapi/index.html?constant-values.html
    """
    PG_TYPE_MAP = {
        16: "boolean",
        17: "bytea",
        20: "bigint",
        21: "smallint",
        23: "integer",
        25: "text",
        114: "json",
        199: "json[]",
        700: "real",
        701: "double precision",
        1000: "boolean[]",
        1001: "bytea[]",
        1005: "smallint[]",
        1007: "integer[]",
        1009: "text[]",
        1014: "character[]",
        1016: "bigint[]",
        1021: "real[]",
        1022: "double precision[]",
        1042: "character",
        1043: "character varying",
        1082: "date",
        1083: "time without time zone",
        1114: "timestamp without time zone",
        1115: "timestamp without time zone[]",
        1182: "date[]",
        1183: "time without time zone[]",
        1184: "timestamp with time zone",
        1185: "timestamp with time zone[]",
        1231: "numeric[]",
        1700: "numeric",
        2950: "uuid",
        2951: "uuid[]",
        3802: "jsonb",
        3807: "jsonb[]",
    }

    return PG_TYPE_MAP.get(code, "unknown")


def pg_type_code_to_arrow(code: int) -> str:
    return feast_value_type_to_pa(
        pg_type_to_feast_value_type(pg_type_code_to_pg_type(code))
    )


def athena_to_feast_value_type(athena_type_as_str: str) -> ValueType:
    # Type names from https://docs.aws.amazon.com/athena/latest/ug/data-types.html
    type_map = {
        "null": ValueType.UNKNOWN,
        "boolean": ValueType.BOOL,
        "tinyint": ValueType.INT32,
        "smallint": ValueType.INT32,
        "int": ValueType.INT32,
        "bigint": ValueType.INT64,
        "double": ValueType.DOUBLE,
        "float": ValueType.FLOAT,
        "binary": ValueType.BYTES,
        "char": ValueType.STRING,
        "varchar": ValueType.STRING,
        "string": ValueType.STRING,
        "timestamp": ValueType.UNIX_TIMESTAMP,
        # skip date,decimal,array,map,struct
    }
    return type_map[athena_type_as_str.lower()]


def pa_to_athena_value_type(pa_type: "pyarrow.DataType") -> str:
    # PyArrow types: https://arrow.apache.org/docs/python/api/datatypes.html
    # Type names from https://docs.aws.amazon.com/athena/latest/ug/data-types.html
    pa_type_as_str = str(pa_type).lower()
    if pa_type_as_str.startswith("timestamp"):
        return "timestamp"

    if pa_type_as_str.startswith("date"):
        return "date"

    if pa_type_as_str.startswith("python_values_to_proto_values"):
        return pa_type_as_str

    # We have to take into account how arrow types map to parquet types as well.
    # For example, null type maps to int32 in parquet, so we have to use int4 in Redshift.
    # Other mappings have also been adjusted accordingly.
    type_map = {
        "null": "null",
        "bool": "boolean",
        "int8": "tinyint",
        "int16": "smallint",
        "int32": "int",
        "int64": "bigint",
        "uint8": "tinyint",
        "uint16": "tinyint",
        "uint32": "tinyint",
        "uint64": "tinyint",
        "float": "float",
        "double": "double",
        "binary": "binary",
        "string": "string",
    }

    return type_map[pa_type_as_str]


def cb_columnar_type_to_feast_value_type(type_str: str) -> ValueType:
    """
    Convert a Couchbase Columnar type string to a Feast ValueType
    """
    type_map: Dict[str, ValueType] = {
        # primitive types
        "boolean": ValueType.BOOL,
        "string": ValueType.STRING,
        "bigint": ValueType.INT64,
        "double": ValueType.DOUBLE,
        # special types
        "null": ValueType.NULL,
        "missing": ValueType.UNKNOWN,
        # composite types
        # todo: support for arrays of primitives
        "object": ValueType.UNKNOWN,
        "array": ValueType.UNKNOWN,
        "multiset": ValueType.UNKNOWN,
        "uuid": ValueType.STRING,
    }
    value = (
        type_map[type_str.lower()]
        if type_str.lower() in type_map
        else ValueType.UNKNOWN
    )
    if value == ValueType.UNKNOWN:
        print("unknown type:", type_str)
    return value


def convert_scalar_column(
    series: pd.Series, value_type: ValueType, target_pandas_type: str
) -> pd.Series:
    """Convert a scalar feature column to the appropriate pandas type."""
    if value_type == ValueType.INT32:
        return pd.to_numeric(series, errors="coerce").astype("Int32")
    elif value_type == ValueType.INT64:
        return pd.to_numeric(series, errors="coerce").astype("Int64")
    elif value_type in [ValueType.FLOAT, ValueType.DOUBLE]:
        return pd.to_numeric(series, errors="coerce").astype("float64")
    elif value_type == ValueType.BOOL:
        return series.astype("boolean")
    elif value_type == ValueType.STRING:
        return series.astype("string")
    elif value_type == ValueType.UNIX_TIMESTAMP:
        return pd.to_datetime(series, unit="s", errors="coerce")
    else:
        return series.astype(target_pandas_type)


def convert_array_column(series: pd.Series, value_type: ValueType) -> pd.Series:
    """Convert an array feature column to the appropriate type with proper empty array handling."""
    base_type_map = {
        ValueType.INT32_LIST: np.int32,
        ValueType.INT64_LIST: np.int64,
        ValueType.FLOAT_LIST: np.float32,
        ValueType.DOUBLE_LIST: np.float64,
        ValueType.BOOL_LIST: np.bool_,
        ValueType.STRING_LIST: object,
        ValueType.BYTES_LIST: object,
        ValueType.UNIX_TIMESTAMP_LIST: "datetime64[s]",
    }

    target_dtype = base_type_map.get(value_type, object)

    def convert_array_item(item) -> Union[np.ndarray, Any]:
        if item is None or (isinstance(item, list) and len(item) == 0):
            if target_dtype == object:
                return np.empty(0, dtype=object)
            else:
                return np.empty(0, dtype=target_dtype)  # type: ignore
        else:
            return item

    return series.apply(convert_array_item)
