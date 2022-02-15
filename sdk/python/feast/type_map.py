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

from datetime import datetime, timezone
from typing import (
    Any,
    Dict,
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
import pyarrow
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
        val = [datetime.fromtimestamp(v, tz=timezone.utc) for v in val]
    elif val_attr == "unix_timestamp_val":
        val = datetime.fromtimestamp(val, tz=timezone.utc)

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
    name: str, value: Any = None, recurse: bool = True, type_name: Optional[str] = None
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
        "bool": ValueType.BOOL,
        "timedelta": ValueType.UNIX_TIMESTAMP,
        "timestamp": ValueType.UNIX_TIMESTAMP,
        "datetime": ValueType.UNIX_TIMESTAMP,
        "datetime64[ns]": ValueType.UNIX_TIMESTAMP,
        "datetime64[ns, tz]": ValueType.UNIX_TIMESTAMP,
        "category": ValueType.STRING,
    }

    if type_name in type_map:
        return type_map[type_name]

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
        f"Value with native type {type_name} "
        f"cannot be converted into Feast value type"
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
        lambda x: int(x.timestamp())
        if isinstance(x, pd._libs.tslibs.timestamps.Timestamp)
        else int(x),
        None,
    ),
    ValueType.FLOAT: ("float_val", lambda x: float(x), None),
    ValueType.DOUBLE: ("double_val", lambda x: x, {float, np.float64}),
    ValueType.STRING: ("string_val", lambda x: str(x), None),
    ValueType.BYTES: ("bytes_val", lambda x: x, {bytes}),
    ValueType.BOOL: ("bool_val", lambda x: x, {bool, np.bool_}),
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
            int_timestamps.append(value.astype("datetime64[s]").astype(np.int_))
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

            if sample is not None and not all(
                type(item) in valid_types for item in sample
            ):
                first_invalid = next(
                    item for item in sample if type(item) not in valid_types
                )
                raise _type_err(first_invalid, valid_types[0])

            if feast_value_type == ValueType.UNIX_TIMESTAMP_LIST:
                int_timestamps_lists = (
                    _python_datetime_to_int_timestamp(value) for value in values
                )
                return [
                    # ProtoValue does actually accept `np.int_` but the typing complains.
                    ProtoValue(unix_timestamp_list_val=Int64List(val=ts))  # type: ignore
                    for ts in int_timestamps_lists
                ]

            return [
                ProtoValue(**{field_name: proto_type(val=value)})  # type: ignore
                if value is not None
                else ProtoValue()
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

        if feast_value_type in PYTHON_SCALAR_VALUE_TYPE_TO_PROTO_VALUE:
            (
                field_name,
                func,
                valid_scalar_types,
            ) = PYTHON_SCALAR_VALUE_TYPE_TO_PROTO_VALUE[feast_value_type]
            if valid_scalar_types:
                assert type(sample) in valid_scalar_types

            return [
                ProtoValue(**{field_name: func(value)})
                if not pd.isnull(value)
                else ProtoValue()
                for value in values
            ]

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

    return _python_value_to_proto_value(value_type, values)


def _proto_value_to_value_type(proto_value: ProtoValue) -> ValueType:
    """
    Returns Feast ValueType given Feast ValueType string.

    Args:
        proto_str: str

    Returns:
        A variant of ValueType.
    """
    proto_str = proto_value.WhichOneof("val")
    type_map = {
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
        None: ValueType.NULL,
    }

    return type_map[proto_str]


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
        "INT64": ValueType.INT64,
        "STRING": ValueType.STRING,
        "FLOAT": ValueType.DOUBLE,
        "FLOAT64": ValueType.DOUBLE,
        "BYTES": ValueType.BYTES,
        "BOOL": ValueType.BOOL,
        "BOOLEAN": ValueType.BOOL,  # legacy sql data type
        "NULL": ValueType.NULL,
    }

    value_type = type_map[bq_type_as_str]
    if is_list:
        value_type = ValueType[value_type.name + "_LIST"]

    return value_type


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
        # skip date, geometry, hllsketch, time, timetz
    }

    return type_map[redshift_type_as_str.lower()]


def snowflake_python_type_to_feast_value_type(
    snowflake_python_type_as_str: str,
) -> ValueType:

    type_map = {
        "str": ValueType.STRING,
        "float64": ValueType.DOUBLE,
        "int64": ValueType.INT64,
        "uint64": ValueType.INT64,
        "int32": ValueType.INT32,
        "uint32": ValueType.INT32,
        "int16": ValueType.INT32,
        "uint16": ValueType.INT32,
        "uint8": ValueType.INT32,
        "int8": ValueType.INT32,
        "datetime64[ns]": ValueType.UNIX_TIMESTAMP,
        "object": ValueType.UNKNOWN,
    }

    return type_map[snowflake_python_type_as_str.lower()]


def pa_to_redshift_value_type(pa_type: pyarrow.DataType) -> str:
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
