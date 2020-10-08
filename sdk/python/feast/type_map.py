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
from typing import Any, Dict, List, Union

import numpy as np
import pandas as pd
import pyarrow as pa
from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp
from pyarrow.lib import TimestampType

from feast.types.Value_pb2 import (
    BoolList,
    BytesList,
    DoubleList,
    FloatList,
    Int32List,
    Int64List,
    StringList,
)
from feast.types.Value_pb2 import Value as ProtoValue
from feast.types.Value_pb2 import ValueType as ProtoValueType
from feast.value_type import ValueType


def feast_value_type_to_python_type(field_value_proto: ProtoValue) -> Any:
    """
    Converts field value Proto to Dict and returns each field's Feast Value Type value
    in their respective Python value.

    Args:
        field_value_proto: Field value Proto

    Returns:
        Python native type representation/version of the given field_value_proto
    """
    field_value_dict = MessageToDict(field_value_proto)

    for k, v in field_value_dict.items():
        if k == "int64Val":
            return int(v)
        if k == "bytesVal":
            return bytes(v)
        if (k == "int64ListVal") or (k == "int32ListVal"):
            return [int(item) for item in v["val"]]
        if (k == "floatListVal") or (k == "doubleListVal"):
            return [float(item) for item in v["val"]]
        if k == "stringListVal":
            return [str(item) for item in v["val"]]
        if k == "bytesListVal":
            return [bytes(item) for item in v["val"]]
        if k == "boolListVal":
            return [bool(item) for item in v["val"]]

        if k in ["int32Val", "floatVal", "doubleVal", "stringVal", "boolVal"]:
            return v
        else:
            raise TypeError(
                f"Casting to Python native type for type {k} failed. "
                f"Type {k} not found"
            )


def python_type_to_feast_value_type(
    name: str, value, recurse: bool = True
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

    type_name = type(value).__name__

    type_map = {
        "int": ValueType.INT64,
        "str": ValueType.STRING,
        "float": ValueType.DOUBLE,
        "bytes": ValueType.BYTES,
        "float64": ValueType.DOUBLE,
        "float32": ValueType.FLOAT,
        "int64": ValueType.INT64,
        "uint64": ValueType.INT64,
        "int32": ValueType.INT32,
        "uint32": ValueType.INT32,
        "uint8": ValueType.INT32,
        "int8": ValueType.INT32,
        "bool": ValueType.BOOL,
        "timedelta": ValueType.INT64,
        "datetime64[ns]": ValueType.INT64,
        "datetime64[ns, tz]": ValueType.INT64,
        "category": ValueType.STRING,
    }

    if type_name in type_map:
        return type_map[type_name]

    if type_name == "ndarray" or isinstance(value, list):
        if recurse:

            # Convert to list type
            list_items = pd.core.series.Series(value)

            # This is the final type which we infer from the list
            common_item_value_type = None
            for item in list_items:
                if isinstance(item, ProtoValue):
                    current_item_value_type = _proto_str_to_value_type(
                        item.WhichOneof("val")
                    )
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
                raise ValueError(
                    f"field {name} cannot have null values for type inference."
                )
            return ValueType[common_item_value_type.name + "_LIST"]
        else:
            raise ValueError(
                f"Value type for field {name} is {value.dtype.__str__()} but "
                f"recursion is not allowed. Array types can only be one level "
                f"deep."
            )

    return type_map[value.dtype.__str__()]


def _pd_datetime_to_timestamp_proto(dtype, value) -> Timestamp:
    """
    Converts a Pandas datetime to a Timestamp Proto

    Args:
        dtype: Pandas datatype
        value: Value of datetime

    Returns:
        Timestamp protobuf value
    """

    if type(value) in [np.float64, np.float32, np.int32, np.int64]:
        return Timestamp(seconds=int(value))
    if dtype.__str__() == "datetime64[ns]":
        # If timestamp does not contain timezone, we assume it is of local
        # timezone and adjust it to UTC
        local_timezone = datetime.now(timezone.utc).astimezone().tzinfo
        value = value.tz_localize(local_timezone).tz_convert("UTC").tz_localize(None)
        return Timestamp(seconds=int(value.timestamp()))
    if dtype.__str__() == "datetime64[ns, UTC]":
        return Timestamp(seconds=int(value.timestamp()))
    else:
        return Timestamp(seconds=np.datetime64(value).astype("int64") // 1000000)


def _type_err(item, dtype):
    raise ValueError(f'Value "{item}" is of type {type(item)} not of type {dtype}')


def _python_value_to_proto_value(feast_value_type, value) -> ProtoValue:
    """
    Converts a Python (native, pandas) value to a Feast Proto Value based
    on a provided value type

    Args:
        feast_value_type: The target value type
        value: Value that will be converted

    Returns:
        Feast Value Proto
    """

    # Detect list type and handle separately
    if "list" in feast_value_type.name.lower():

        if feast_value_type == ValueType.FLOAT_LIST:
            return ProtoValue(
                float_list_val=FloatList(
                    val=[
                        item
                        if type(item) in [np.float32, np.float64]
                        else _type_err(item, np.float32)
                        for item in value
                    ]
                )
            )

        if feast_value_type == ValueType.DOUBLE_LIST:
            return ProtoValue(
                double_list_val=DoubleList(
                    val=[
                        item
                        if type(item) in [np.float64, np.float32]
                        else _type_err(item, np.float64)
                        for item in value
                    ]
                )
            )

        if feast_value_type == ValueType.INT32_LIST:
            return ProtoValue(
                int32_list_val=Int32List(
                    val=[
                        item if type(item) is np.int32 else _type_err(item, np.int32)
                        for item in value
                    ]
                )
            )

        if feast_value_type == ValueType.INT64_LIST:
            return ProtoValue(
                int64_list_val=Int64List(
                    val=[
                        item
                        if type(item) in [np.int64, np.int32]
                        else _type_err(item, np.int64)
                        for item in value
                    ]
                )
            )

        if feast_value_type == ValueType.STRING_LIST:
            return ProtoValue(
                string_list_val=StringList(
                    val=[
                        item
                        if type(item) in [np.str_, str]
                        else _type_err(item, np.str_)
                        for item in value
                    ]
                )
            )

        if feast_value_type == ValueType.BOOL_LIST:
            return ProtoValue(
                bool_list_val=BoolList(
                    val=[
                        item
                        if type(item) in [np.bool_, bool]
                        else _type_err(item, np.bool_)
                        for item in value
                    ]
                )
            )

        if feast_value_type == ValueType.BYTES_LIST:
            return ProtoValue(
                bytes_list_val=BytesList(
                    val=[
                        item
                        if type(item) in [np.bytes_, bytes]
                        else _type_err(item, np.bytes_)
                        for item in value
                    ]
                )
            )

    # Handle scalar types below
    else:
        if pd.isnull(value):
            return ProtoValue()
        elif feast_value_type == ValueType.INT32:
            return ProtoValue(int32_val=int(value))
        elif feast_value_type == ValueType.INT64:
            return ProtoValue(int64_val=int(value))
        elif feast_value_type == ValueType.FLOAT:
            return ProtoValue(float_val=float(value))
        elif feast_value_type == ValueType.DOUBLE:
            assert type(value) is float or np.float64
            return ProtoValue(double_val=value)
        elif feast_value_type == ValueType.STRING:
            return ProtoValue(string_val=str(value))
        elif feast_value_type == ValueType.BYTES:
            assert type(value) is bytes
            return ProtoValue(bytes_val=value)
        elif feast_value_type == ValueType.BOOL:
            assert type(value) is bool
            return ProtoValue(bool_val=value)

    raise Exception(f"Unsupported data type: ${str(type(value))}")


def _proto_str_to_value_type(proto_str: str) -> ValueType:
    """
    Returns Feast ValueType given Feast ValueType string.

    Args:
        proto_str: str

    Returns:
        A variant of ValueType.
    """
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
    }

    return type_map[proto_str]


def pa_to_feast_value_attr(pa_type: object):
    """
    Returns the equivalent Feast ValueType string for the given pa.lib type.

    Args:
        pa_type (object):
            PyArrow type.

    Returns:
        str:
            Feast attribute name in Feast ValueType string-ed representation.
    """
    # Mapping of PyArrow type to attribute name in Feast ValueType strings
    type_map = {
        "timestamp[ms]": "int64_val",
        "int32": "int32_val",
        "int64": "int64_val",
        "double": "double_val",
        "float": "float_val",
        "string": "string_val",
        "binary": "bytes_val",
        "bool": "bool_val",
        "list<item: int32>": "int32_list_val",
        "list<item: int64>": "int64_list_val",
        "list<item: double>": "double_list_val",
        "list<item: float>": "float_list_val",
        "list<item: string>": "string_list_val",
        "list<item: binary>": "bytes_list_val",
        "list<item: bool>": "bool_list_val",
    }

    return type_map[pa_type.__str__()]


def pa_to_value_type(pa_type: object):
    """
    Returns the equivalent Feast ValueType for the given pa.lib type.

    Args:
        pa_type (object):
            PyArrow type.

    Returns:
        feast.types.Value_pb2.ValueType:
            Feast ValueType.

    """

    # Mapping of PyArrow to attribute name in Feast ValueType
    type_map = {
        "timestamp[ms]": ProtoValueType.INT64,
        "int32": ProtoValueType.INT32,
        "int64": ProtoValueType.INT64,
        "double": ProtoValueType.DOUBLE,
        "float": ProtoValueType.FLOAT,
        "string": ProtoValueType.STRING,
        "binary": ProtoValueType.BYTES,
        "bool": ProtoValueType.BOOL,
        "list<item: int32>": ProtoValueType.INT32_LIST,
        "list<item: int64>": ProtoValueType.INT64_LIST,
        "list<item: double>": ProtoValueType.DOUBLE_LIST,
        "list<item: float>": ProtoValueType.FLOAT_LIST,
        "list<item: string>": ProtoValueType.STRING_LIST,
        "list<item: binary>": ProtoValueType.BYTES_LIST,
        "list<item: bool>": ProtoValueType.BOOL_LIST,
    }
    return type_map[pa_type.__str__()]


def pa_to_feast_value_type(value: pa.lib.ChunkedArray) -> ValueType:
    type_map = {
        "timestamp[ms]": ValueType.INT64,
        "int32": ValueType.INT32,
        "int64": ValueType.INT64,
        "double": ValueType.DOUBLE,
        "float": ValueType.FLOAT,
        "string": ValueType.STRING,
        "binary": ValueType.BYTES,
        "bool": ValueType.BOOL,
        "list<item: int32>": ValueType.INT32_LIST,
        "list<item: int64>": ValueType.INT64_LIST,
        "list<item: double>": ValueType.DOUBLE_LIST,
        "list<item: float>": ValueType.FLOAT_LIST,
        "list<item: string>": ValueType.STRING_LIST,
        "list<item: binary>": ValueType.BYTES_LIST,
        "list<item: bool>": ValueType.BOOL_LIST,
    }
    return type_map[value.type.__str__()]


def pa_column_to_timestamp_proto_column(column: pa.lib.ChunkedArray) -> List[Timestamp]:
    if not isinstance(column.type, TimestampType):
        raise Exception("Only TimestampType columns are allowed")

    proto_column = []
    for val in column:
        timestamp = Timestamp()
        timestamp.FromMicroseconds(micros=int(val.as_py().timestamp() * 1_000_000))
        proto_column.append(timestamp)
    return proto_column


def pa_column_to_proto_column(
    feast_value_type: ValueType, column: pa.lib.ChunkedArray
) -> List[ProtoValue]:
    type_map: Dict[ValueType, Union[str, Dict[str, Any]]] = {
        ValueType.INT32: "int32_val",
        ValueType.INT64: "int64_val",
        ValueType.FLOAT: "float_val",
        ValueType.DOUBLE: "double_val",
        ValueType.STRING: "string_val",
        ValueType.BYTES: "bytes_val",
        ValueType.BOOL: "bool_val",
        ValueType.BOOL_LIST: {"bool_list_val": BoolList},
        ValueType.BYTES_LIST: {"bytes_list_val": BytesList},
        ValueType.STRING_LIST: {"string_list_val": StringList},
        ValueType.FLOAT_LIST: {"float_list_val": FloatList},
        ValueType.DOUBLE_LIST: {"double_list_val": DoubleList},
        ValueType.INT32_LIST: {"int32_list_val": Int32List},
        ValueType.INT64_LIST: {"int64_list_val": Int64List},
    }

    value: Union[str, Dict[str, Any]] = type_map[feast_value_type]
    # Process list types
    if isinstance(value, dict):
        list_param_name = list(value.keys())[0]
        return [
            ProtoValue(**{list_param_name: value[list_param_name](val=x.as_py())})
            for x in column
        ]
    else:
        return [ProtoValue(**{value: x.as_py()}) for x in column]
