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

import numpy as np
import pandas as pd
from datetime import datetime, timezone
from feast.value_type import ValueType
from feast.types.Value_pb2 import (
    Value as ProtoValue,
    Int64List,
    Int32List,
    BoolList,
    BytesList,
    DoubleList,
    StringList,
    FloatList,
)
from feast.types import FeatureRow_pb2 as FeatureRowProto, Field_pb2 as FieldProto
from google.protobuf.timestamp_pb2 import Timestamp
from feast.constants import DATETIME_COLUMN


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

    if type_name == "ndarray":
        if recurse:

            # Convert to list type
            list_items = pd.core.series.Series(value)

            # This is the final type which we infer from the list
            common_item_value_type = None
            for item in list_items:
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
                f"Value type for field {name} is {value.dtype.__str__()} "
                f"but recursion is not allowed. Array types can only be one "
                f"level deep."
            )

    return type_map[value.dtype.__str__()]


def convert_df_to_feature_rows(dataframe: pd.DataFrame, feature_set):
    """
    Returns a function that converts a Pandas Series to a Feast FeatureRow
    for a given Feature Set and Pandas Dataframe

    Args:
        dataframe: Dataframe that will be converted
        feature_set: Feature set used as schema for conversion

    Returns:
        Function that will do conversion
    """

    def convert_series_to_proto_values(row: pd.Series):
        """
        Converts a Pandas Series to a Feast FeatureRow

        Args:
            row: pd.Series The row that should be converted

        Returns:
            Feast FeatureRow
        """

        feature_row = FeatureRowProto.FeatureRow(
            event_timestamp=_pd_datetime_to_timestamp_proto(
                dataframe[DATETIME_COLUMN].dtype, row[DATETIME_COLUMN]
            ),
            feature_set=feature_set.name + ":" + str(feature_set.version),
        )

        for field_name, field in feature_set.fields.items():
            feature_row.fields.extend(
                [
                    FieldProto.Field(
                        name=field.name,
                        value=_python_value_to_proto_value(
                            field.dtype, row[field.name]
                        ),
                    )
                ]
            )
        return feature_row

    return convert_series_to_proto_values


def convert_dict_to_proto_values(
    row: dict, df_datetime_dtype: pd.DataFrame.dtypes, feature_set
) -> FeatureRowProto.FeatureRow:
    """
    Encode a dictionary describing a feature row into a FeatureRows object.

    Args:
        row: Dictionary describing a feature row.
        df_datetime_dtype:  Pandas dtype of datetime column.
        feature_set: Feature set describing feature row.

    Returns:
        FeatureRow
    """

    feature_row = FeatureRowProto.FeatureRow(
        event_timestamp=_pd_datetime_to_timestamp_proto(
            df_datetime_dtype, row[DATETIME_COLUMN]
        ),
        feature_set=feature_set.name + ":" + str(feature_set.version),
    )

    for field_name, field in feature_set.fields.items():
        feature_row.fields.extend(
            [
                FieldProto.Field(
                    name=field.name,
                    value=_python_value_to_proto_value(field.dtype, row[field.name]),
                )
            ]
        )
    return feature_row


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
