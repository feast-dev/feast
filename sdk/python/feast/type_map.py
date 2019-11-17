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
    ValueType as ProtoValueType,
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

# Mapping of feast value type to Pandas DataFrame dtypes
# Integer and floating values are all 64-bit for better integration
# with BigQuery data types
FEAST_VALUE_TYPE_TO_DTYPE = {
    "BYTES": np.byte,
    "STRING": np.object,
    "INT32": "Int32",  # Use pandas nullable int type
    "INT64": "Int64",  # Use pandas nullable int type
    "DOUBLE": np.float64,
    "FLOAT": np.float64,
    "BOOL": np.bool,
}

FEAST_VALUE_ATTR_TO_DTYPE = {
    "bytes_val": np.byte,
    "string_val": np.object,
    "int32_val": "Int32",
    "int64_val": "Int64",
    "double_val": np.float64,
    "float_val": np.float64,
    "bool_val": np.bool,
}


def dtype_to_feast_value_attr(dtype):
    # Mapping of Pandas dtype to attribute name in Feast Value
    type_map = {
        "float64": "double_val",
        "float32": "float_val",
        "int64": "int64_val",
        "uint64": "int64_val",
        "int32": "int32_val",
        "uint32": "int32_val",
        "uint8": "int32_val",
        "int8": "int32_val",
        "bool": "bool_val",
        "timedelta": "int64_val",
        "datetime64[ns]": "int64_val",
        "datetime64[ns, UTC]": "int64_val",
        "category": "string_val",
        "object": "string_val",
    }
    return type_map[dtype.__str__()]


def dtype_to_value_type(dtype):
    """Returns the equivalent feast valueType for the given dtype
    Args:
        dtype (pandas.dtype): pandas dtype
    Returns:
        feast.types.ValueType2.ValueType: equivalent feast valuetype
    """
    # mapping of pandas dtypes to feast value type strings
    type_map = {
        "float64": ProtoValueType.DOUBLE,
        "float32": ProtoValueType.FLOAT,
        "int64": ProtoValueType.INT64,
        "uint64": ProtoValueType.INT64,
        "int32": ProtoValueType.INT32,
        "uint32": ProtoValueType.INT32,
        "uint8": ProtoValueType.INT32,
        "int8": ProtoValueType.INT32,
        "bool": ProtoValueType.BOOL,
        "timedelta": ProtoValueType.INT64,
        "datetime64[ns]": ProtoValueType.INT64,
        "datetime64[ns, UTC]": ProtoValueType.INT64,
        "category": ProtoValueType.STRING,
        "object": ProtoValueType.STRING,
    }
    return type_map[dtype.__str__()]


# TODO: to pass test_importer
def pandas_dtype_to_feast_value_type(dtype: pd.DataFrame.dtypes) -> ValueType:
    type_map = {
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
        "object": ValueType.STRING,
    }
    return type_map[dtype.__str__()]


def convert_df_to_feature_rows(dataframe: pd.DataFrame, feature_set):
    def convert_series_to_proto_values(row: pd.Series):
        feature_row = FeatureRowProto.FeatureRow(
            event_timestamp=pd_datetime_to_timestamp_proto(
                dataframe[DATETIME_COLUMN].dtype, row[DATETIME_COLUMN]
            ),
            feature_set=feature_set.name + ":" + str(feature_set.version),
        )

        for field_name, field in feature_set.fields.items():
            feature_row.fields.extend(
                [
                    FieldProto.Field(
                        name=field.name,
                        value=pd_value_to_proto_value(field.dtype, row[field.name]),
                    )
                ]
            )
        return feature_row

    return convert_series_to_proto_values


def pd_datetime_to_timestamp_proto(dtype, value) -> Timestamp:
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


def type_err(item, dtype):
    raise ValueError(f'Value "{item}" is of type {type(item)} not of type {dtype}')


def pd_value_to_proto_value(feast_value_type, value) -> ProtoValue:

    # Detect list type and handle separately
    if "list" in feast_value_type.name.lower():

        if feast_value_type == ValueType.FLOAT_LIST:
            return ProtoValue(
                float_list_val=FloatList(
                    val=[
                        item if type(item) is np.float32 else type_err(item, np.float32)
                        for item in value
                    ]
                )
            )

        if feast_value_type == ValueType.DOUBLE_LIST:
            return ProtoValue(
                double_list_val=DoubleList(
                    val=[
                        item if type(item) is np.float64 else type_err(item, np.float64)
                        for item in value
                    ]
                )
            )

        if feast_value_type == ValueType.INT32_LIST:
            return ProtoValue(
                int32_list_val=Int32List(
                    val=[
                        item if type(item) is np.int32 else type_err(item, np.int32)
                        for item in value
                    ]
                )
            )

        if feast_value_type == ValueType.INT64_LIST:
            return ProtoValue(
                int64_list_val=Int64List(
                    val=[
                        item if type(item) is np.int64 else type_err(item, np.int64)
                        for item in value
                    ]
                )
            )

        if feast_value_type == ValueType.STRING_LIST:
            return ProtoValue(
                string_list_val=StringList(
                    val=[
                        item if type(item) is np.str_ else type_err(item, np.str_)
                        for item in value
                    ]
                )
            )

        if feast_value_type == ValueType.BOOL_LIST:
            return ProtoValue(
                bool_list_val=BoolList(
                    val=[
                        item if type(item) is np.bool_ else type_err(item, np.bool_)
                        for item in value
                    ]
                )
            )

        if feast_value_type == ValueType.BYTES_LIST:
            return ProtoValue(
                bytes_list_val=BytesList(
                    val=[
                        item if type(item) is np.bytes_ else type_err(item, np.bytes_)
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
            assert type(value) is float
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
