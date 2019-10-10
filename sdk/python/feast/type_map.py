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

from feast.serving.ServingService_pb2 import GetFeaturesRequest
from feast.value_type import ValueType
from feast.types.Value_pb2 import Value as ProtoValue, ValueType as ProtoValueType
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
        "float64": ValueType.FLOAT,
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

        for column, dtype in dataframe.dtypes.items():
            if column == DATETIME_COLUMN:
                continue

            feature_row.fields.extend(
                [
                    FieldProto.Field(
                        name=column, value=pd_value_to_proto_value(dtype, row[column])
                    )
                ]
            )
            return feature_row

    return convert_series_to_proto_values


def convert_df_to_entity_rows(dataframe: pd.DataFrame):
    def convert_series_to_proto_values(row: pd.Series):
        fields = {}

        for column, dtype in dataframe.dtypes.items():
            if column == DATETIME_COLUMN:
                continue

            fields[column] = pd_value_to_proto_value(
                dtype, row[dataframe.columns.get_loc(column)]
            )

        return GetFeaturesRequest.EntityRow(fields=fields)

    return convert_series_to_proto_values


def pd_datetime_to_timestamp_proto(dtype, value) -> Timestamp:
    if type(value) == np.float64:
        return Timestamp(seconds=int(value))
    if dtype.__str__() in ["datetime64[ns]", "datetime64[ns, UTC]"]:
        return Timestamp(seconds=int(value.timestamp()))
    else:
        return Timestamp(seconds=np.datetime64(value).astype("int64") // 1000000)


def pd_value_to_proto_value(dtype, value) -> ProtoValue:
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

    if pd.isnull(value):
        return ProtoValue()
    elif dtype == "int64":
        return ProtoValue(int64_val=int(value))
    elif dtype == "int32":
        return ProtoValue(int32_val=int(value))
    elif dtype == "object":
        return ProtoValue(string_val=value)
    elif dtype == "float64":
        return ProtoValue(float_val=value)
    elif dtype in type_map:
        raise NotImplemented(f"Data type known but not implemented: ${str(dtype)}")
    else:
        raise Exception(f"Unsupported data type: ${str(dtype)}")
