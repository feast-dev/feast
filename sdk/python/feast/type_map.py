# Copyright 2018 The Feast Authors
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

from feast.value_type import ValueType
from feast.types.Value_pb2 import Value as ProtoValue, ValueType as ProtoValueType

# Mapping of feast value type to Pandas DataFrame dtypes
# Integer and floating values are all 64-bit for better integration
# with BigQuery data types
FEAST_VALUETYPE_TO_DTYPE = {
    "BYTES": np.byte,
    "STRING": np.object,
    "INT32": "Int32",  # Use pandas nullable int type
    "INT64": "Int64",  # Use pandas nullable int type
    "DOUBLE": np.float64,
    "FLOAT": np.float64,
    "BOOL": np.bool,
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


def pandas_value_to_proto_value(pandas_dtype, pandas_value) -> ProtoValue:
    value = ProtoValue()
    value_attr = dtype_to_feast_value_attr(pandas_dtype)
    if pandas_dtype.__str__() in ["datetime64[ns]", "datetime64[ns, UTC]"]: 
        pandas_value = int(pandas_value.timestamp())
    try:
        value.__setattr__(value_attr, pandas_value)
    except TypeError as type_error:
        # Numpy treats NaN as float. So if there is NaN values in column of
        # "str" type, __setattr__ will raise TypeError. This handles that case.
        if value_attr == "stringVal" and pd.isnull(pandas_value):
            value.__setattr__("stringVal", "")
        else:
            raise type_error

    return value
