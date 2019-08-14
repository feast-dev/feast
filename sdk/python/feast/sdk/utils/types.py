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

from feast.types.Value_pb2 import ValueType
from feast.sdk.resources.feature import ValueType as FeastValueType
import numpy as np


# Mapping of feast value type to Pandas DataFrame dtypes
# Integer and floating values are all 64-bit for better integration
# with BigQuery data types
FEAST_VALUETYPE_TO_DTYPE = {
    "bytesVal": np.byte,
    "stringVal": np.object,
    "int32Val": "Int32",  # Use pandas nullable int type
    "int64Val": "Int64",  # Use pandas nullable int type
    "doubleVal": np.float64,
    "floatVal": np.float64,
    "boolVal": np.bool,
    "timestampVal": np.datetime64,
}


def dtype_to_feast_value_attr(dtype):
    # Mapping of Pandas dtype to attribute name in Feast Value
    type_map = {
        "float64": "doubleVal",
        "float32": "floatVal",
        "int64": "int64Val",
        "uint64": "int64Val",
        "int32": "int32Val",
        "uint32": "int32Val",
        "uint8": "int32Val",
        "int8": "int32Val",
        "bool": "boolVal",
        "timedelta": "int64Val",
        "datetime64[ns]": "timestampVal",
        "datetime64[ns, UTC]": "timestampVal",
        "category": "stringVal",
        "object": "stringVal",
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
        "datetime64[ns]": ValueType.TIMESTAMP,
        "datetime64[ns, UTC]": ValueType.TIMESTAMP,
        "category": ValueType.STRING,
        "object": ValueType.STRING,
    }
    return type_map[dtype.__str__()]


# TODO: to pass test_importer
def dtype_to_feast_value_type(dtype):
    type_map = {
        "float64": FeastValueType.FLOAT,
        "float32": FeastValueType.FLOAT,
        "int64": FeastValueType.INT64,
        "uint64": FeastValueType.INT64,
        "int32": FeastValueType.INT32,
        "uint32": FeastValueType.INT32,
        "uint8": FeastValueType.INT32,
        "int8": FeastValueType.INT32,
        "bool": FeastValueType.BOOL,
        "timedelta": FeastValueType.INT64,
        "datetime64[ns]": FeastValueType.TIMESTAMP,
        "datetime64[ns, tz]": FeastValueType.TIMESTAMP,
        "category": FeastValueType.STRING,
        "object": FeastValueType.STRING,
    }
    return type_map[dtype.__str__()]
