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

from feast.sdk.resources.feature import ValueType
import numpy as np

# mapping of pandas dtypes to feast value type strings
DTYPE_TO_VALUE_TYPE_MAPPING = {
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
    "datetime64[ns, tz]": ValueType.TIMESTAMP,
    "category": ValueType.STRING,
    "object": ValueType.STRING
}

# Mapping of feast value type to Pandas DataFrame dtypes
# Integer and floating values are all 64-bit for better integration
# with BigQuery data types
FEAST_VALUETYPE_TO_DTYPE = {
    "bytesVal": np.byte,
    "stringVal": np.object,
    "int32Val": np.int64,
    "int64Val": np.int64,
    "doubleVal": np.float64,
    "floatVal": np.float64,
    "boolVal": np.bool,
    "timestampVal": np.datetime64,
}


def dtype_to_value_type(dtype):
    """Returns the equivalent feast valueType for the given dtype

    Args:
        dtype (pandas.dtype): pandas dtype

    Returns:
        feast.types.ValueType2.ValueType: equivalent feast valuetype
    """
    return DTYPE_TO_VALUE_TYPE_MAPPING[dtype.__str__()]
