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

# mapping of pandas dtypes to feast value type strings
_DTYPE_TO_VALUE_TYPE_MAPPING = {
    "float64": ValueType.DOUBLE,
    "float32": ValueType.FLOAT,
    "int64": ValueType.INT64,
    "int32": ValueType.INT32,
    "int8": ValueType.INT32,
    "bool": ValueType.BOOL,
    "timedelta": ValueType.INT64,
    "datetime64[ns]": ValueType.TIMESTAMP,
    "datetime64[ns, tz]": ValueType.TIMESTAMP,
    "category": ValueType.STRING,
    "object": ValueType.STRING
}


def dtype_to_value_type(dtype):
    """Returns the equivalent feast valueType for the given dtype

    Args:
        dtype (pandas.dtype): pandas dtype

    Returns:
        feast.types.ValueType2.ValueType: equivalent feast valuetype
    """
    return _DTYPE_TO_VALUE_TYPE_MAPPING[dtype.__str__()]
