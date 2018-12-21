import pandas as pd
from enum import Enum

class Granularity(Enum):
    NONE = 0
    DAY = 1
    HOUR = 2
    MINUTE = 3
    SECOND = 4

class ValueType(Enum):
    UNKNOWN = 0
    BYTES = 1
    STRING = 2
    INT32 = 3
    INT64 = 4
    DOUBLE = 5
    FLOAT = 6
    BOOL = 7
    TIMESTAMP = 8

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
    '''Returns the equivalent feast valueType for the given dtype
    
    Args:
        dtype (pandas.dtype): pandas dtype
    
    Returns:
        feast.types.ValueType2.ValueType: equivalent feast valuetype
    '''
    return _DTYPE_TO_VALUE_TYPE_MAPPING[dtype.__str__()]