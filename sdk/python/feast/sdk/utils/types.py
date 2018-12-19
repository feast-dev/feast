import pandas as pd
from feast.types.Value_pb2 import ValueType

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
        dtype (dtype): pandas dtype
    
    Returns:
        ValueType.Enum: equivalent feast valuetype
    '''
    return _DTYPE_TO_VALUE_TYPE_MAPPING[dtype.__str__()]