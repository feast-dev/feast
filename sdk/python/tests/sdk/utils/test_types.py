import pytest
import pandas as pd
import numpy as np 

from feast.sdk.utils.types import dtype_to_value_type, ValueType

def test_convert_dtype_to_value_type():
    dft = pd.DataFrame(dict(A = np.random.rand(3),
                            B = 1,
                            C = 'foo',
                            D = pd.Timestamp('20010102'),
                            E = pd.Series([1.0]*3).astype('float32'),
                            F = False,
                            G = pd.Series([1]*3,dtype='int8')))
    
    assert dtype_to_value_type(dft['A'].dtype) == ValueType.DOUBLE
    assert dtype_to_value_type(dft['B'].dtype) == ValueType.INT64
    assert dtype_to_value_type(dft['C'].dtype) == ValueType.STRING
    assert dtype_to_value_type(dft['D'].dtype) == ValueType.TIMESTAMP
    assert dtype_to_value_type(dft['E'].dtype) == ValueType.FLOAT
    assert dtype_to_value_type(dft['F'].dtype) == ValueType.BOOL
    assert dtype_to_value_type(dft['G'].dtype) == ValueType.INT32