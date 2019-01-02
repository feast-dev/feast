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