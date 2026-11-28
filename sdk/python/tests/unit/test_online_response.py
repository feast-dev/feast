# Copyright 2025 The Feast Authors
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

import math

import pyarrow as pa
import pytest

from feast.online_response import _ALL_MISSING_ARROW_TYPES, OnlineResponse
from feast.protos.feast.serving.ServingService_pb2 import GetOnlineFeaturesResponse
from feast.protos.feast.types.Value_pb2 import (
    BoolList,
    BytesList,
    DoubleList,
    FloatList,
    Int32List,
    Int64List,
    StringList,
    Value,
)
from feast.value_type import ValueType


def _response(feature_name, values, feature_types=None):
    resp = GetOnlineFeaturesResponse()
    resp.metadata.feature_names.val.extend([feature_name])
    vector = resp.results.add()
    for v in values:
        vector.values.append(v)
    return OnlineResponse(resp, feature_types=feature_types)


def test_to_tensor_string_feature_with_missing_first_row():
    # driver 0 is not in the online store -> null; driver 1 has a name.
    response = _response("name", [Value(), Value(string_val="John")])

    assert response.to_dict() == {"name": [None, "John"]}

    tensors = response.to_tensor()

    assert isinstance(tensors["name"], list)
    assert math.isnan(tensors["name"][0])
    assert tensors["name"][1] == "John"


def test_to_tensor_numeric_feature_all_missing_still_tensor():
    response = _response("trips", [Value(), Value()])

    tensors = response.to_tensor()

    # numeric-looking default keeps the tensor path (regression guard)
    assert hasattr(tensors["trips"], "shape")
    assert tensors["trips"].shape[0] == 2


_UUID = "00000000-0000-0000-0000-000000000001"


_PRESENT_VALUES = [
    (ValueType.INT32, Value(int32_val=1)),
    (ValueType.INT64, Value(int64_val=1)),
    (ValueType.FLOAT, Value(float_val=0.5)),
    (ValueType.DOUBLE, Value(double_val=0.5)),
    (ValueType.STRING, Value(string_val="a")),
    (ValueType.BYTES, Value(bytes_val=b"a")),
    (ValueType.PDF_BYTES, Value(bytes_val=b"%PDF")),
    (ValueType.IMAGE_BYTES, Value(bytes_val=b"\x89PNG")),
    (ValueType.BOOL, Value(bool_val=True)),
    (ValueType.UNIX_TIMESTAMP, Value(unix_timestamp_val=1700000000)),
    (ValueType.UUID, Value(uuid_val=_UUID)),
    (ValueType.TIME_UUID, Value(time_uuid_val=_UUID)),
    (ValueType.INT32_LIST, Value(int32_list_val=Int32List(val=[1]))),
    (ValueType.INT64_LIST, Value(int64_list_val=Int64List(val=[1]))),
    (ValueType.FLOAT_LIST, Value(float_list_val=FloatList(val=[0.5]))),
    (ValueType.DOUBLE_LIST, Value(double_list_val=DoubleList(val=[0.5]))),
    (ValueType.STRING_LIST, Value(string_list_val=StringList(val=["a"]))),
    (ValueType.BYTES_LIST, Value(bytes_list_val=BytesList(val=[b"a"]))),
    (ValueType.BOOL_LIST, Value(bool_list_val=BoolList(val=[True]))),
    (
        ValueType.UNIX_TIMESTAMP_LIST,
        Value(unix_timestamp_list_val=Int64List(val=[1700000000])),
    ),
    (ValueType.UUID_LIST, Value(uuid_list_val=StringList(val=[_UUID]))),
    (ValueType.TIME_UUID_LIST, Value(time_uuid_list_val=StringList(val=[_UUID]))),
]


@pytest.mark.parametrize(
    "value_type,present",
    _PRESENT_VALUES,
    ids=[value_type.name for value_type, _ in _PRESENT_VALUES],
)
def test_to_arrow_all_missing_feature_keeps_its_type(value_type, present):
    # A batch where no entity has the feature must not change the column type.
    found = _response("f", [present, Value()], {"f": value_type})
    missing = _response("f", [Value(), Value()], {"f": value_type})

    expected = found.to_arrow().schema.field("f").type
    table = missing.to_arrow()

    assert table.schema.field("f").type == expected
    assert table.column("f").to_pylist() == [None, None]


def test_to_arrow_all_missing_cases_cover_every_mapped_type():
    assert {value_type for value_type, _ in _PRESENT_VALUES} == set(
        _ALL_MISSING_ARROW_TYPES
    )


def test_to_arrow_all_missing_feature_with_full_feature_name():
    response = _response(
        "driver_fv__trips",
        [Value(), Value()],
        {"trips": ValueType.INT64, "driver_fv__trips": ValueType.INT64},
    )

    assert response.to_arrow().schema.field("driver_fv__trips").type == pa.int64()


def test_to_arrow_all_missing_feature_without_known_type_stays_null():
    response = _response("f", [Value(), Value()], {})

    assert response.to_arrow().schema.field("f").type == pa.null()


def test_to_arrow_partially_missing_feature_is_unchanged():
    response = _response("f", [Value(float_val=0.5), Value()], {"f": ValueType.FLOAT})

    table = response.to_arrow()

    assert table.schema.field("f").type == pa.float64()
    assert table.column("f").to_pylist() == [0.5, None]


def test_to_arrow_zero_rows_keeps_feature_type():
    response = _response("f", [], {"f": ValueType.INT64})

    table = response.to_arrow()

    assert table.num_rows == 0
    assert table.schema.field("f").type == pa.int64()
