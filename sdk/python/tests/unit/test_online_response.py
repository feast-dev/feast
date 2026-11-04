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

from feast.online_response import OnlineResponse
from feast.protos.feast.serving.ServingService_pb2 import GetOnlineFeaturesResponse
from feast.protos.feast.types.Value_pb2 import Value


def _response(feature_name, values):
    resp = GetOnlineFeaturesResponse()
    resp.metadata.feature_names.val.extend([feature_name])
    vector = resp.results.add()
    for v in values:
        vector.values.append(v)
    return OnlineResponse(resp)


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
