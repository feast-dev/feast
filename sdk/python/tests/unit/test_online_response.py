from datetime import datetime, timezone

from google.protobuf.timestamp_pb2 import Timestamp

from feast.online_response import OnlineResponse
from feast.protos.feast.serving.ServingService_pb2 import (
    FieldStatus,
    GetOnlineFeaturesResponse,
)
from feast.protos.feast.types.Value_pb2 import Value as ValueProto


def test_online_response_include_created_timestamps():
    # Construct a sample GetOnlineFeaturesResponse proto
    event_ts = Timestamp()
    event_ts.FromDatetime(datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc))

    created_ts = Timestamp()
    created_ts.FromDatetime(datetime(2026, 1, 1, 12, 5, 0, tzinfo=timezone.utc))

    proto = GetOnlineFeaturesResponse()
    proto.metadata.feature_names.val.append("driver_fv:conv_rate")

    proto.results.append(
        GetOnlineFeaturesResponse.FeatureVector(
            values=[ValueProto(float_val=0.85)],
            statuses=[FieldStatus.PRESENT],
            event_timestamps=[event_ts],
            created_timestamps=[created_ts],
        )
    )

    response = OnlineResponse(proto)

    # 1. Test dictionary with both timestamps enabled
    res_dict = response.to_dict(
        include_event_timestamps=True,
        include_created_timestamps=True,
    )
    assert "driver_fv:conv_rate" in res_dict
    assert "driver_fv:conv_rate__ts" in res_dict
    assert "driver_fv:conv_rate__created_timestamp" in res_dict
    assert res_dict["driver_fv:conv_rate__created_timestamp"] == [created_ts.seconds]

    # 2. Test DataFrame with both timestamps enabled
    df = response.to_df(
        include_event_timestamps=True,
        include_created_timestamps=True,
    )
    assert "driver_fv:conv_rate__created_timestamp" in df.columns
    assert df["driver_fv:conv_rate__created_timestamp"].iloc[0] == created_ts.seconds
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
