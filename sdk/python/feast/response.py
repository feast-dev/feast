# Copyright 2020 The Feast Authors
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

from typing import Any, Dict, List

from feast.serving.ServingService_pb2 import GetOnlineFeaturesResponse
from feast.type_map import feast_value_type_to_python_type


class OnlineResponse:
    """
    Defines a online response in feast.
    """

    def __init__(self, online_response_proto: GetOnlineFeaturesResponse):
        """
        Construct a native online response from its protobuf version.

        Args:
        online_response_proto: GetOnlineResponse proto object to construct from.
        """
        self.proto = online_response_proto

    @property
    def field_values(self):
        """
        Getter for GetOnlineResponse's field_values.
        """
        return self.proto.field_values

    def to_dict(self) -> Dict[str, Any]:
        """
        Converts GetOnlineFeaturesResponse features into a dictionary form.
        """
        fields = [k for row in self.field_values for k, _ in row.fields.items()]
        features_dict: Dict[str, List[Any]] = {k: list() for k in fields}

        for row in self.field_values:
            for feature in features_dict.keys():
                native_type_value = feast_value_type_to_python_type(row.fields[feature])
                features_dict[feature].append(native_type_value)

        return features_dict
