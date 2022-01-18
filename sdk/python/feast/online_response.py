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

import pandas as pd

from feast.feature_view import DUMMY_ENTITY_ID
from feast.protos.feast.serving.ServingService_pb2 import GetOnlineFeaturesResponse
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
        # Delete DUMMY_ENTITY_ID from proto if it exists
        for idx, val in enumerate(self.proto.metadata.feature_names.val):
            if val == DUMMY_ENTITY_ID:
                del self.proto.metadata.feature_names.val[idx]
                for result in self.proto.results:
                    del result.values[idx]
                    del result.statuses[idx]
                    del result.event_timestamps[idx]
                break

    def to_dict(self) -> Dict[str, Any]:
        """
        Converts GetOnlineFeaturesResponse features into a dictionary form.
        """
        response: Dict[str, List[Any]] = {}

        for result in self.proto.results:
            for idx, feature_ref in enumerate(self.proto.metadata.feature_names.val):
                native_type_value = feast_value_type_to_python_type(result.values[idx])
                if feature_ref not in response:
                    response[feature_ref] = [native_type_value]
                else:
                    response[feature_ref].append(native_type_value)

        return response

    def to_df(self) -> pd.DataFrame:
        """
        Converts GetOnlineFeaturesResponse features into Panda dataframe form.
        """

        return pd.DataFrame(self.to_dict())
