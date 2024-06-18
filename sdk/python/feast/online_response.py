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
import pyarrow as pa

from feast.feature_view import DUMMY_ENTITY_ID
from feast.protos.feast.serving.ServingService_pb2 import GetOnlineFeaturesResponse
from feast.type_map import feast_value_type_to_python_type

TIMESTAMP_POSTFIX: str = "__ts"


class OnlineResponse:
    """
    Defines an online response in feast.
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
                del self.proto.results[idx]

                break

    def to_dict(self, include_event_timestamps: bool = False) -> Dict[str, Any]:
        """
        Converts GetOnlineFeaturesResponse features into a dictionary form.

        Args:
        include_event_timestamps: bool Optionally include feature timestamps in the dictionary
        """
        response: Dict[str, List[Any]] = {}

        for feature_ref, feature_vector in zip(
            self.proto.metadata.feature_names.val, self.proto.results
        ):
            response[feature_ref] = [
                feast_value_type_to_python_type(v) for v in feature_vector.values
            ]

            if include_event_timestamps:
                timestamp_ref = feature_ref + TIMESTAMP_POSTFIX
                response[timestamp_ref] = [
                    ts.seconds for ts in feature_vector.event_timestamps
                ]

        return response

    def to_df(self, include_event_timestamps: bool = False) -> pd.DataFrame:
        """
        Converts GetOnlineFeaturesResponse features into Panda dataframe form.

        Args:
        include_event_timestamps: bool Optionally include feature timestamps in the dataframe
        """

        return pd.DataFrame(self.to_dict(include_event_timestamps))

    def to_arrow(self, include_event_timestamps: bool = False) -> pa.Table:
        """
        Converts GetOnlineFeaturesResponse features into pyarrow Table.

        Args:
        include_event_timestamps: bool Optionally include feature timestamps in the table
        """

        return pa.Table.from_pydict(self.to_dict(include_event_timestamps))
