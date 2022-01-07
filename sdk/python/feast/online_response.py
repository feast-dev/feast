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
        for item in self.proto.field_values:
            if DUMMY_ENTITY_ID in item.statuses:
                del item.statuses[DUMMY_ENTITY_ID]
            if DUMMY_ENTITY_ID in item.fields:
                del item.fields[DUMMY_ENTITY_ID]

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
        # Status for every Feature should be present in every record.
        features_dict: Dict[str, List[Any]] = {
            k: list() for k in self.field_values[0].statuses.keys()
        }
        rows = [record.fields for record in self.field_values]

        # Find the first non-null instance of each Feature to determine
        # which ValueType.
        val_types = {k: None for k in features_dict.keys()}
        for feature in features_dict.keys():
            for row in rows:
                try:
                    val_types[feature] = row[feature].WhichOneof("val")
                except KeyError:
                    continue
                if val_types[feature] is not None:
                    break

        # Now we know what attribute to fetch.
        for feature, val_type in val_types.items():
            if val_type is None:
                features_dict[feature] = [None] * len(rows)
            else:
                for row in rows:
                    val = getattr(row[feature], val_type)
                    if "_list_" in val_type:
                        val = list(val.val)
                    features_dict[feature].append(val)

        return features_dict

    def to_df(self) -> pd.DataFrame:
        """
        Converts GetOnlineFeaturesResponse features into Panda dataframe form.
        """

        return pd.DataFrame(self.to_dict())
