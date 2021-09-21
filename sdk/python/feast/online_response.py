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

from collections import defaultdict
from typing import Any, Dict, List, cast

import pandas as pd

from feast.feature_view import DUMMY_ENTITY_ID
from feast.protos.feast.serving.ServingService_pb2 import (
    GetOnlineFeaturesRequestV2,
    GetOnlineFeaturesResponse,
)
from feast.protos.feast.types.Value_pb2 import Value as Value
from feast.type_map import (
    _proto_value_to_value_type,
    _python_value_to_proto_value,
    feast_value_type_to_python_type,
    python_values_to_feast_value_type,
)
from feast.value_type import ValueType


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
        fields = [
            k
            for row in self.field_values
            for k, _ in row.statuses.items()
            if k != DUMMY_ENTITY_ID
        ]
        features_dict: Dict[str, List[Any]] = {k: list() for k in fields}

        for row in self.field_values:
            for feature in features_dict.keys():
                native_type_value = feast_value_type_to_python_type(row.fields[feature])
                features_dict[feature].append(native_type_value)

        return features_dict

    def to_df(self) -> pd.DataFrame:
        """
        Converts GetOnlineFeaturesResponse features into Panda dataframe form.
        """

        return pd.DataFrame(self.to_dict()).drop(
            DUMMY_ENTITY_ID, axis=1, errors="ignore"
        )


def _infer_online_entity_rows(
    entity_rows: List[Dict[str, Any]]
) -> List[GetOnlineFeaturesRequestV2.EntityRow]:
    """
    Builds a list of EntityRow protos from Python native type format passed by user.

    Args:
        entity_rows: A list of dictionaries where each key-value is an entity-name, entity-value pair.
    Returns:
        A list of EntityRow protos parsed from args.
    """

    entity_rows_dicts = cast(List[Dict[str, Any]], entity_rows)
    entity_row_list = []
    entity_type_map: Dict[str, ValueType] = dict()
    entity_python_values_map = defaultdict(list)

    # Flatten keys-value dicts into lists for type inference
    for entity in entity_rows_dicts:
        for key, value in entity.items():
            if isinstance(value, Value):
                inferred_type = _proto_value_to_value_type(value)
                # If any ProtoValues were present their types must all be the same
                if key in entity_type_map and entity_type_map.get(key) != inferred_type:
                    raise TypeError(
                        f"Input entity {key} has mixed types, {entity_type_map.get(key)} and {inferred_type}. That is not allowed."
                    )
                entity_type_map[key] = inferred_type
            else:
                entity_python_values_map[key].append(value)

    # Loop over all entities to infer dtype first in case of empty lists or nulls
    for key, values in entity_python_values_map.items():
        inferred_type = python_values_to_feast_value_type(key, values)

        # If any ProtoValues were present their types must match the inferred type
        if key in entity_type_map and entity_type_map.get(key) != inferred_type:
            raise TypeError(
                f"Input entity {key} has mixed types, {entity_type_map.get(key)} and {inferred_type}. That is not allowed."
            )

        entity_type_map[key] = inferred_type

    for entity in entity_rows_dicts:
        fields = {}
        for key, value in entity.items():
            if key not in entity_type_map:
                raise ValueError(
                    f"field {key} cannot have all null values for type inference."
                )

            if isinstance(value, Value):
                proto_value = value
            else:
                proto_value = _python_value_to_proto_value(entity_type_map[key], value)
            fields[key] = proto_value
        entity_row_list.append(GetOnlineFeaturesRequestV2.EntityRow(fields=fields))
    return entity_row_list
