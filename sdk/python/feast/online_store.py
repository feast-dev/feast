# Copyright 2019 The Feast Authors
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
from abc import ABC, abstractmethod
from typing import Any

import pyarrow

from feast.feature_view import FeatureView
from feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.types.Value_pb2 import Value as ValueProto


class OnlineStore(ABC):
    @abstractmethod
    def ingest(self, table: pyarrow.Table, feature_view: FeatureView):
        pass


class FirestoreOnlineStore(OnlineStore):
    def ingest(self, table: pyarrow.Table, feature_view: FeatureView):
        rows_to_write = []
        for row in zip(*table.to_pydict().values()):
            entity_key = EntityKeyProto()
            for entity in feature_view.entities:
                entity_key.entity_names.append(entity.name)
                idx = table.column_names.index(entity.name)
                value = _convert_to_proto(row[idx])
                entity_key.entity_values.append(value)
            feature_dict = {}
            for feature in feature_view.features:
                idx = table.column_names.index(feature.name)
                value = _convert_to_proto(row[idx])
                feature_dict[feature.name] = value
            event_timestamp_idx = table.column_names.index(
                feature_view.inputs.event_timestamp_column
            )
            rows_to_write.append((entity_key, feature_dict, row[event_timestamp_idx]))
        print(rows_to_write)


def _convert_to_proto(value: Any) -> ValueProto:
    value_proto = ValueProto()
    if isinstance(value, str):
        value_proto.string_val = value
    elif isinstance(value, bool):
        value_proto.bool_val = value
    elif isinstance(value, int):
        value_proto.int32_val = value
    elif isinstance(value, float):
        value_proto.double_val = value
    elif isinstance(value, bytes):
        value_proto.bytes_val = value
    else:
        raise ValueError(f"Cannot convert value {value} of type {type(value)}.")
    return value_proto
