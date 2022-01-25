# Copyright 2022 The Feast Authors
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
from typing import NamedTuple, Set

from feast.entity import Entity
from feast.feature_service import FeatureService
from feast.feature_view import FeatureView
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.request_feature_view import RequestFeatureView


class RepoContents(NamedTuple):
    """
    Represents the objects in a Feast feature repo.
    """

    feature_views: Set[FeatureView]
    on_demand_feature_views: Set[OnDemandFeatureView]
    request_feature_views: Set[RequestFeatureView]
    entities: Set[Entity]
    feature_services: Set[FeatureService]

    def to_registry_proto(self) -> RegistryProto:
        registry_proto = RegistryProto()
        registry_proto.entities.extend([e.to_proto() for e in self.entities])
        registry_proto.feature_views.extend(
            [fv.to_proto() for fv in self.feature_views]
        )
        registry_proto.on_demand_feature_views.extend(
            [fv.to_proto() for fv in self.on_demand_feature_views]
        )
        registry_proto.request_feature_views.extend(
            [fv.to_proto() for fv in self.request_feature_views]
        )
        registry_proto.feature_services.extend(
            [fs.to_proto() for fs in self.feature_services]
        )
        return registry_proto
