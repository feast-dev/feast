from typing import Dict, List, Optional

from attr import dataclass

from feast.feature import Feature
from feast.protos.feast.core.FeatureViewProjection_pb2 import (
    FeatureViewProjection as FeatureViewProjectionProto,
)


@dataclass
class FeatureViewProjection:
    name: str
    name_alias: Optional[str]
    features: List[Feature]
    join_key_map: Dict[str, str] = {}

    def name_to_use(self):
        return self.name_alias or self.name

    def to_proto(self):
        feature_reference_proto = FeatureViewProjectionProto(
            feature_view_name=self.name,
            feature_view_name_alias=self.name_alias,
            join_key_map=self.join_key_map,
        )
        for feature in self.features:
            feature_reference_proto.feature_columns.append(feature.to_proto())

        return feature_reference_proto

    @staticmethod
    def from_proto(proto: FeatureViewProjectionProto):
        ref = FeatureViewProjection(
            name=proto.feature_view_name,
            name_alias=proto.feature_view_name_alias,
            features=[],
            join_key_map=dict(proto.join_key_map),
        )
        for feature_column in proto.feature_columns:
            ref.features.append(Feature.from_proto(feature_column))

        return ref

    @staticmethod
    def from_definition(feature_grouping):
        return FeatureViewProjection(
            name=feature_grouping.name,
            name_alias=None,
            features=feature_grouping.features,
        )
