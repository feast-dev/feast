from typing import List

from attr import dataclass

from feast.feature import Feature
from feast.protos.feast.core.FeatureViewProjection_pb2 import (
    FeatureViewProjection as FeatureViewProjectionProto,
)


@dataclass
class FeatureViewProjection:
    name: str
    name_to_use: str
    features: List[Feature]

    def to_proto(self):
        feature_reference_proto = FeatureViewProjectionProto(
            feature_view_name=self.name, feature_view_name_to_use=self.name_to_use
        )
        for feature in self.features:
            feature_reference_proto.feature_columns.append(feature.to_proto())

        return feature_reference_proto

    def copy(self):
        return FeatureViewProjection(
            name=self.name, name_to_use=self.name_to_use, features=self.features,
        )

    @staticmethod
    def from_proto(proto: FeatureViewProjectionProto):
        ref = FeatureViewProjection(
            name=proto.feature_view_name,
            name_to_use=proto.feature_view_name_to_use,
            features=[],
        )
        for feature_column in proto.feature_columns:
            ref.features.append(Feature.from_proto(feature_column))

        return ref

    @staticmethod
    def from_definition(feature_grouping):
        return FeatureViewProjection(
            name=feature_grouping.name,
            name_to_use=feature_grouping.name,
            features=feature_grouping.features,
        )
