from datetime import datetime
from typing import List, Optional, Union

from feast.feature_reference import FeatureReference
from feast.feature_table import FeatureTable
from feast.feature_view import FeatureView
from feast.protos.feast.core.FeatureService_pb2 import (
    FeatureService as FeatureServiceProto,
)
from feast.protos.feast.core.FeatureService_pb2 import (
    FeatureServiceMeta,
    FeatureServiceSpec,
)


class FeatureService:
    name: str
    features: List[FeatureReference]
    created_timestamp: Optional[datetime] = None
    last_updated_timestamp: Optional[datetime] = None

    def __init__(
        self,
        name: str,
        features: List[Union[FeatureTable, FeatureView, FeatureReference]],
    ):
        self.name = name
        self.features = []
        for feature in features:
            if isinstance(feature, FeatureTable) or isinstance(feature, FeatureView):
                self.features.append(FeatureReference.from_definition(feature))
            elif isinstance(feature, FeatureReference):
                self.features.append(feature)
            else:
                raise ValueError(f"Unexpected type: {type(feature)}")

    def __eq__(self, other):
        pass

    @staticmethod
    def from_proto(feature_service_proto: FeatureServiceProto):
        fs = FeatureService(
            name=feature_service_proto.spec.name,
            features=[
                FeatureReference.from_proto(fp)
                for fp in feature_service_proto.spec.features
            ],
        )

        if feature_service_proto.meta.HasField("created_timestamp"):
            fs.created_timestamp = (
                feature_service_proto.meta.created_timestamp.ToDatetime()
            )
        if feature_service_proto.meta.HasField("last_updated_timestamp"):
            fs.last_updated_timestamp = (
                feature_service_proto.meta.last_updated_timestamp.ToDatetime()
            )

        return fs

    def to_proto(self):
        meta = FeatureServiceMeta()
        if self.created_timestamp:
            meta.created_timestamp.FromDatetime(self.created_timestamp)

        spec = FeatureServiceSpec()
        spec.name = self.name
        for definition in self.features:
            if isinstance(definition, FeatureTable) or isinstance(
                definition, FeatureView
            ):
                feature_ref = FeatureReference(definition.name, definition.features)
            else:
                feature_ref = definition

            spec.features.append(feature_ref.to_proto())

        feature_service_proto = FeatureServiceProto(spec=spec, meta=meta)
        return feature_service_proto

    def validate(self):
        pass
