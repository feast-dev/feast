from datetime import datetime
from typing import Dict, List, Optional

from google.protobuf.json_format import MessageToJson
from google.protobuf.timestamp_pb2 import Timestamp
from typeguard import typechecked

from feast.protos.feast.core.Model_pb2 import ModelMetadata as ModelMetadataProto


@typechecked
class ModelMetadata:
    """
    ModelMetadata represents metadata associated with a trained model in Feast.

    Attributes:
        name: Name for the model.
        feature_view: (Optional) Name of associated feature view.
        feature_service: (Optional) Name of associated feature service.
        features: (Optional) List of fully qualified feature references.
        project: Project the model belongs to.
        tags: Metadata tags (e.g., version, type).
        training_timestamp: Timestamp when the model was trained.
        description: Human-readable model description.
    """

    def __init__(
        self,
        *,
        name: str,
        project: str,
        feature_view: Optional[List[str]] = None,
        feature_service: Optional[List[str]] = None,
        features: Optional[List[str]] = None,
        tags: Optional[Dict[str, str]] = None,
        training_timestamp: Optional[datetime] = None,
        description: str = "",
    ):
        self.name = name
        self.project = project
        self.feature_view = feature_view or []
        self.feature_service = feature_service or []
        self.features = features or []
        self.tags = tags or {}
        self.training_timestamp = training_timestamp
        self.description = description

    def __repr__(self):
        return (
            f"ModelMetadata(\n"
            f"  name={self.name!r},\n"
            f"  project={self.project!r},\n"
            f"  feature_view={self.feature_view!r},\n"
            f"  feature_service={self.feature_service!r},\n"
            f"  features={self.features!r},\n"
            f"  tags={self.tags!r},\n"
            f"  training_timestamp={self.training_timestamp!r},\n"
            f"  description={self.description!r},\n"
            f")"
        )

    def __eq__(self, other):
        if not isinstance(other, ModelMetadata):
            return False
        return (
            self.name == other.name
            and self.project == other.project
            and self.feature_view == other.feature_view
            and self.feature_service == other.feature_service
            and self.features == other.features
            and self.tags == other.tags
            and self.training_timestamp == other.training_timestamp
            and self.description == other.description
        )

    def __str__(self):
        return str(MessageToJson(self.to_proto()))

    def to_proto(self) -> ModelMetadataProto:
        proto = ModelMetadataProto(
            name=self.name,
            project=self.project,
            description=self.description,
        )
        if self.features:
            proto.features.extend(self.features)
        if self.feature_view:
            proto.feature_view.extend(self.feature_view)
        if self.feature_service:
            proto.feature_service.extend(self.feature_service)
        if self.tags:
            proto.tags.update(self.tags)
        if self.training_timestamp:
            ts = Timestamp()
            ts.FromDatetime(self.training_timestamp)
            proto.training_timestamp.CopyFrom(ts)
        return proto

    @classmethod
    def from_proto(cls, proto: ModelMetadataProto) -> "ModelMetadata":
        training_ts: Optional[datetime] = None
        if proto.HasField("training_timestamp"):
            training_ts = proto.training_timestamp.ToDatetime()

        return cls(
            name=proto.name,
            project=proto.project,
            feature_view=list(proto.feature_view),
            feature_service=list(proto.feature_service),
            features=list(proto.features),
            tags=dict(proto.tags),
            training_timestamp=training_ts,
            description=proto.description,
        )
