import inspect
from datetime import datetime
from typing import Dict, List, Optional, Union

from google.protobuf.json_format import MessageToJson

from feast.feature_table import FeatureTable
from feast.feature_view import FeatureView
from feast.importer import get_calling_file_name
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.protos.feast.core.FeatureService_pb2 import (
    FeatureService as FeatureServiceProto,
)
from feast.protos.feast.core.FeatureService_pb2 import (
    FeatureServiceMeta,
    FeatureServiceSpec,
)
from feast.usage import log_exceptions


class FeatureService:
    """
    A feature service is a logical grouping of features for retrieval (training or serving).
    The features grouped by a feature service may come from any number of feature views.

    Args:
        name: Unique name of the feature service.
        features: A list of Features that are grouped as part of this FeatureService.
            The list may contain Feature Views, Feature Tables, or a subset of either. The
            strings should be in the format 'my_feature_view:my_feature'.
        tags (optional): A dictionary of key-value pairs used for organizing Feature
            Services.
    """

    name: str
    features: List[str]
    feature_tables: List[FeatureTable]
    feature_views: List[FeatureView]
    on_demand_feature_views: List[OnDemandFeatureView]
    tags: Dict[str, str]
    description: Optional[str] = None
    created_timestamp: Optional[datetime] = None
    last_updated_timestamp: Optional[datetime] = None

    defined_in: str

    @log_exceptions
    def __init__(
        self,
        name: str,
        features: List[Union[FeatureTable, FeatureView, OnDemandFeatureView]],
        tags: Optional[Dict[str, str]] = None,
        description: Optional[str] = None,
    ):
        """
        Creates a FeatureService object.

        Raises:
            ValueError: If one of the specified features is not a valid type.
        """
        self.name = name
        self.features = []
        self.feature_tables, self.feature_views, self.on_demand_feature_views = (
            [],
            [],
            [],
        )

        for feature_grouping in features:
            if isinstance(feature_grouping, FeatureTable):
                self.feature_tables.append(feature_grouping)
            elif isinstance(feature_grouping, FeatureView):
                self.feature_views.append(feature_grouping)
            elif isinstance(feature_grouping, OnDemandFeatureView):
                self.on_demand_feature_views.append(feature_grouping)
            else:
                raise ValueError(f"Unexpected type: {type(feature_grouping)}")

            self.features.extend(
                [f"{feature_grouping.name}:{f.name}" for f in feature_grouping.features]
            )

        self.tags = tags or {}
        self.description = description
        self.created_timestamp = None
        self.last_updated_timestamp = None

        self.defined_in = get_calling_file_name(inspect.stack())

    def __repr__(self):
        items = (f"{k} = {v}" for k, v in self.__dict__.items())
        return f"<{self.__class__.__name__}({', '.join(items)})>"

    def __str__(self):
        return str(MessageToJson(self.to_proto()))

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        if not isinstance(other, FeatureService):
            raise TypeError(
                "Comparisons should only involve FeatureService class objects."
            )
        if self.tags != other.tags or self.name != other.name:
            return False

        if sorted(self.features) != sorted(other.features):
            return False

        return True

    @staticmethod
    def from_proto(feature_service_proto: FeatureServiceProto):
        """
        Converts a FeatureServiceProto to a FeatureService object.

        Args:
            feature_service_proto: A protobuf representation of a FeatureService.
        """
        fs = FeatureService(
            name=feature_service_proto.spec.name,
            features=[],
            tags=dict(feature_service_proto.spec.tags),
            description=(
                feature_service_proto.spec.description
                if feature_service_proto.spec.description != ""
                else None
            ),
        )

        fs.features = [feature for feature in feature_service_proto.spec.features]
        fs.feature_tables = [
            FeatureTable.from_proto(table)
            for table in feature_service_proto.spec.feature_tables
        ]
        fs.feature_views = [
            FeatureView.from_proto(view)
            for view in feature_service_proto.spec.feature_views
        ]
        fs.on_demand_feature_views = [
            OnDemandFeatureView.from_proto(view)
            for view in feature_service_proto.spec.on_demand_feature_views
        ]

        if feature_service_proto.meta.HasField("created_timestamp"):
            fs.created_timestamp = (
                feature_service_proto.meta.created_timestamp.ToDatetime()
            )
        if feature_service_proto.meta.HasField("last_updated_timestamp"):
            fs.last_updated_timestamp = (
                feature_service_proto.meta.last_updated_timestamp.ToDatetime()
            )

        return fs

    def to_proto(self) -> FeatureServiceProto:
        """
        Converts a FeatureService to its protobuf representation.

        Returns:
            A FeatureServiceProto protobuf.
        """
        meta = FeatureServiceMeta()
        if self.created_timestamp:
            meta.created_timestamp.FromDatetime(self.created_timestamp)

        spec = FeatureServiceSpec(
            name=self.name,
            features=self.features,
            feature_tables=[table.to_proto() for table in self.feature_tables],
            feature_views=[view.to_proto() for view in self.feature_views],
            on_demand_feature_views=[
                view.to_proto() for view in self.on_demand_feature_views
            ],
        )
        if self.tags:
            spec.tags.update(self.tags)
        if self.description:
            spec.description = self.description

        feature_service_proto = FeatureServiceProto(spec=spec, meta=meta)
        return feature_service_proto

    def validate(self):
        pass
