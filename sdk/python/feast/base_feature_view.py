# Copyright 2021 The Feast Authors
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
from datetime import datetime
from typing import Dict, List, Optional, Type, Union

from google.protobuf.json_format import MessageToJson
from google.protobuf.message import Message

from feast.data_source import DataSource
from feast.feature_view_projection import FeatureViewProjection
from feast.field import Field
from feast.protos.feast.core.FeatureView_pb2 import FeatureView as FeatureViewProto
from feast.protos.feast.core.OnDemandFeatureView_pb2 import (
    OnDemandFeatureView as OnDemandFeatureViewProto,
)
from feast.protos.feast.core.StreamFeatureView_pb2 import (
    StreamFeatureView as StreamFeatureViewProto,
)


class BaseFeatureView(ABC):
    """
    A BaseFeatureView defines a logical group of features.

    Attributes:
        name: The unique name of the base feature view.
        features: The list of features defined as part of this base feature view.
        description: A human-readable description.
        tags: A dictionary of key-value pairs to store arbitrary metadata.
        owner: The owner of the base feature view, typically the email of the primary
            maintainer.
        projection: The feature view projection storing modifications to be applied to
            this base feature view at retrieval time.
        created_timestamp: The time when the base feature view was created.
        last_updated_timestamp: The time when the base feature view was last
            updated.
    """

    name: str
    features: List[Field]
    description: str
    tags: Dict[str, str]
    owner: str
    projection: FeatureViewProjection
    created_timestamp: Optional[datetime]
    last_updated_timestamp: Optional[datetime]

    @abstractmethod
    def __init__(
        self,
        *,
        name: str,
        features: Optional[List[Field]] = None,
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
        owner: str = "",
        source: Optional[DataSource] = None,
    ):
        """
        Creates a BaseFeatureView object.

        Args:
            name: The unique name of the base feature view.
            features (optional): The list of features defined as part of this base feature view.
            description (optional): A human-readable description.
            tags (optional): A dictionary of key-value pairs to store arbitrary metadata.
            owner (optional): The owner of the base feature view, typically the email of the
                primary maintainer.
            source (optional): The source of data for this group of features. May be a stream source, or a batch source.
                If a stream source, the source should contain a batch_source for backfills & batch materialization.
        Raises:
            ValueError: A field mapping conflicts with an Entity or a Feature.
        """
        assert name is not None
        self.name = name
        self.features = features or []
        self.description = description
        self.tags = tags or {}
        self.owner = owner
        self.projection = FeatureViewProjection.from_definition(self)
        self.created_timestamp = None
        self.last_updated_timestamp = None

        if source:
            self.source = source

    @property
    @abstractmethod
    def proto_class(self) -> Type[Message]:
        pass

    @abstractmethod
    def to_proto(
        self,
    ) -> Union[FeatureViewProto, OnDemandFeatureViewProto, StreamFeatureViewProto]:
        pass

    @classmethod
    @abstractmethod
    def from_proto(cls, feature_view_proto):
        pass

    @abstractmethod
    def __copy__(self):
        """Returns a deep copy of this base feature view."""
        pass

    def __repr__(self):
        items = (f"{k} = {v}" for k, v in self.__dict__.items())
        return f"<{self.__class__.__name__}({', '.join(items)})>"

    def __str__(self):
        return str(MessageToJson(self.to_proto()))

    def __hash__(self):
        return hash(self.name)

    def __getitem__(self, item):
        assert isinstance(item, list)

        cp = self.__copy__()
        if self.features:
            feature_name_to_feature = {
                feature.name: feature for feature in self.features
            }
            referenced_features = []
            for feature in item:
                if feature not in feature_name_to_feature:
                    raise ValueError(
                        f"Feature {feature} does not exist in this feature view."
                    )
                referenced_features.append(feature_name_to_feature[feature])
            cp.projection.features = referenced_features
        else:
            cp.projection.desired_features = item

        return cp

    def __eq__(self, other):
        if not isinstance(other, BaseFeatureView):
            raise TypeError(
                "Comparisons should only involve BaseFeatureView class objects."
            )

        if (
            self.name != other.name
            or sorted(self.features) != sorted(other.features)
            or self.projection != other.projection
            or self.description != other.description
            or self.tags != other.tags
            or self.owner != other.owner
        ):
            # This is meant to ignore the File Source change to Push Source
            if isinstance(type(self.source), type(other.source)):
                if self.source != other.source:
                    return False
            return False

        return True

    def ensure_valid(self):
        """
        Validates the state of this feature view locally.

        Raises:
            ValueError: The feature view is invalid.
        """
        if not self.name:
            raise ValueError("Feature view needs a name.")

    def with_name(self, name: str):
        """
        Returns a renamed copy of this base feature view. This renamed copy should only be
        used for query operations and will not modify the underlying base feature view.

        Args:
            name: The name to assign to the copy.
        """
        cp = self.__copy__()
        cp.projection.name_alias = name

        return cp

    def set_projection(self, feature_view_projection: FeatureViewProjection) -> None:
        """
        Sets the feature view projection of this base feature view to the given projection.

        Args:
            feature_view_projection: The feature view projection to be set.

        Raises:
            ValueError: The name or features of the projection do not match.
        """
        if feature_view_projection.name != self.name:
            raise ValueError(
                f"The projection for the {self.name} FeatureView cannot be applied because it differs in name. "
                f"The projection is named {feature_view_projection.name} and the name indicates which "
                "FeatureView the projection is for."
            )

        for feature in feature_view_projection.features:
            if feature not in self.features:
                raise ValueError(
                    f"The projection for {self.name} cannot be applied because it contains {feature.name} which the "
                    "FeatureView doesn't have."
                )

        self.projection = feature_view_projection

    def with_projection(self, feature_view_projection: FeatureViewProjection):
        """
        Returns a copy of this base feature view with the feature view projection set to
        the given projection.

        Args:
            feature_view_projection: The feature view projection to assign to the copy.

        Raises:
            ValueError: The name or features of the projection do not match.
        """
        if feature_view_projection.name != self.name:
            raise ValueError(
                f"The projection for the {self.name} FeatureView cannot be applied because it differs in name. "
                f"The projection is named {feature_view_projection.name} and the name indicates which "
                "FeatureView the projection is for."
            )

        for feature in feature_view_projection.features:
            if feature not in self.features:
                raise ValueError(
                    f"The projection for {self.name} cannot be applied because it contains {feature.name} which the "
                    "FeatureView doesn't have."
                )

        cp = self.__copy__()
        cp.projection = feature_view_projection

        return cp
