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
from typing import Dict, List, Optional, Type

from google.protobuf.json_format import MessageToJson
from proto import Message

from feast.feature import Feature
from feast.feature_view_projection import FeatureViewProjection


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
        projection: The feature view projection to be applied to this base feature view
            at retrieval time.
        created_timestamp (optional): The time when the base feature view was created.
        last_updated_timestamp (optional): The time when the base feature view was last
            updated.
    """

    name: str
    features: List[Feature]
    description: str
    tags: Dict[str, str]
    owner: str
    projection: FeatureViewProjection
    created_timestamp: Optional[datetime]
    last_updated_timestamp: Optional[datetime]

    @abstractmethod
    def __init__(
        self,
        name: str,
        features: List[Feature],
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
        owner: str = "",
    ):
        """
        Creates a BaseFeatureView object.

        Args:
            name: The unique name of the base feature view.
            features: The list of features defined as part of this base feature view.
            description (optional): A human-readable description.
            tags (optional): A dictionary of key-value pairs to store arbitrary metadata.
            owner (optional): The owner of the base feature view, typically the email of the
                primary maintainer.

        Raises:
            ValueError: A field mapping conflicts with an Entity or a Feature.
        """
        self.name = name
        self.features = features
        self.description = description
        self.tags = tags or {}
        self.owner = owner
        self.projection = FeatureViewProjection.from_definition(self)
        self.created_timestamp = None
        self.last_updated_timestamp = None

    @property
    @abstractmethod
    def proto_class(self) -> Type[Message]:
        pass

    @abstractmethod
    def to_proto(self) -> Message:
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
        return hash((id(self), self.name))

    def __getitem__(self, item):
        assert isinstance(item, list)

        referenced_features = []
        for feature in self.features:
            if feature.name in item:
                referenced_features.append(feature)

        cp = self.__copy__()
        cp.projection.features = referenced_features

        return cp

    def __eq__(self, other):
        if not isinstance(other, BaseFeatureView):
            raise TypeError(
                "Comparisons should only involve BaseFeatureView class objects."
            )

        if (
            self.name != other.name
            or sorted(self.features) != sorted(other.features)
            or self.description != other.description
            or self.tags != other.tags
            or self.owner != other.owner
        ):
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
        Renames this feature view by returning a copy of this feature view with an alias
        set for the feature view name. This rename operation is only used as part of query
        operations and will not modify the underlying FeatureView.

        Args:
            name: Name to assign to the FeatureView copy.

        Returns:
            A copy of this FeatureView with the name replaced with the 'name' input.
        """
        cp = self.__copy__()
        cp.projection.name_alias = name

        return cp

    def set_projection(self, feature_view_projection: FeatureViewProjection) -> None:
        """
        Setter for the projection object held by this FeatureView. A projection is an
        object that stores the modifications to a FeatureView that is applied to the FeatureView
        when the FeatureView is used such as during feature_store.get_historical_features.
        This method also performs checks to ensure the projection is consistent with this
        FeatureView before doing the set.

        Args:
            feature_view_projection: The FeatureViewProjection object to set this FeatureView's
                'projection' field to.
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
        Sets the feature view projection by returning a copy of this on-demand feature view
        with its projection set to the given projection. A projection is an
        object that stores the modifications to a feature view that is used during
        query operations.

        Args:
            feature_view_projection: The FeatureViewProjection object to link to this
                OnDemandFeatureView.

        Returns:
            A copy of this OnDemandFeatureView with its projection replaced with the
            'feature_view_projection' argument.
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
