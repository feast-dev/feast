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

from typing import Dict, Optional

from feast.protos.feast.core.Feature_pb2 import FeatureSpecV2 as FeatureSpecProto
from feast.protos.feast.types.Value_pb2 import ValueType as ValueTypeProto
from feast.value_type import ValueType


class Feature:
    """
    A Feature represents a class of serveable feature.

    Args:
        name: Name of the feature.
        dtype: The type of the feature, such as string or float.
        labels (optional): User-defined metadata in dictionary form.
    """

    def __init__(
        self,
        name: str,
        dtype: ValueType,
        description: str = "",
        labels: Optional[Dict[str, str]] = None,
    ):
        """Creates a Feature object."""
        self._name = name
        if not isinstance(dtype, ValueType):
            raise ValueError("dtype is not a valid ValueType")
        if dtype is ValueType.UNKNOWN:
            raise ValueError(f"dtype cannot be {dtype}")
        self._dtype = dtype
        self._description = description
        if labels is None:
            self._labels = dict()
        else:
            self._labels = labels

    def __eq__(self, other):
        if self.name != other.name or self.dtype != other.dtype:
            return False
        return True

    def __lt__(self, other):
        return self.name < other.name

    def __repr__(self):
        return (
            f"Feature(\n"
            f"    name={self._name!r},\n"
            f"    dtype={self._dtype!r},\n"
            f"    description={self._description!r},\n"
            f"    labels={self._labels!r}\n"
            f")"
        )

    def __str__(self):
        # readable string of the reference
        return f"Feature<{self.name}: {self.dtype}>"

    @property
    def name(self):
        """
        Gets the name of this feature.
        """
        return self._name

    @property
    def dtype(self) -> ValueType:
        """
        Gets the data type of this feature.
        """
        return self._dtype

    @property
    def description(self) -> str:
        """
        Gets the description of the feature
        """
        return self._description

    @property
    def labels(self) -> Dict[str, str]:
        """
        Gets the labels of this feature.
        """
        return self._labels

    def to_proto(self) -> FeatureSpecProto:
        """
        Converts Feature object to its Protocol Buffer representation.

        Returns:
            A FeatureSpecProto protobuf.
        """
        value_type = ValueTypeProto.Enum.Value(self.dtype.name)

        return FeatureSpecProto(
            name=self.name,
            value_type=value_type,
            description=self.description,
            tags=self.labels,
        )

    @classmethod
    def from_proto(cls, feature_proto: FeatureSpecProto):
        """
        Args:
            feature_proto: FeatureSpecV2 protobuf object

        Returns:
            Feature object
        """
        feature = cls(
            name=feature_proto.name,
            dtype=ValueType(feature_proto.value_type),
            description=feature_proto.description,
            labels=dict(feature_proto.tags),
        )

        return feature
