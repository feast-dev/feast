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

from typing import List, MutableMapping, Optional

from feast.protos.feast.core.Feature_pb2 import FeatureSpecV2 as FeatureSpecProto
from feast.protos.feast.serving.ServingService_pb2 import (
    FeatureReferenceV2 as FeatureRefProto,
)
from feast.protos.feast.types import Value_pb2 as ValueTypeProto
from feast.value_type import ValueType


class Feature:
    """Feature field type"""

    def __init__(
        self,
        name: str,
        dtype: ValueType,
        labels: Optional[MutableMapping[str, str]] = None,
    ):
        self._name = name
        if not isinstance(dtype, ValueType):
            raise ValueError("dtype is not a valid ValueType")
        self._dtype = dtype
        if labels is None:
            self._labels = dict()  # type: MutableMapping
        else:
            self._labels = labels

    def __eq__(self, other):
        if (
            self.name != other.name
            or self.dtype != other.dtype
            or self.labels != other.labels
        ):
            return False
        return True

    def __lt__(self, other):
        return self.name < other.name

    def __repr__(self):
        # return string representation of the reference
        return self.name

    def __str__(self):
        # readable string of the reference
        return f"Feature<{self.__repr__()}>"

    @property
    def name(self):
        """
        Getter for name of this field
        """
        return self._name

    @property
    def dtype(self) -> ValueType:
        """
        Getter for data type of this field
        """
        return self._dtype

    @property
    def labels(self) -> MutableMapping[str, str]:
        """
        Getter for labels of this field
        """
        return self._labels

    def to_proto(self) -> FeatureSpecProto:
        """Converts Feature object to its Protocol Buffer representation"""
        value_type = ValueTypeProto.ValueType.Enum.Value(self.dtype.name)

        return FeatureSpecProto(
            name=self.name, value_type=value_type, labels=self.labels,
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
            labels=feature_proto.labels,
        )

        return feature


class FeatureRef:
    """ Feature Reference represents a reference to a specific feature. """

    def __init__(self, name: str, feature_table: str):
        self.proto = FeatureRefProto(name=name, feature_table=feature_table)

    @classmethod
    def from_proto(cls, proto: FeatureRefProto):
        """
        Construct a feature reference from the given FeatureReference proto

        Args:
            proto: Protobuf FeatureReference to construct from
        Returns:
            FeatureRef that refers to the given feature
        """
        return cls(name=proto.name, feature_table=proto.feature_table)

    @classmethod
    def from_str(cls, feature_ref_str: str):
        """
        Parse the given string feature reference into FeatureRef model
        String feature reference should be in the format feature_table:feature.
        Where "feature_table" and "name" are the feature_table name and feature name
        respectively.

        Args:
            feature_ref_str: String representation of the feature reference
        Returns:
            FeatureRef that refers to the given feature
        """
        proto = FeatureRefProto()

        # parse feature table name if specified
        if ":" in feature_ref_str:
            proto.feature_table, proto.name = feature_ref_str.split(":")
        else:
            raise ValueError(
                f"Unsupported feature reference: {feature_ref_str} - Feature reference string should be in the form [featuretable_name:featurename]"
            )

        return cls.from_proto(proto)

    def to_proto(self) -> FeatureRefProto:
        """
        Convert and return this feature table reference to protobuf.

        Returns:
            Protobuf respresentation of this feature table reference.
        """

        return self.proto

    def __repr__(self):
        # return string representation of the reference
        ref_str = self.proto.feature_table + ":" + self.proto.name
        return ref_str

    def __str__(self):
        # readable string of the reference
        return f"FeatureRef<{self.__repr__()}>"


def _build_feature_references(feature_ref_strs: List[str]) -> List[FeatureRefProto]:
    """
    Builds a list of FeatureReference protos from a list of FeatureReference strings

    Args:
        feature_ref_strs: List of string feature references
    Returns:
        A list of FeatureReference protos parsed from args.
    """

    feature_refs = [FeatureRef.from_str(ref_str) for ref_str in feature_ref_strs]
    feature_ref_protos = [ref.to_proto() for ref in feature_refs]

    return feature_ref_protos
