# Copyright 2019 The Feast Authors
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

from feast.core.FeatureSet_pb2 import FeatureSpec as FeatureProto
from feast.field import Field
from feast.serving.ServingService_pb2 import FeatureReference as FeatureRefProto
from feast.types import Value_pb2 as ValueTypeProto
from feast.value_type import ValueType


class Feature(Field):
    """Feature field type"""

    def to_proto(self) -> FeatureProto:
        """Converts Feature object to its Protocol Buffer representation"""
        value_type = ValueTypeProto.ValueType.Enum.Value(self.dtype.name)
        return FeatureProto(
            name=self.name,
            value_type=value_type,
            presence=self.presence,
            group_presence=self.group_presence,
            shape=self.shape,
            value_count=self.value_count,
            domain=self.domain,
            int_domain=self.int_domain,
            float_domain=self.float_domain,
            string_domain=self.string_domain,
            bool_domain=self.bool_domain,
            struct_domain=self.struct_domain,
            natural_language_domain=self.natural_language_domain,
            image_domain=self.image_domain,
            mid_domain=self.mid_domain,
            url_domain=self.url_domain,
            time_domain=self.time_domain,
            time_of_day_domain=self.time_of_day_domain,
        )

    @classmethod
    def from_proto(cls, feature_proto: FeatureProto):
        """

        Args:
            feature_proto: FeatureSpec protobuf object

        Returns:
            Feature object
        """
        feature = cls(
            name=feature_proto.name, dtype=ValueType(feature_proto.value_type),
        )
        feature.update_presence_constraints(feature_proto)
        feature.update_shape_type(feature_proto)
        feature.update_domain_info(feature_proto)
        return feature


class FeatureRef:
    """ Feature Reference represents a reference to a specific feature.  """

    def __init__(self, name: str, feature_set: str = None):
        self.proto = FeatureRefProto(name=name, feature_set=feature_set)

    @classmethod
    def from_proto(cls, proto: FeatureRefProto):
        """
        Construct a feature reference from the given FeatureReference proto

        Arg:
            proto: Protobuf FeatureReference to construct from

        Returns:
            FeatureRef that refers to the given feature
        """
        return cls(name=proto.name, feature_set=proto.feature_set)

    @classmethod
    def from_str(cls, feature_ref_str: str, ignore_project: bool = False):
        """
        Parse the given string feature reference into FeatureRef model
        String feature reference should be in the format feature_set:feature.
        Where "feature_set" and "name" are the feature_set name and feature name
        respectively.

        Args:
            feature_ref_str: String representation of the feature reference
            ignore_project: Ignore projects in given string feature reference
                            instead throwing an error

        Returns:
            FeatureRef that refers to the given feature
        """
        proto = FeatureRefProto()
        if "/" in feature_ref_str:
            if ignore_project:
                _, feature_ref_str = feature_ref_str.split("/")
            else:
                raise ValueError(f"Unsupported feature reference: {feature_ref_str}")

        # parse feature set name if specified
        if ":" in feature_ref_str:
            proto.feature_set, feature_ref_str = feature_ref_str.split(":")

        proto.name = feature_ref_str
        return cls.from_proto(proto)

    def to_proto(self) -> FeatureRefProto:
        """
        Convert and return this feature set reference to protobuf.

        Returns:
            Protobuf respresentation of this feature set reference.
        """
        return self.proto

    def __repr__(self):
        # return string representation of the reference
        # [project/][feature_set:]name
        # in protov3 unset string and int fields default to "" and 0
        ref_str = ""
        if len(self.proto.project) > 0:
            ref_str += self.proto.project + "/"
        if len(self.proto.feature_set) > 0:
            ref_str += self.proto.feature_set + ":"
        ref_str += self.proto.name
        return ref_str

    def __str__(self):
        # human readable string of the reference
        return f"FeatureRef<{self.__repr__()}>"

    def __eq__(self, other):
        # compare with other feature set
        return hash(self) == hash(other)

    def __hash__(self):
        # hash this reference
        return hash(repr(self))
