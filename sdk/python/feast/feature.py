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

from typing import MutableMapping, Optional

from feast.core.Feature_pb2 import FeatureSpecV2 as FeatureSpecProto
from feast.types import Value_pb2 as ValueTypeProto
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
