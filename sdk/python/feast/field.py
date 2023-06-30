# Copyright 2022 The Feast Authors
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

from json import dumps
from typing import Dict, Optional

from pydantic import BaseModel, validator
from typeguard import typechecked

from feast.feature import Feature
from feast.protos.feast.core.Feature_pb2 import FeatureSpecV2 as FieldProto
from feast.types import FeastType, from_value_type, ComplexFeastType, PrimitiveFeastType
from feast.value_type import ValueType


@typechecked
class Field(BaseModel):
    """
    A Field represents a set of values with the same structure.

    Attributes:
        name: The name of the field.
        dtype: The type of the field, such as string or float.
        description: A human-readable description.
        tags: User-defined metadata in dictionary form.
    """

    name: str
    dtype: FeastType
    description: Optional[str] = ""
    tags: Optional[Dict[str, str]] = {}

    class Config:
        arbitrary_types_allowed = True
        extra = "allow"
        json_encoders = {
            FeastType: lambda v: int(dumps(v.to_value_type().value, default=str)),
            ComplexFeastType: lambda v: int(dumps(v.to_value_type().value, default=str)),
            PrimitiveFeastType: lambda v: int(dumps(v.to_value_type().value, default=str))
        }

    @validator('dtype', pre=True, always=True)
    def dtype_is_feasttype(cls, v):
        if not isinstance(v, FeastType):
            raise TypeError("dtype must be of type FeastType")
        return v

    def __eq__(self, other):
        if type(self) != type(other):
            return False

        if (
            self.name != other.name
            or self.dtype != other.dtype
            or self.description != other.description
            or self.tags != other.tags
        ):
            return False
        return True

    def __hash__(self):
        return hash((self.name, hash(self.dtype)))

    def __lt__(self, other):
        return self.name < other.name

    def __repr__(self):
        return f"{self.name}-{self.dtype}"

    def __str__(self):
        return f"Field(name={self.name}, dtype={self.dtype}, tags={self.tags})"

    def to_proto(self) -> FieldProto:
        """Converts a Field object to its protobuf representation."""
        value_type = self.dtype.to_value_type()
        return FieldProto(
            name=self.name,
            value_type=value_type.value,
            description=self.description,
            tags=self.tags,
        )

    @classmethod
    def from_proto(cls, field_proto: FieldProto):
        """
        Creates a Field object from a protobuf representation.

        Args:
            field_proto: FieldProto protobuf object
        """
        value_type = ValueType(field_proto.value_type)
        return cls(
            name=field_proto.name,
            dtype=from_value_type(value_type=value_type),
            tags=dict(field_proto.tags),
            description=field_proto.description,
        )

    @classmethod
    def from_feature(cls, feature: Feature):
        """
        Creates a Field object from a Feature object.

        Args:
            feature: Feature object to convert.
        """
        return cls(
            name=feature.name,
            dtype=from_value_type(feature.dtype),
            description=feature.description,
            tags=feature.labels,
        )
