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

import json
from typing import Dict, Optional

from typeguard import typechecked

from feast.feature import Feature
from feast.protos.feast.core.Feature_pb2 import FeatureSpecV2 as FieldProto
from feast.types import FeastType, Struct, from_value_type
from feast.value_type import ValueType

STRUCT_SCHEMA_TAG = "feast:struct_schema"


@typechecked
class Field:
    """
    A Field represents a set of values with the same structure.

    Attributes:
        name: The name of the field.
        dtype: The type of the field, such as string or float.
        description: A human-readable description.
        tags: User-defined metadata in dictionary form.
        vector_index: If set to True the field will be indexed for vector similarity search.
        vector_length: The length of the vector if the vector index is set to True.
        vector_search_metric: The metric used for vector similarity search.
    """

    name: str
    dtype: FeastType
    description: str
    tags: Dict[str, str]
    vector_index: bool
    vector_length: int
    vector_search_metric: Optional[str]

    def __init__(
        self,
        *,
        name: str,
        dtype: FeastType,
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
        vector_index: bool = False,
        vector_length: int = 0,
        vector_search_metric: Optional[str] = None,
    ):
        """
        Creates a Field object.

        Args:
            name: The name of the field.
            dtype: The type of the field, such as string or float.
            description (optional): A human-readable description.
            tags (optional): User-defined metadata in dictionary form.
            vector_index (optional): If set to True the field will be indexed for vector similarity search.
            vector_search_metric (optional): The metric used for vector similarity search.
        """
        self.name = name
        self.dtype = dtype
        self.description = description
        self.tags = tags or {}
        self.vector_index = vector_index
        self.vector_length = vector_length
        self.vector_search_metric = vector_search_metric

    def __eq__(self, other):
        if type(self) != type(other):
            return False

        if (
            self.name != other.name
            or self.dtype != other.dtype
            or self.description != other.description
            or self.tags != other.tags
            or self.vector_length != other.vector_length
            # or self.vector_index != other.vector_index
            # or self.vector_search_metric != other.vector_search_metric
        ):
            return False
        return True

    def __hash__(self):
        return hash((self.name, hash(self.dtype)))

    def __lt__(self, other):
        return self.name < other.name

    def __repr__(self):
        return (
            f"Field(\n"
            f"    name={self.name!r},\n"
            f"    dtype={self.dtype!r},\n"
            f"    description={self.description!r},\n"
            f"    tags={self.tags!r}\n"
            f"    vector_index={self.vector_index!r}\n"
            f"    vector_length={self.vector_length!r}\n"
            f"    vector_search_metric={self.vector_search_metric!r}\n"
            f")"
        )

    def __str__(self):
        return f"Field(name={self.name}, dtype={self.dtype}, tags={self.tags})"

    def to_proto(self) -> FieldProto:
        """Converts a Field object to its protobuf representation."""
        from feast.types import Array

        value_type = self.dtype.to_value_type()
        vector_search_metric = self.vector_search_metric or ""
        tags = dict(self.tags)
        # Persist Struct field schema in tags
        if isinstance(self.dtype, Struct):
            tags[STRUCT_SCHEMA_TAG] = _serialize_struct_schema(self.dtype)
        elif isinstance(self.dtype, Array) and isinstance(self.dtype.base_type, Struct):
            tags[STRUCT_SCHEMA_TAG] = _serialize_struct_schema(self.dtype.base_type)
        return FieldProto(
            name=self.name,
            value_type=value_type.value,
            description=self.description,
            tags=tags,
            vector_index=self.vector_index,
            vector_length=self.vector_length,
            vector_search_metric=vector_search_metric,
        )

    @classmethod
    def from_proto(cls, field_proto: FieldProto):
        """
        Creates a Field object from a protobuf representation.

        Args:
            field_proto: FieldProto protobuf object
        """
        value_type = ValueType(field_proto.value_type)
        tags = dict(field_proto.tags)
        vector_search_metric = getattr(field_proto, "vector_search_metric", "")
        vector_index = getattr(field_proto, "vector_index", False)
        vector_length = getattr(field_proto, "vector_length", 0)

        # Reconstruct Struct type from persisted schema in tags
        from feast.types import Array

        dtype: FeastType
        if value_type == ValueType.STRUCT and STRUCT_SCHEMA_TAG in tags:
            dtype = _deserialize_struct_schema(tags[STRUCT_SCHEMA_TAG])
            user_tags = {k: v for k, v in tags.items() if k != STRUCT_SCHEMA_TAG}
        elif value_type == ValueType.STRUCT_LIST and STRUCT_SCHEMA_TAG in tags:
            inner_struct = _deserialize_struct_schema(tags[STRUCT_SCHEMA_TAG])
            dtype = Array(inner_struct)
            user_tags = {k: v for k, v in tags.items() if k != STRUCT_SCHEMA_TAG}
        else:
            dtype = from_value_type(value_type=value_type)
            user_tags = tags

        return cls(
            name=field_proto.name,
            dtype=dtype,
            tags=user_tags,
            description=field_proto.description,
            vector_index=vector_index,
            vector_length=vector_length,
            vector_search_metric=vector_search_metric,
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


def _feast_type_to_str(feast_type: FeastType) -> str:
    """Convert a FeastType to a string representation for serialization."""
    from feast.types import (
        Array,
        PrimitiveFeastType,
    )

    if isinstance(feast_type, PrimitiveFeastType):
        return feast_type.name
    elif isinstance(feast_type, Struct):
        nested = {
            name: _feast_type_to_str(ft) for name, ft in feast_type.fields.items()
        }
        return json.dumps({"__struct__": nested})
    elif isinstance(feast_type, Array):
        return f"Array({_feast_type_to_str(feast_type.base_type)})"
    else:
        return str(feast_type)


def _str_to_feast_type(type_str: str) -> FeastType:
    """Convert a string representation back to a FeastType."""
    from feast.types import (
        Array,
        PrimitiveFeastType,
    )

    # Check if it's an Array type
    if type_str.startswith("Array(") and type_str.endswith(")"):
        inner = type_str[6:-1]
        base_type = _str_to_feast_type(inner)
        return Array(base_type)

    # Check if it's a nested Struct (JSON encoded)
    if type_str.startswith("{"):
        try:
            parsed = json.loads(type_str)
            if "__struct__" in parsed:
                fields = {
                    name: _str_to_feast_type(ft_str)
                    for name, ft_str in parsed["__struct__"].items()
                }
                return Struct(fields)
        except (json.JSONDecodeError, TypeError):
            pass

    # Must be a PrimitiveFeastType name
    try:
        return PrimitiveFeastType[type_str]
    except KeyError:
        from feast.types import String

        return String


def _serialize_struct_schema(struct_type: Struct) -> str:
    """Serialize a Struct's field schema to a JSON string for tag storage."""
    schema_dict = {}
    for name, feast_type in struct_type.fields.items():
        schema_dict[name] = _feast_type_to_str(feast_type)
    return json.dumps(schema_dict)


def _deserialize_struct_schema(schema_str: str) -> Struct:
    """Deserialize a JSON string from tags back to a Struct type."""
    schema_dict = json.loads(schema_str)
    fields = {}
    for name, type_str in schema_dict.items():
        fields[name] = _str_to_feast_type(type_str)
    return Struct(fields)
