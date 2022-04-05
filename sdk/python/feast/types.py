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
from abc import ABC, abstractmethod
from enum import Enum
from typing import Union

from feast.protos.feast.types.Value_pb2 import ValueType as ValueTypeProto

PRIMITIVE_FEAST_TYPES_TO_VALUE_TYPES = {
    "INVALID": "INVALID",
    "STRING": "STRING",
    "BYTES": "BYTES",
    "BOOL": "BOOL",
    "INT32": "INT32",
    "INT64": "INT64",
    "FLOAT32": "FLOAT",
    "FLOAT64": "DOUBLE",
    "UNIX_TIMESTAMP": "UNIX_TIMESTAMP",
}


class ComplexFeastType(ABC):
    """
    A ComplexFeastType represents a structured type that is recognized by Feast.
    """

    def __init__(self):
        """Creates a ComplexFeastType object."""
        pass

    @abstractmethod
    def to_value_type(self) -> ValueTypeProto.Enum:
        """
        Converts a ComplexFeastType object to the corresponding ValueTypeProto.Enum value.
        """
        raise NotImplementedError

    def __eq__(self, other):
        return self.to_value_type() == other.to_value_type()


class PrimitiveFeastType(Enum):
    """
    A PrimitiveFeastType represents a primitive type in Feast.

    Note that these values must match the values in ValueTypeProto.Enum.
    """

    INVALID = 0
    BYTES = 1
    STRING = 2
    INT32 = 3
    INT64 = 4
    FLOAT64 = 5
    FLOAT32 = 6
    BOOL = 7
    UNIX_TIMESTAMP = 8

    def to_value_type(self) -> ValueTypeProto.Enum:
        """
        Converts a PrimitiveFeastType object to the corresponding ValueTypeProto.Enum value.
        """
        value_type_name = PRIMITIVE_FEAST_TYPES_TO_VALUE_TYPES[self.name]
        return ValueTypeProto.Enum.Value(value_type_name)


Invalid = PrimitiveFeastType.INVALID
Bytes = PrimitiveFeastType.BYTES
String = PrimitiveFeastType.STRING
Bool = PrimitiveFeastType.BOOL
Int32 = PrimitiveFeastType.INT32
Int64 = PrimitiveFeastType.INT64
Float32 = PrimitiveFeastType.FLOAT32
Float64 = PrimitiveFeastType.FLOAT64
UnixTimestamp = PrimitiveFeastType.UNIX_TIMESTAMP


SUPPORTED_BASE_TYPES = [
    Invalid,
    String,
    Bytes,
    Bool,
    Int32,
    Int64,
    Float32,
    Float64,
    UnixTimestamp,
]


class Array(ComplexFeastType):
    """
    An Array represents a list of types.

    Attributes:
        base_type: The base type of the array.
    """

    base_type: Union[PrimitiveFeastType, ComplexFeastType]

    def __init__(self, base_type: Union[PrimitiveFeastType, ComplexFeastType]):
        if base_type not in SUPPORTED_BASE_TYPES:
            raise ValueError(
                f"Type {type(base_type)} is currently not supported as a base type for Array."
            )

        self.base_type = base_type

    def to_value_type(self) -> int:
        assert isinstance(self.base_type, PrimitiveFeastType)
        value_type_name = PRIMITIVE_FEAST_TYPES_TO_VALUE_TYPES[self.base_type.name]
        value_type_list_name = value_type_name + "_LIST"
        return ValueTypeProto.Enum.Value(value_type_list_name)


def from_value_type(
    value_type: ValueTypeProto.Enum,
) -> Union[ComplexFeastType, PrimitiveFeastType]:
    """
    Converts a ValueTypeProto.Enum to a Feast type.

    Args:
        value_type: The ValueTypeProto.Enum to be converted.

    Raises:
        ValueError: The conversion could not be performed.
    """
    # Primitive types can be directly converted.
    primitive_values = [primitive_type.value for primitive_type in PrimitiveFeastType]
    if value_type in primitive_values:
        return PrimitiveFeastType(value_type)

    # Complex types must be constructed. Currently only arrays are supported. Note that
    # enum values for arrays are precisely the enum value for the array's base type plus 10.
    # TODO(felixwang9817): Make this more robust.
    base_type = value_type - 10
    if base_type in primitive_values:
        return Array(PrimitiveFeastType(base_type))

    raise ValueError("Could not convert value type to FeastType")
