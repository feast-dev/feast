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
from typing import Dict, Union

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

    Note that these values must match the values in ValueTypeProto.Enum. See
    /feast/protos/types/Value.proto for the exact values.
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


VALUE_TYPES_TO_FEAST_TYPES: Dict[
    "ValueTypeProto.Enum", Union[ComplexFeastType, PrimitiveFeastType]
] = {
    ValueTypeProto.Enum.INVALID: Invalid,
    ValueTypeProto.Enum.BYTES: Bytes,
    ValueTypeProto.Enum.STRING: String,
    ValueTypeProto.Enum.INT32: Int32,
    ValueTypeProto.Enum.INT64: Int64,
    ValueTypeProto.Enum.DOUBLE: Float64,
    ValueTypeProto.Enum.FLOAT: Float32,
    ValueTypeProto.Enum.BOOL: Bool,
    ValueTypeProto.Enum.UNIX_TIMESTAMP: UnixTimestamp,
    ValueTypeProto.Enum.BYTES_LIST: Array(Bytes),
    ValueTypeProto.Enum.STRING_LIST: Array(String),
    ValueTypeProto.Enum.INT32_LIST: Array(Int32),
    ValueTypeProto.Enum.INT64_LIST: Array(Int64),
    ValueTypeProto.Enum.DOUBLE_LIST: Array(Float64),
    ValueTypeProto.Enum.FLOAT_LIST: Array(Float32),
    ValueTypeProto.Enum.BOOL_LIST: Array(Bool),
    ValueTypeProto.Enum.UNIX_TIMESTAMP_LIST: Array(UnixTimestamp),
}


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
    if value_type in VALUE_TYPES_TO_FEAST_TYPES:
        return VALUE_TYPES_TO_FEAST_TYPES[value_type]

    raise ValueError(f"Could not convert value type {value_type} to FeastType.")
