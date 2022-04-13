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

from feast.value_type import ValueType

PRIMITIVE_FEAST_TYPES_TO_VALUE_TYPES = {
    "INVALID": "UNKNOWN",
    "BYTES": "BYTES",
    "STRING": "STRING",
    "INT32": "INT32",
    "INT64": "INT64",
    "FLOAT64": "DOUBLE",
    "FLOAT32": "FLOAT",
    "BOOL": "BOOL",
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
    def to_value_type(self) -> ValueType:
        """
        Converts a ComplexFeastType object to the corresponding ValueType enum.
        """
        raise NotImplementedError

    def __hash__(self):
        return hash(self.to_value_type().value)

    def __eq__(self, other):
        return self.to_value_type() == other.to_value_type()


class PrimitiveFeastType(Enum):
    """
    A PrimitiveFeastType represents a primitive type in Feast.

    Note that these values must match the values in /feast/protos/types/Value.proto.
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

    def to_value_type(self) -> ValueType:
        """
        Converts a PrimitiveFeastType object to the corresponding ValueType enum.
        """
        value_type_name = PRIMITIVE_FEAST_TYPES_TO_VALUE_TYPES[self.name]
        return ValueType[value_type_name]

    def __str__(self):
        return PRIMITIVE_FEAST_TYPES_TO_STRING[self.name]

    def __eq__(self, other):
        if isinstance(other, PrimitiveFeastType):
            return self.value == other.value
        else:
            return False

    def __hash__(self):
        return hash((PRIMITIVE_FEAST_TYPES_TO_STRING[self.name]))


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

PRIMITIVE_FEAST_TYPES_TO_STRING = {
    "INVALID": "Invalid",
    "STRING": "String",
    "BYTES": "Bytes",
    "BOOL": "Bool",
    "INT32": "Int32",
    "INT64": "Int64",
    "FLOAT32": "Float32",
    "FLOAT64": "Float64",
    "UNIX_TIMESTAMP": "UnixTimestamp",
}


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

    def to_value_type(self) -> ValueType:
        assert isinstance(self.base_type, PrimitiveFeastType)
        value_type_name = PRIMITIVE_FEAST_TYPES_TO_VALUE_TYPES[self.base_type.name]
        value_type_list_name = value_type_name + "_LIST"
        return ValueType[value_type_list_name]

    def __str__(self):
        return f"Array({self.base_type})"


FeastType = Union[ComplexFeastType, PrimitiveFeastType]


VALUE_TYPES_TO_FEAST_TYPES: Dict["ValueType", FeastType] = {
    ValueType.UNKNOWN: Invalid,
    ValueType.BYTES: Bytes,
    ValueType.STRING: String,
    ValueType.INT32: Int32,
    ValueType.INT64: Int64,
    ValueType.DOUBLE: Float64,
    ValueType.FLOAT: Float32,
    ValueType.BOOL: Bool,
    ValueType.UNIX_TIMESTAMP: UnixTimestamp,
    ValueType.BYTES_LIST: Array(Bytes),
    ValueType.STRING_LIST: Array(String),
    ValueType.INT32_LIST: Array(Int32),
    ValueType.INT64_LIST: Array(Int64),
    ValueType.DOUBLE_LIST: Array(Float64),
    ValueType.FLOAT_LIST: Array(Float32),
    ValueType.BOOL_LIST: Array(Bool),
    ValueType.UNIX_TIMESTAMP_LIST: Array(UnixTimestamp),
}


def from_value_type(value_type: ValueType,) -> FeastType:
    """
    Converts a ValueType enum to a Feast type.

    Args:
        value_type: The ValueType to be converted.

    Raises:
        ValueError: The conversion could not be performed.
    """
    if value_type in VALUE_TYPES_TO_FEAST_TYPES:
        return VALUE_TYPES_TO_FEAST_TYPES[value_type]

    raise ValueError(f"Could not convert value type {value_type} to FeastType.")
