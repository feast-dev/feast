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

from feast.protos.feast.types.Value_pb2 import ValueType as ValueTypeProto

PRIMITIVE_FEAST_TYPES_TO_VALUE_TYPES = {
    "Invalid": "INVALID",
    "String": "STRING",
    "Bytes": "BYTES",
    "Bool": "BOOL",
    "Int32": "INT32",
    "Int64": "INT64",
    "Float32": "FLOAT",
    "Float64": "DOUBLE",
    "UnixTimestamp": "UNIX_TIMESTAMP",
}


class FeastType(ABC):
    """
    A FeastType represents a structured type that is recognized by Feast.
    """

    def __init__(self):
        """Creates a FeastType object."""
        pass

    @abstractmethod
    def to_proto(self) -> ValueTypeProto:
        """
        Converts a FeastType object to a protobuf representation.
        """
        raise NotImplementedError


class PrimitiveFeastType(FeastType):
    """
    A PrimitiveFeastType represents a primitive type in Feast.
    """

    def __init__(self):
        """Creates a PrimitiveFeastType object."""
        pass

    @classmethod
    def value_type(cls) -> str:
        """
        Returns the value type of the FeastType in string format.
        """
        return cls.__name__

    @classmethod
    def to_proto(cls) -> ValueTypeProto:
        """
        Converts a PrimitiveFeastType object to a protobuf representation.
        """
        value_type_name = PRIMITIVE_FEAST_TYPES_TO_VALUE_TYPES[cls.value_type()]
        return ValueTypeProto.Enum.Value(value_type_name)


class Invalid(PrimitiveFeastType):
    """
    An Invalid represents an invalid type.
    """


class String(PrimitiveFeastType):
    """
    A String represents a string type.
    """


class Bytes(PrimitiveFeastType):
    """
    A Bytes represents a bytes type.
    """


class Bool(PrimitiveFeastType):
    """
    A Bool represents a bool type.
    """


class Int32(PrimitiveFeastType):
    """
    An Int32 represents an int32 type.
    """


class Int64(PrimitiveFeastType):
    """
    An Int64 represents an int64 type.
    """


class Float32(PrimitiveFeastType):
    """
    A Float32 represents a float32 type.
    """


class Float64(PrimitiveFeastType):
    """
    A Float64 represents a float64 type.
    """


class UnixTimestamp(PrimitiveFeastType):
    """
    A UnixTimestamp represents a unix timestamp type.
    """


SUPPORTED_BASE_TYPES = [Invalid, String, Bytes, Bool, Int32, Int64, Float32, Float64]


class Array(FeastType):
    """
    An Array represents a list of types.

    Attributes:
        base_type: The base type of the array.
    """

    base_type: type

    def __init__(self, base_type: type):
        if base_type not in SUPPORTED_BASE_TYPES:
            raise ValueError(
                f"Type {type(base_type)} is currently not supported as a base type for Array."
            )

        self.base_type = base_type

    def to_proto(self) -> ValueTypeProto:
        assert issubclass(self.base_type, PrimitiveFeastType)
        value_type_name = PRIMITIVE_FEAST_TYPES_TO_VALUE_TYPES[
            self.base_type.value_type()
        ]
        value_type_list_name = value_type_name + "_LIST"
        return ValueTypeProto.Enum.Value(value_type_list_name)
