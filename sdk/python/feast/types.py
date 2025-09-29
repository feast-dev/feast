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
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Union

import pyarrow

from feast.value_type import ValueType

PRIMITIVE_FEAST_TYPES_TO_VALUE_TYPES = {
    "INVALID": "UNKNOWN",
    "BYTES": "BYTES",
    "PDF_BYTES": "PDF_BYTES",
    "IMAGE_BYTES": "IMAGE_BYTES",
    "STRING": "STRING",
    "INT32": "INT32",
    "INT64": "INT64",
    "FLOAT64": "DOUBLE",
    "FLOAT32": "FLOAT",
    "BOOL": "BOOL",
    "UNIX_TIMESTAMP": "UNIX_TIMESTAMP",
}


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


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
        if isinstance(other, ComplexFeastType):
            return self.to_value_type() == other.to_value_type()
        else:
            return False


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
    PDF_BYTES = 9
    IMAGE_BYTES = 10

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
PdfBytes = PrimitiveFeastType.PDF_BYTES
ImageBytes = PrimitiveFeastType.IMAGE_BYTES
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
    PdfBytes,
    ImageBytes,
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
    "PDF_BYTES": "PdfBytes",
    "IMAGE_BYTES": "ImageBytes",
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
    ValueType.PDF_BYTES: PdfBytes,
    ValueType.IMAGE_BYTES: ImageBytes,
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

FEAST_TYPES_TO_PYARROW_TYPES = {
    String: pyarrow.string(),
    Bool: pyarrow.bool_(),
    Int32: pyarrow.int32(),
    Int64: pyarrow.int64(),
    Float32: pyarrow.float32(),
    Float64: pyarrow.float64(),
    # Note: datetime only supports microseconds https://github.com/python/cpython/blob/3.8/Lib/datetime.py#L1559
    UnixTimestamp: pyarrow.timestamp("us", tz=_utc_now().tzname()),
}

FEAST_VECTOR_TYPES: List[Union[ValueType, PrimitiveFeastType, ComplexFeastType]] = [
    ValueType.BYTES_LIST,
    ValueType.INT32_LIST,
    ValueType.INT64_LIST,
    ValueType.FLOAT_LIST,
    ValueType.BOOL_LIST,
]
for k in VALUE_TYPES_TO_FEAST_TYPES:
    if k in FEAST_VECTOR_TYPES:
        FEAST_VECTOR_TYPES.append(VALUE_TYPES_TO_FEAST_TYPES[k])


def from_feast_to_pyarrow_type(feast_type: FeastType) -> pyarrow.DataType:
    """
    Converts a Feast type to a PyArrow type.

    Args:
        feast_type: The Feast type to be converted.

    Raises:
        ValueError: The conversion could not be performed.
    """
    assert isinstance(feast_type, (ComplexFeastType, PrimitiveFeastType)), (
        f"Expected FeastType, got {type(feast_type)}"
    )
    if isinstance(feast_type, PrimitiveFeastType):
        if feast_type in FEAST_TYPES_TO_PYARROW_TYPES:
            return FEAST_TYPES_TO_PYARROW_TYPES[feast_type]
    elif isinstance(feast_type, ComplexFeastType):
        # Handle the case when feast_type is an instance of ComplexFeastType
        pass

    raise ValueError(f"Could not convert Feast type {feast_type} to PyArrow type.")


def from_value_type(
    value_type: ValueType,
) -> FeastType:
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


def from_feast_type(
    feast_type: FeastType,
) -> ValueType:
    """
    Converts a Feast type to a ValueType enum.

    Args:
        feast_type: The Feast type to be converted.

    Returns:
        The corresponding ValueType enum.

    Raises:
        ValueError: The conversion could not be performed.
    """
    if feast_type in VALUE_TYPES_TO_FEAST_TYPES.values():
        return list(VALUE_TYPES_TO_FEAST_TYPES.keys())[
            list(VALUE_TYPES_TO_FEAST_TYPES.values()).index(feast_type)
        ]

    raise ValueError(f"Could not convert feast type {feast_type} to ValueType.")
