import logging
from enum import Enum

from pymilvus import DataType

from feast.types import Array, Bytes, Float32, Float64, Int32, Int64, Invalid, String

logger = logging.getLogger(__name__)


class FeastType(Enum):
    """
    Mapping for converting Feast data type to a data type compatible wih Milvus.
    """

    INT32 = Int32
    INT64 = Int64
    FLOAT32 = Float32
    FLOAT64 = Float64
    STRING = String
    VARCHAR = String
    UNKNOWN = Invalid
    FLOAT_VECTOR = Array(Float32)
    BINARY_VECTOR = Array(Bytes)


class MilvusType(Enum):
    """
    Mapping for converting Feast data type to a data type compatible wih Milvus.
    """

    INT32 = DataType.INT32
    INT64 = DataType.INT64
    FLOAT32 = DataType.FLOAT
    FLOAT64 = DataType.DOUBLE
    STRING = DataType.STRING
    INVALID = DataType.UNKNOWN
    FLOAT_VECTOR = DataType.FLOAT_VECTOR
    BINARY_VECTOR = DataType.BINARY_VECTOR


class TypeConverter:
    @staticmethod
    def feast_to_milvus_data_type(feast_type: FeastType) -> DataType:
        mapped = FeastType(feast_type).name
        milvus_type = MilvusType._member_map_[f"{mapped}"].value
        return milvus_type

    @staticmethod
    def milvus_to_feast_type(milvus_type: DataType) -> FeastType:
        mapped = MilvusType(milvus_type.value).name
        feast_type = FeastType._member_map_[f"{mapped}"].value
        return feast_type
