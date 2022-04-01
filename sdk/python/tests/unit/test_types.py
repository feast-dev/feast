from feast.protos.feast.types.Value_pb2 import ValueType as ValueTypeProto
from feast.types import Array, Float32, String


def test_primitive_feast_type():
    assert String.value_type() == "String"
    assert String.to_proto() == ValueTypeProto.Enum.Value("STRING")
    assert Float32.value_type() == "Float32"
    assert Float32.to_proto() == ValueTypeProto.Enum.Value("FLOAT")


def test_array_feast_type():
    array_float_32 = Array(Float32)
    assert array_float_32.to_proto() == ValueTypeProto.Enum.Value("FLOAT_LIST")

    array_string = Array(String)
    assert array_string.to_proto() == ValueTypeProto.Enum.Value("STRING_LIST")
