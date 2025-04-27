import pytest

from feast.infra.key_encoding_utils import (
    _deserialize_value,
    _serialize_val,
    deserialize_entity_key,
    reserialize_entity_v2_key_to_v3,
    serialize_entity_key,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.protos.feast.types.Value_pb2 import ValueType


def test_serialize_entity_key():
    # Should be fine
    serialize_entity_key(
        EntityKeyProto(
            join_keys=["user"], entity_values=[ValueProto(int64_val=int(2**15))]
        ),
        entity_key_serialization_version=2,
    )
    # True int64, but should also be fine.
    serialize_entity_key(
        EntityKeyProto(
            join_keys=["user"], entity_values=[ValueProto(int64_val=int(2**31))]
        ),
        entity_key_serialization_version=2,
    )

    # Old serialization scheme, should fail.
    with pytest.raises(BaseException):
        serialize_entity_key(
            EntityKeyProto(
                join_keys=["user"], entity_values=[ValueProto(int64_val=int(2**31))]
            ),
        )


def test_deserialize_entity_key():
    serialized_entity_key = serialize_entity_key(
        EntityKeyProto(
            join_keys=["user"], entity_values=[ValueProto(int64_val=int(2**15))]
        ),
        entity_key_serialization_version=3,
    )

    deserialized_entity_key = deserialize_entity_key(
        serialized_entity_key, entity_key_serialization_version=3
    )
    assert deserialized_entity_key == EntityKeyProto(
        join_keys=["user"], entity_values=[ValueProto(int64_val=int(2**15))]
    )


def test_deserialize_multiple_entity_keys():
    entity_key_proto = EntityKeyProto(
        join_keys=["customer", "user"],
        entity_values=[ValueProto(string_val="test"), ValueProto(int64_val=int(2**15))],
    )

    serialized_entity_key = serialize_entity_key(
        entity_key_proto,
        entity_key_serialization_version=3,
    )

    deserialized_entity_key = deserialize_entity_key(
        serialized_entity_key,
        entity_key_serialization_version=3,
    )
    assert deserialized_entity_key == entity_key_proto


def test_serialize_value():
    v, t = _serialize_val("string_val", ValueProto(string_val="test"))
    assert t == ValueType.STRING
    assert v == b"test"

    v, t = _serialize_val("bytes_val", ValueProto(bytes_val=b"test"))
    assert t == ValueType.BYTES
    assert v == b"test"

    v, t = _serialize_val("int32_val", ValueProto(int32_val=1))
    assert t == ValueType.INT32
    assert v == b"\x01\x00\x00\x00"

    # default entity_key_serialization_version is 1, so the result should be 4 bytes
    v, t = _serialize_val("int64_val", ValueProto(int64_val=1))
    assert t == ValueType.INT64
    assert v == b"\x01\x00\x00\x00"

    # current entity_key_serialization_version is 2, so the result should be 8 bytes
    v, t = _serialize_val(
        "int64_val", ValueProto(int64_val=1), entity_key_serialization_version=2
    )
    assert t == ValueType.INT64
    assert v == b"\x01\x00\x00\x00\x00\x00\x00\x00"

    # new entity_key_serialization_version is 3, the result should be same as version 2
    v, t = _serialize_val(
        "int64_val", ValueProto(int64_val=1), entity_key_serialization_version=3
    )
    assert t == ValueType.INT64
    assert v == b"\x01\x00\x00\x00\x00\x00\x00\x00"


def test_deserialize_value():
    v = _deserialize_value(ValueType.STRING, b"test")
    assert v.string_val == "test"

    v = _deserialize_value(ValueType.BYTES, b"test")
    assert v.bytes_val == b"test"

    v = _deserialize_value(ValueType.INT32, b"\x01\x00\x00\x00")
    assert v.int32_val == 1

    v = _deserialize_value(ValueType.INT64, b"\x01\x00\x00\x00\x00\x00\x00\x00")
    assert v.int64_val == 1


def test_reserialize_entity_v2_key_to_v3():
    entity_key_proto_v2 = EntityKeyProto(
        join_keys=["user"],
        entity_values=[ValueProto(int64_val=int(2**15))],
    )
    serialized_key_v2 = serialize_entity_key(
        entity_key_proto_v2,
        entity_key_serialization_version=2,
    )

    serialized_key_v3 = reserialize_entity_v2_key_to_v3(serialized_key_v2)

    deserialized_key_v3 = deserialize_entity_key(
        serialized_key_v3,
        entity_key_serialization_version=3,
    )

    assert deserialized_key_v3 == EntityKeyProto(
        join_keys=["user"],
        entity_values=[ValueProto(int64_val=int(2**15))],
    )
