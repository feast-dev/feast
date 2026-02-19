from feast.infra.key_encoding_utils import (
    _deserialize_value,
    _serialize_val,
    deserialize_entity_key,
    reserialize_entity_v2_key_to_v3,
    serialize_entity_key,
    serialize_entity_key_prefix,
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
        entity_key_serialization_version=3,
    )
    # True int64, but should also be fine.
    serialize_entity_key(
        EntityKeyProto(
            join_keys=["user"], entity_values=[ValueProto(int64_val=int(2**31))]
        ),
        entity_key_serialization_version=3,
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

    # Default entity_key_serialization_version is 3, so result is of 8 bytes
    v, t = _serialize_val("int64_val", ValueProto(int64_val=1))
    assert t == ValueType.INT64
    assert v == b"\x01\x00\x00\x00\x00\x00\x00\x00"

    # Test unix_timestamp_val serialization
    v, t = _serialize_val(
        "unix_timestamp_val", ValueProto(unix_timestamp_val=1758823656)
    )
    assert t == ValueType.UNIX_TIMESTAMP
    # Verify roundtrip: deserialize the serialized value
    deserialized = _deserialize_value(ValueType.UNIX_TIMESTAMP, v)
    assert deserialized.unix_timestamp_val == 1758823656


def test_deserialize_value():
    v = _deserialize_value(ValueType.STRING, b"test")
    assert v.string_val == "test"

    v = _deserialize_value(ValueType.BYTES, b"test")
    assert v.bytes_val == b"test"

    v = _deserialize_value(ValueType.INT32, b"\x01\x00\x00\x00")
    assert v.int32_val == 1

    v = _deserialize_value(ValueType.INT64, b"\x01\x00\x00\x00\x00\x00\x00\x00")
    assert v.int64_val == 1

    timestamp_val = 1758823656
    serialized_bytes, _ = _serialize_val(
        "unix_timestamp_val", ValueProto(unix_timestamp_val=timestamp_val)
    )
    v = _deserialize_value(ValueType.UNIX_TIMESTAMP, serialized_bytes)
    assert v.unix_timestamp_val == timestamp_val


def test_serialize_deserialize_unix_timestamp_entity():
    entity_key_proto = EntityKeyProto(
        join_keys=["e2"],
        entity_values=[ValueProto(unix_timestamp_val=1758823656)],
    )

    serialized_key = serialize_entity_key(
        entity_key_proto,
        entity_key_serialization_version=3,
    )

    deserialized_key = deserialize_entity_key(
        serialized_key,
        entity_key_serialization_version=3,
    )

    assert deserialized_key == entity_key_proto
    assert deserialized_key.entity_values[0].unix_timestamp_val == 1758823656


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


def test_single_entity_fast_path():
    """Test that single entity optimization works correctly."""
    entity_key_proto = EntityKeyProto(
        join_keys=["user_id"],
        entity_values=[ValueProto(string_val="test_user")],
    )

    serialized_key = serialize_entity_key(
        entity_key_proto, entity_key_serialization_version=3
    )
    deserialized_key = deserialize_entity_key(
        serialized_key, entity_key_serialization_version=3
    )

    assert deserialized_key == entity_key_proto


def test_empty_entity_key():
    """Test handling of empty entity keys."""
    entity_key_proto = EntityKeyProto(join_keys=[], entity_values=[])

    serialized_key = serialize_entity_key(
        entity_key_proto, entity_key_serialization_version=3
    )
    deserialized_key = deserialize_entity_key(
        serialized_key, entity_key_serialization_version=3
    )

    assert deserialized_key == entity_key_proto


def test_binary_format_deterministic():
    """Test that serialization is deterministic (same input produces same output)."""
    entity_key_proto = EntityKeyProto(
        join_keys=["customer", "user", "session"],
        entity_values=[
            ValueProto(string_val="cust1"),
            ValueProto(string_val="user1"),
            ValueProto(string_val="sess1"),
        ],
    )

    # Serialize the same entity multiple times
    serializations = []
    for _ in range(5):
        serialized = serialize_entity_key(
            entity_key_proto, entity_key_serialization_version=3
        )
        serializations.append(serialized)

    # All serializations should be identical
    for s in serializations[1:]:
        assert s == serializations[0], "Serialization is not deterministic"


def test_optimization_preserves_sorting():
    """Test that optimizations preserve the sorting behavior for multi-entity keys."""
    # Create entity key with unsorted keys
    entity_key_proto = EntityKeyProto(
        join_keys=["zebra", "alpha", "beta"],
        entity_values=[
            ValueProto(string_val="z_val"),
            ValueProto(string_val="a_val"),
            ValueProto(string_val="b_val"),
        ],
    )

    serialized = serialize_entity_key(
        entity_key_proto, entity_key_serialization_version=3
    )
    deserialized = deserialize_entity_key(
        serialized, entity_key_serialization_version=3
    )

    # Keys should be sorted in the result
    expected_sorted_keys = ["alpha", "beta", "zebra"]
    expected_sorted_values = ["a_val", "b_val", "z_val"]

    assert deserialized.join_keys == expected_sorted_keys
    assert [v.string_val for v in deserialized.entity_values] == expected_sorted_values


def test_performance_bounds_single_entity():
    """Regression test to ensure single entity performance meets minimum bounds."""
    import time

    entity_key = EntityKeyProto(
        join_keys=["user_id"], entity_values=[ValueProto(string_val="user123")]
    )

    # Measure serialization time for 1000 operations
    start = time.perf_counter()
    for _ in range(1000):
        serialize_entity_key(entity_key, entity_key_serialization_version=3)
    serialize_time = time.perf_counter() - start

    # Measure deserialization time
    serialized = serialize_entity_key(entity_key, entity_key_serialization_version=3)
    start = time.perf_counter()
    for _ in range(1000):
        deserialize_entity_key(serialized, entity_key_serialization_version=3)
    deserialize_time = time.perf_counter() - start

    # Conservative performance bounds (should be much faster with optimizations)
    # 1000 operations should complete in < 20ms each for serialization and deserialization
    assert serialize_time < 0.02, f"Serialization too slow: {serialize_time:.4f}s"
    assert deserialize_time < 0.02, f"Deserialization too slow: {deserialize_time:.4f}s"


def test_non_ascii_prefix_compatibility():
    """Critical test: ensure prefix serialization matches full entity key serialization for non-ASCII keys."""
    # Test with non-ASCII characters that have different byte vs character lengths
    non_ascii_keys = ["用户ID", "사용자ID", "идентификатор", "مُعرِّف"]

    for key in non_ascii_keys:
        # Test single key prefix
        prefix_result = serialize_entity_key_prefix(
            [key], entity_key_serialization_version=3
        )

        # Create full entity key and serialize it
        entity_key = EntityKeyProto(
            join_keys=[key], entity_values=[ValueProto(string_val="test_value")]
        )
        full_result = serialize_entity_key(
            entity_key, entity_key_serialization_version=3
        )

        # The prefix should match the beginning of the full serialization
        # Extract just the key portion (skip entity count, but include key metadata)
        prefix_len = len(prefix_result)
        assert full_result[:prefix_len] == prefix_result, (
            f"Prefix mismatch for non-ASCII key '{key}': "
            f"Character length: {len(key)}, "
            f"UTF-8 byte length: {len(key.encode('utf8'))}"
        )


def test_ascii_prefix_compatibility():
    """Verify prefix compatibility still works for ASCII keys."""
    ascii_keys = ["user_id", "session_id", "device_id"]

    for key in ascii_keys:
        prefix_result = serialize_entity_key_prefix(
            [key], entity_key_serialization_version=3
        )

        entity_key = EntityKeyProto(
            join_keys=[key], entity_values=[ValueProto(string_val="test_value")]
        )
        full_result = serialize_entity_key(
            entity_key, entity_key_serialization_version=3
        )

        prefix_len = len(prefix_result)
        assert full_result[:prefix_len] == prefix_result, (
            f"Prefix mismatch for ASCII key '{key}'"
        )


def test_multi_key_non_ascii_prefix_compatibility():
    """Test multi-key prefix compatibility with non-ASCII characters."""
    mixed_keys = ["user_id", "用户会话", "session_id"]  # Mix ASCII and non-ASCII

    prefix_result = serialize_entity_key_prefix(
        mixed_keys, entity_key_serialization_version=3
    )

    entity_key = EntityKeyProto(
        join_keys=mixed_keys,
        entity_values=[
            ValueProto(string_val="test1"),
            ValueProto(string_val="test2"),
            ValueProto(string_val="test3"),
        ],
    )
    full_result = serialize_entity_key(entity_key, entity_key_serialization_version=3)

    prefix_len = len(prefix_result)
    assert full_result[:prefix_len] == prefix_result, (
        "Multi-key prefix mismatch with non-ASCII"
    )
