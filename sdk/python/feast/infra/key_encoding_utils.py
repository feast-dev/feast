import struct
import warnings
from typing import List, Tuple, Union

from google.protobuf.internal.containers import RepeatedScalarFieldContainer

from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.protos.feast.types.Value_pb2 import ValueType


def _serialize_val(
    value_type, v: ValueProto, entity_key_serialization_version=3
) -> Tuple[bytes, int]:
    if value_type == "string_val":
        return v.string_val.encode("utf8"), ValueType.STRING
    elif value_type == "bytes_val":
        return v.bytes_val, ValueType.BYTES
    elif value_type == "int32_val":
        return struct.pack("<i", v.int32_val), ValueType.INT32
    elif value_type == "int64_val":
        if 0 <= entity_key_serialization_version <= 1:
            return struct.pack("<l", v.int64_val), ValueType.INT64
        return struct.pack("<q", v.int64_val), ValueType.INT64
    elif value_type == "unix_timestamp_val":
        return struct.pack("<q", v.unix_timestamp_val), ValueType.UNIX_TIMESTAMP
    else:
        raise ValueError(f"Value type not supported for feast feature store: {v}")


def _deserialize_value(value_type, value_bytes) -> ValueProto:
    if value_type == ValueType.INT64:
        value = struct.unpack("<q", value_bytes)[0]
        return ValueProto(int64_val=value)
    if value_type == ValueType.INT32:
        value = struct.unpack("<i", value_bytes)[0]
        return ValueProto(int32_val=value)
    elif value_type == ValueType.STRING:
        value = value_bytes.decode("utf-8")
        return ValueProto(string_val=value)
    elif value_type == ValueType.BYTES:
        return ValueProto(bytes_val=value_bytes)
    elif value_type == ValueType.UNIX_TIMESTAMP:
        value = struct.unpack("<q", value_bytes)[0]
        return ValueProto(unix_timestamp_val=value)
    else:
        raise ValueError(f"Unsupported value type: {value_type}")


def serialize_entity_key_prefix(
    entity_keys: List[str], entity_key_serialization_version: int = 3
) -> bytes:
    """
    Serialize keys to a bytestring, so it can be used to prefix-scan through items stored in the online store
    using serialize_entity_key.

    This encoding is a partial implementation of serialize_entity_key, only operating on the keys of entities,
    and not the values.
    """
    sorted_keys = sorted(entity_keys)
    output: List[bytes] = []
    if entity_key_serialization_version > 2:
        output.append(struct.pack("<I", len(sorted_keys)))
    for k in sorted_keys:
        output.append(struct.pack("<I", ValueType.STRING))
        if entity_key_serialization_version > 2:
            output.append(struct.pack("<I", len(k)))
        output.append(k.encode("utf8"))
    return b"".join(output)


def reserialize_entity_v2_key_to_v3(
    serialized_key_v2: bytes,
) -> bytes:
    """
    Deserialize version 2 entity key and reserialize it to version 3.

    Args:
        serialized_key_v2: serialized entity key of version 2

    Returns: bytes of the serialized entity key in version 3
    """
    offset = 0
    keys = []
    values = []
    num_keys = 1
    for _ in range(num_keys):
        value_type = struct.unpack_from("<I", serialized_key_v2, offset)[0]
        offset += 4
        print(f"Value Type: {value_type}")

        fixed_tail_size = 4 + 4 + 8
        string_end = len(serialized_key_v2) - fixed_tail_size

        key = serialized_key_v2[offset:string_end].decode("utf-8")
        keys.append(key)
        offset = string_end

    while offset < len(serialized_key_v2):
        (value_type,) = struct.unpack_from("<I", serialized_key_v2, offset)
        offset += 4

        (value_length,) = struct.unpack_from("<I", serialized_key_v2, offset)
        offset += 4

        # Read the value based on its type and length
        value_bytes = serialized_key_v2[offset : offset + value_length]
        value = _deserialize_value(value_type, value_bytes)
        values.append(value)
        offset += value_length

    return serialize_entity_key(
        EntityKeyProto(join_keys=keys, entity_values=values),
        entity_key_serialization_version=3,
    )


def serialize_entity_key(
    entity_key: EntityKeyProto, entity_key_serialization_version=3
) -> bytes:
    """
    Serialize entity key to a bytestring so it can be used as a lookup key in a hash table.

    We need this encoding to be stable; therefore we cannot just use protobuf serialization
    here since it does not guarantee that two proto messages containing the same data will
    serialize to the same byte string[1].

    [1] https://developers.google.com/protocol-buffers/docs/encoding

    Args:
        entity_key_serialization_version: version of the entity key serialization
        Versions:
        version 3: entity_key size is added to the serialization for deserialization purposes
        entity_key: EntityKeyProto

    Returns: bytes of the serialized entity key
    """
    if entity_key_serialization_version < 3:
        # Not raising the error, keeping it in warning state for reserialization purpose
        # We should remove this after few releases
        warnings.warn(
            "Serialization of entity key with version < 3 is removed. Please use version 3 by setting entity_key_serialization_version=3."
            "To reserializa your online store featrues refer -  https://github.com/feast-dev/feast/blob/master/docs/how-to-guides/entity-reserialization-of-from-v2-to-v3.md"
        )
    sorted_keys, sorted_values = zip(
        *sorted(zip(entity_key.join_keys, entity_key.entity_values))
    )

    output: List[bytes] = []
    if entity_key_serialization_version > 2:
        output.append(struct.pack("<I", len(sorted_keys)))
    for k in sorted_keys:
        output.append(struct.pack("<I", ValueType.STRING))
        if entity_key_serialization_version > 2:
            output.append(struct.pack("<I", len(k)))
        output.append(k.encode("utf8"))
    for v in sorted_values:
        val_bytes, value_type = _serialize_val(
            v.WhichOneof("val"),
            v,
            entity_key_serialization_version=entity_key_serialization_version,
        )

        output.append(struct.pack("<I", value_type))

        output.append(struct.pack("<I", len(val_bytes)))
        output.append(val_bytes)

    return b"".join(output)


def deserialize_entity_key(
    serialized_entity_key: bytes, entity_key_serialization_version=3
) -> EntityKeyProto:
    """
    Deserialize entity key from a bytestring. This function can only be used with entity_key_serialization_version > 2.
    Args:
        entity_key_serialization_version: version of the entity key serialization
        serialized_entity_key: serialized entity key bytes

    Returns: EntityKeyProto

    """
    if entity_key_serialization_version < 3:
        # Not raising the error, keeping it in warning state for reserialization purpose
        # We should remove this after few releases
        warnings.warn(
            "Deserialization of entity key with version < 3 is removed. Please use version 3 by setting entity_key_serialization_version=3."
            "To reserializa your online store featrues refer -  https://github.com/feast-dev/feast/blob/master/docs/how-to-guides/entity-reserialization-of-from-v2-to-v3.md"
        )
    offset = 0
    keys = []
    values = []

    num_keys = struct.unpack_from("<I", serialized_entity_key, offset)[0]
    offset += 4

    for _ in range(num_keys):
        key_type = struct.unpack_from("<I", serialized_entity_key, offset)[0]
        offset += 4

        # Read the length of the key
        key_length = struct.unpack_from("<I", serialized_entity_key, offset)[0]
        offset += 4

        if key_type == ValueType.STRING:
            key = struct.unpack_from(f"<{key_length}s", serialized_entity_key, offset)[
                0
            ]
            keys.append(key.decode("utf-8").rstrip("\x00"))
            offset += key_length
        else:
            raise ValueError(f"Unsupported key type: {key_type}")

    while offset < len(serialized_entity_key):
        (value_type,) = struct.unpack_from("<I", serialized_entity_key, offset)
        offset += 4

        (value_length,) = struct.unpack_from("<I", serialized_entity_key, offset)
        offset += 4

        # Read the value based on its type and length
        value_bytes = serialized_entity_key[offset : offset + value_length]
        value = _deserialize_value(value_type, value_bytes)
        values.append(value)
        offset += value_length

    return EntityKeyProto(join_keys=keys, entity_values=values)


def get_list_val_str(val):
    accept_value_types = [
        "float_list_val",
        "double_list_val",
        "int32_list_val",
        "int64_list_val",
    ]
    for accept_type in accept_value_types:
        if val.HasField(accept_type):
            return str(getattr(val, accept_type).val)
    return None


def serialize_f32(
    vector: Union[RepeatedScalarFieldContainer[float], List[float]], vector_length: int
) -> bytes:
    """serializes a list of floats into a compact "raw bytes" format"""
    return struct.pack(f"{vector_length}f", *vector)


def deserialize_f32(byte_vector: bytes, vector_length: int) -> List[float]:
    """deserializes a list of floats from a compact "raw bytes" format"""
    num_floats = vector_length // 4  # 4 bytes per float
    return list(struct.unpack(f"{num_floats}f", byte_vector))
