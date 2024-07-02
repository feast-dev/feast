import struct
from typing import List, Tuple

from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.protos.feast.types.Value_pb2 import ValueType


def _serialize_val(
    value_type, v: ValueProto, entity_key_serialization_version=1
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
    else:
        raise ValueError(f"Unsupported value type: {value_type}")


def serialize_entity_key_prefix(entity_keys: List[str]) -> bytes:
    """
    Serialize keys to a bytestring, so it can be used to prefix-scan through items stored in the online store
    using serialize_entity_key.

    This encoding is a partial implementation of serialize_entity_key, only operating on the keys of entities,
    and not the values.
    """
    sorted_keys = sorted(entity_keys)
    output: List[bytes] = []
    for k in sorted_keys:
        output.append(struct.pack("<I", ValueType.STRING))
        output.append(k.encode("utf8"))
    return b"".join(output)


def serialize_entity_key(
    entity_key: EntityKeyProto, entity_key_serialization_version=1
) -> bytes:
    """
    Serialize entity key to a bytestring so it can be used as a lookup key in a hash table.

    We need this encoding to be stable; therefore we cannot just use protobuf serialization
    here since it does not guarantee that two proto messages containing the same data will
    serialize to the same byte string[1].

    [1] https://developers.google.com/protocol-buffers/docs/encoding

    Args:
        entity_key_serialization_version: version of the entity key serialization
        version 1: int64 values are serialized as 4 bytes
        version 2: int64 values are serialized as 8 bytes
        version 3: entity_key size is added to the serialization for deserialization purposes
        entity_key: EntityKeyProto

    Returns: bytes of the serialized entity key
    """
    sorted_keys, sorted_values = zip(
        *sorted(zip(entity_key.join_keys, entity_key.entity_values))
    )

    output: List[bytes] = []
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
    if entity_key_serialization_version <= 2:
        raise ValueError(
            "Deserialization of entity key with version <= 2 is not supported. Please use version > 2 by setting entity_key_serialization_version=3"
        )
    offset = 0
    keys = []
    values = []
    while offset < len(serialized_entity_key):
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
