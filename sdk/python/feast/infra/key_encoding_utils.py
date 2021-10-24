import struct
from typing import List, Tuple

from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.protos.feast.types.Value_pb2 import ValueType


def _serialize_val(value_type, v: ValueProto) -> Tuple[bytes, int]:
    if value_type == "string_val":
        return v.string_val.encode("utf8"), ValueType.STRING
    elif value_type == "bytes_val":
        return v.bytes_val, ValueType.BYTES
    elif value_type == "int32_val":
        return struct.pack("<i", v.int32_val), ValueType.INT32
    elif value_type == "int64_val":
        return struct.pack("<l", v.int64_val), ValueType.INT64
    else:
        raise ValueError(f"Value type not supported for Firestore: {v}")


def serialize_entity_key_prefix(entity_keys: List[str]) -> bytes:
    """
    Serialize keys to a bytestring so it can be used to prefix-scan through items stored in the online store
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


def serialize_entity_key(entity_key: EntityKeyProto) -> bytes:
    """
    Serialize entity key to a bytestring so it can be used as a lookup key in a hash table.

    We need this encoding to be stable; therefore we cannot just use protobuf serialization
    here since it does not guarantee that two proto messages containing the same data will
    serialize to the same byte string[1].

    [1] https://developers.google.com/protocol-buffers/docs/encoding
    """
    sorted_keys, sorted_values = zip(
        *sorted(zip(entity_key.join_keys, entity_key.entity_values))
    )

    output: List[bytes] = []
    for k in sorted_keys:
        output.append(struct.pack("<I", ValueType.STRING))
        output.append(k.encode("utf8"))
    for v in sorted_values:
        val_bytes, value_type = _serialize_val(v.WhichOneof("val"), v)

        output.append(struct.pack("<I", value_type))

        output.append(struct.pack("<I", len(val_bytes)))
        output.append(val_bytes)

    return b"".join(output)
