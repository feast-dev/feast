import struct
from typing import Any, List

import mmh3

from feast.importer import import_class
from feast.infra.key_encoding_utils import (
    serialize_entity_key,
    serialize_entity_key_prefix,
)
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto


def get_online_store_from_config(online_store_config: Any) -> OnlineStore:
    """Creates an online store corresponding to the given online store config."""
    module_name = online_store_config.__module__
    qualified_name = type(online_store_config).__name__
    class_name = qualified_name.replace("Config", "")
    online_store_class = import_class(module_name, class_name, "OnlineStore")
    return online_store_class()


def _redis_key(project: str, entity_key: EntityKeyProto) -> bytes:
    key: List[bytes] = [serialize_entity_key(entity_key), project.encode("utf-8")]
    return b"".join(key)


def _redis_key_prefix(entity_keys: List[str]) -> bytes:
    return serialize_entity_key_prefix(entity_keys)


def _mmh3(key: str):
    """
    Calculate murmur3_32 hash which is equal to scala version which is using little endian:
        https://stackoverflow.com/questions/29932956/murmur3-hash-different-result-between-python-and-java-implementation
        https://stackoverflow.com/questions/13141787/convert-decimal-int-to-little-endian-string-x-x
    """
    key_hash = mmh3.hash(key, signed=False)
    return bytes.fromhex(struct.pack("<Q", key_hash).hex()[:8])


def compute_entity_id(entity_key: EntityKeyProto) -> str:
    """
    Compute Entity id given Feast Entity Key for online stores.
    Remember that Entity here refers to `EntityKeyProto` which is used in some online stores to encode the keys.
    It has nothing to do with the Entity concept we have in Feast.
    """
    return mmh3.hash_bytes(serialize_entity_key(entity_key)).hex()
