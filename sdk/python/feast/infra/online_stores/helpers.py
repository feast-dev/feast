import struct

import mmh3

from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.storage.Redis_pb2 import RedisKeyV2 as RedisKeyProto
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.repo_config import OnlineStoreConfig, RedisOnlineStoreConfig


def get_online_store_from_config(
    online_store_config: OnlineStoreConfig,
) -> OnlineStore:
    """Get the offline store from offline store config"""

    if online_store_config.__repr_name__() == "SqliteOnlineStoreConfig":
        from feast.infra.online_stores.sqlite import SqliteOnlineStore

        return SqliteOnlineStore()
    elif online_store_config.__repr_name__() == "DatastoreOnlineStoreConfig":
        from feast.infra.online_stores.datastore import DatastoreOnlineStore

        return DatastoreOnlineStore()
    elif isinstance(online_store_config, RedisOnlineStoreConfig):
        from feast.infra.online_stores.redis import RedisOnlineStore

        return RedisOnlineStore()
    raise ValueError(f"Unsupported online store config '{online_store_config}'")


def _redis_key(project: str, entity_key: EntityKeyProto):
    redis_key = RedisKeyProto(
        project=project,
        entity_names=entity_key.join_keys,
        entity_values=entity_key.entity_values,
    )
    return redis_key.SerializeToString()


def _mmh3(key: str):
    """
    Calculate murmur3_32 hash which is equal to scala version which is using little endian:
        https://stackoverflow.com/questions/29932956/murmur3-hash-different-result-between-python-and-java-implementation
        https://stackoverflow.com/questions/13141787/convert-decimal-int-to-little-endian-string-x-x
    """
    key_hash = mmh3.hash(key, signed=False)
    return bytes.fromhex(struct.pack("<Q", key_hash).hex()[:8])
