import struct
from typing import Dict, Set

import mmh3

from feast.data_source import BigQuerySource, DataSource, FileSource
from feast.errors import FeastOnlineStoreUnsupportedDataSource
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.storage.Redis_pb2 import RedisKeyV2 as RedisKeyProto
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.repo_config import (
    DatastoreOnlineStoreConfig,
    OnlineStoreConfig,
    RedisOnlineStoreConfig,
    SqliteOnlineStoreConfig,
)


def get_online_store_from_config(
    online_store_config: OnlineStoreConfig,
) -> OnlineStore:
    """Get the offline store from offline store config"""

    if isinstance(online_store_config, SqliteOnlineStoreConfig):
        from feast.infra.online_stores.sqlite import SqliteOnlineStore

        return SqliteOnlineStore()
    elif isinstance(online_store_config, DatastoreOnlineStoreConfig):
        from feast.infra.online_stores.datastore import DatastoreOnlineStore

        return DatastoreOnlineStore()
    elif isinstance(online_store_config, RedisOnlineStoreConfig):
        from feast.infra.online_stores.redis import RedisOnlineStore

        return RedisOnlineStore()
    raise ValueError(f"Unsupported offline store config '{online_store_config}'")


SUPPORTED_SOURCES = {
    SqliteOnlineStoreConfig: {FileSource},
    DatastoreOnlineStoreConfig: {BigQuerySource},
    RedisOnlineStoreConfig: {FileSource, BigQuerySource},
}


def assert_online_store_supports_data_source(
    online_store_config: OnlineStoreConfig, data_source: DataSource
):
    supported_sources = SUPPORTED_SOURCES.get(online_store_config.__class__, {})
    # This is needed because checking for `in` with Union types breaks mypy.
    # https://github.com/python/mypy/issues/4954
    # We can replace this with `data_source.__class__ in SUPPORTED_SOURCES[online_store_config.__class__]`
    # Once ^ is resolved.
    if supported_sources:
        for source in supported_sources:
            if source == data_source.__class__:
                return
    raise FeastOnlineStoreUnsupportedDataSource(
        online_store_config.type, data_source.__class__.__name__
    )


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
