import struct
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import mmh3

from feast.infra.online_stores.helpers import (
    _mmh3,
    _redis_key,
    _redis_key_prefix,
    _to_naive_utc,
    compute_entity_id,
    get_online_store_from_config,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto


def _make_entity_key(join_keys, values):
    """Helper to build an EntityKeyProto."""
    entity_key = EntityKeyProto()
    for k in join_keys:
        entity_key.join_keys.append(k)
    for v in values:
        entity_key.entity_values.append(v)
    return entity_key


class TestRedisKey:
    def test_redis_key_produces_bytes(self):
        entity_key = _make_entity_key(["user_id"], [ValueProto(string_val="user_1")])
        result = _redis_key("my_project", entity_key)
        assert isinstance(result, bytes)

    def test_redis_key_contains_project(self):
        entity_key = _make_entity_key(["user_id"], [ValueProto(string_val="user_1")])
        result = _redis_key("my_project", entity_key)
        assert b"my_project" in result

    def test_redis_key_different_projects_produce_different_keys(self):
        entity_key = _make_entity_key(["user_id"], [ValueProto(string_val="user_1")])
        key_a = _redis_key("project_a", entity_key)
        key_b = _redis_key("project_b", entity_key)
        assert key_a != key_b

    def test_redis_key_different_entities_produce_different_keys(self):
        ek1 = _make_entity_key(["user_id"], [ValueProto(string_val="user_1")])
        ek2 = _make_entity_key(["user_id"], [ValueProto(string_val="user_2")])
        assert _redis_key("proj", ek1) != _redis_key("proj", ek2)

    def test_redis_key_serialization_version_changes_key(self):
        entity_key = _make_entity_key(["user_id"], [ValueProto(string_val="user_1")])
        key_v2 = _redis_key("proj", entity_key, entity_key_serialization_version=2)
        key_v3 = _redis_key("proj", entity_key, entity_key_serialization_version=3)
        assert key_v2 != key_v3


class TestRedisKeyPrefix:
    def test_returns_bytes(self):
        result = _redis_key_prefix(["user_id"])
        assert isinstance(result, bytes)

    def test_different_keys_produce_different_prefixes(self):
        prefix_a = _redis_key_prefix(["user_id"])
        prefix_b = _redis_key_prefix(["merchant_id"])
        assert prefix_a != prefix_b


class TestMmh3:
    def test_returns_bytes(self):
        result = _mmh3("test_key")
        assert isinstance(result, bytes)

    def test_consistent_hash(self):
        assert _mmh3("hello") == _mmh3("hello")

    def test_different_keys_different_hashes(self):
        assert _mmh3("key_a") != _mmh3("key_b")

    def test_hash_length(self):
        result = _mmh3("test")
        assert len(result) == 4  # 32-bit hash = 4 bytes

    def test_little_endian_format(self):
        """Verify the hash matches the expected little-endian encoding."""
        key = "test_key"
        key_hash = mmh3.hash(key, signed=False)
        expected = bytes.fromhex(struct.pack("<Q", key_hash).hex()[:8])
        assert _mmh3(key) == expected


class TestComputeEntityId:
    def test_returns_hex_string(self):
        entity_key = _make_entity_key(["user_id"], [ValueProto(string_val="user_1")])
        result = compute_entity_id(entity_key)
        assert isinstance(result, str)
        # Verify it's a valid hex string
        int(result, 16)

    def test_consistent_entity_id(self):
        entity_key = _make_entity_key(["user_id"], [ValueProto(string_val="user_1")])
        assert compute_entity_id(entity_key) == compute_entity_id(entity_key)

    def test_different_entities_different_ids(self):
        ek1 = _make_entity_key(["user_id"], [ValueProto(string_val="user_1")])
        ek2 = _make_entity_key(["user_id"], [ValueProto(string_val="user_2")])
        assert compute_entity_id(ek1) != compute_entity_id(ek2)


class TestToNaiveUtc:
    def test_naive_timestamp_unchanged(self):
        ts = datetime(2024, 1, 15, 12, 0, 0)
        result = _to_naive_utc(ts)
        assert result == ts
        assert result.tzinfo is None

    def test_utc_timestamp_stripped(self):
        ts = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        result = _to_naive_utc(ts)
        assert result == datetime(2024, 1, 15, 12, 0, 0)
        assert result.tzinfo is None

    def test_non_utc_converted_to_utc(self):
        from datetime import timedelta

        est = timezone(timedelta(hours=-5))
        ts = datetime(2024, 1, 15, 12, 0, 0, tzinfo=est)
        result = _to_naive_utc(ts)
        assert result == datetime(2024, 1, 15, 17, 0, 0)
        assert result.tzinfo is None


class TestGetOnlineStoreFromConfig:
    @patch("feast.infra.online_stores.helpers.import_class")
    def test_resolves_config_to_store_class(self, mock_import):
        mock_store_cls = MagicMock()
        mock_import.return_value = mock_store_cls

        config = MagicMock()
        config.__module__ = "feast.infra.online_stores.redis"
        type(config).__name__ = "RedisOnlineStoreConfig"

        get_online_store_from_config(config)

        mock_import.assert_called_once_with(
            "feast.infra.online_stores.redis",
            "RedisOnlineStore",
            "OnlineStore",
        )
        mock_store_cls.assert_called_once()
