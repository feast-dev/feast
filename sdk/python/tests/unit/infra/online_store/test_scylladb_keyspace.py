"""#7226: the first read/write in a process must use the configured keyspace."""

from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("cassandra")

from feast import RepoConfig  # noqa: E402
from feast.infra.online_stores.scylladb_online_store import scylladb  # noqa: E402
from feast.infra.online_stores.scylladb_online_store.scylladb import (  # noqa: E402
    ScyllaDBOnlineStore,
    ScyllaDBOnlineStoreConfig,
)


def _config() -> RepoConfig:
    return RepoConfig(
        project="proj",
        provider="local",
        registry="memory://",
        online_store=ScyllaDBOnlineStoreConfig(hosts=["127.0.0.1"], keyspace="my_ks"),
        entity_key_serialization_version=3,
    )


def _fresh_store():
    """A new store whose session setup is faked but still sets _keyspace."""
    store = ScyllaDBOnlineStore()
    session = MagicMock()

    def fake_get_session(self, config):
        self._keyspace = config.online_store.keyspace
        return session

    return (
        store,
        session,
        patch.object(ScyllaDBOnlineStore, "_get_session", fake_get_session),
    )


def _table():
    table = MagicMock()
    table.name = "fv"
    table.ttl = None
    return table


@patch.object(scylladb, "execute_concurrent_with_args", return_value=[])
@patch.object(scylladb, "_get_vector_features", return_value=[])
def test_first_write_uses_configured_keyspace(_vec, _exec):
    store, session, fake = _fresh_store()
    with fake:
        store.online_write_batch(_config(), _table(), [], None)
    cql = session.prepare.call_args[0][0]
    assert '"my_ks"."proj_fv"' in cql


@patch.object(scylladb, "execute_concurrent_with_args", return_value=[])
def test_first_read_uses_configured_keyspace(_exec):
    store, session, fake = _fresh_store()
    with fake:
        store.online_read(_config(), _table(), [])
    cql = session.prepare.call_args[0][0]
    assert '"my_ks"."proj_fv"' in cql
