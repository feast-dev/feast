"""Unit tests for how the PostgreSQL online store hands out connections."""

from unittest.mock import AsyncMock, MagicMock

import psycopg
import pytest

from feast.infra.online_stores.postgres_online_store.postgres import (
    PostgreSQLOnlineStore,
)
from feast.repo_config import RegistryConfig, RepoConfig


def _config(conn_type: str) -> RepoConfig:
    return RepoConfig(
        project="proj",
        provider="local",
        registry=RegistryConfig(path="registry.db"),
        online_store={
            "type": "postgres",
            "host": "localhost",
            "port": 5432,
            "database": "test",
            "user": "root",
            "password": "test",  # pragma: allowlist secret
            "conn_type": conn_type,
        },
        entity_key_serialization_version=3,
    )


def test_pooled_connection_is_returned_when_the_query_fails():
    store = PostgreSQLOnlineStore()
    store._conn_pool = MagicMock()
    conn = store._conn_pool.getconn.return_value

    with pytest.raises(RuntimeError):
        with store._get_conn(_config("pool")):
            raise RuntimeError("query failed")

    store._conn_pool.putconn.assert_called_once_with(conn)


def test_singleton_connection_is_rolled_back_when_the_query_fails():
    store = PostgreSQLOnlineStore()
    store._conn = MagicMock(closed=False)

    with pytest.raises(RuntimeError):
        with store._get_conn(_config("singleton")):
            raise RuntimeError("query failed")

    store._conn.rollback.assert_called_once_with()


def test_failed_rollback_does_not_hide_the_query_error():
    store = PostgreSQLOnlineStore()
    store._conn = MagicMock(closed=False)
    store._conn.rollback.side_effect = psycopg.OperationalError("connection lost")

    with pytest.raises(RuntimeError):
        with store._get_conn(_config("singleton")):
            raise RuntimeError("query failed")


async def test_pooled_async_connection_is_returned_when_the_query_fails():
    store = PostgreSQLOnlineStore()
    store._conn_pool_async = AsyncMock()
    conn = store._conn_pool_async.getconn.return_value

    with pytest.raises(RuntimeError):
        async with store._get_conn_async(_config("pool")):
            raise RuntimeError("query failed")

    store._conn_pool_async.putconn.assert_awaited_once_with(conn)


async def test_singleton_async_connection_is_rolled_back_when_the_query_fails():
    store = PostgreSQLOnlineStore()
    store._conn_async = AsyncMock()
    store._conn_async.closed = False

    with pytest.raises(RuntimeError):
        async with store._get_conn_async(_config("singleton"), autocommit=False):
            raise RuntimeError("query failed")

    store._conn_async.rollback.assert_awaited_once_with()


async def test_failed_async_rollback_does_not_hide_the_query_error():
    store = PostgreSQLOnlineStore()
    store._conn_async = AsyncMock()
    store._conn_async.closed = False
    store._conn_async.rollback.side_effect = psycopg.OperationalError("connection lost")

    with pytest.raises(RuntimeError, match="query failed"):
        async with store._get_conn_async(_config("singleton")):
            raise RuntimeError("query failed")
