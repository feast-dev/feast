from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("couchbase", reason="couchbase not installed")

from couchbase.exceptions import (  # noqa: E402
    AuthenticationException,
    CollectionAlreadyExistsException,
    ScopeAlreadyExistsException,
    TimeoutException,
)

from feast.infra.online_stores.couchbase_online_store.couchbase import (  # noqa: E402
    CouchbaseOnlineStore,
    CouchbaseOnlineStoreConfig,
)
from feast.protos.feast.types.EntityKey_pb2 import (
    EntityKey as EntityKeyProto,  # noqa: E402
)
from feast.protos.feast.types.Value_pb2 import Value as ValueProto  # noqa: E402
from feast.repo_config import RepoConfig  # noqa: E402

pytestmark = pytest.mark.filterwarnings("ignore::RuntimeWarning")


@pytest.fixture
def config():
    return RepoConfig(
        registry="dummy_registry.db",
        project="test_project",
        provider="local",
        entity_key_serialization_version=3,
        online_store=CouchbaseOnlineStoreConfig(),
    )


@pytest.fixture
def table():
    table = MagicMock()
    table.name = "driver_stats"
    return table


def _rows(n):
    now = datetime.now(tz=timezone.utc)
    return [
        (
            EntityKeyProto(
                join_keys=["driver_id"], entity_values=[ValueProto(int64_val=i)]
            ),
            {"trips": ValueProto(int64_val=i * 10)},
            now,
            None,
        )
        for i in range(n)
    ]


def _store_with(collection):
    store = CouchbaseOnlineStore()
    store.bucket = MagicMock()
    return store, patch.object(store, "_get_conn", return_value=collection)


def test_write_batch_raises_when_upsert_fails(config, table):
    collection = MagicMock()
    collection.upsert.side_effect = TimeoutException("upsert timed out")
    progress = MagicMock()
    store, conn = _store_with(collection)

    with conn, pytest.raises(TimeoutException):
        store.online_write_batch(config, table, _rows(3), progress)

    # The failed row is not reported as written.
    progress.assert_not_called()


def test_write_batch_writes_and_reports_every_row(config, table):
    collection = MagicMock()
    progress = MagicMock()
    store, conn = _store_with(collection)

    with conn:
        store.online_write_batch(config, table, _rows(3), progress)

    assert collection.upsert.call_count == 3
    assert progress.call_count == 3


def test_update_raises_when_scope_cannot_be_created(config, table):
    store, conn = _store_with(MagicMock())
    store.bucket.collections.return_value.create_scope.side_effect = (
        AuthenticationException("not permitted")
    )

    with conn, pytest.raises(AuthenticationException):
        store.update(config, [], [table], [], [], partial=False)


def test_update_raises_when_collection_cannot_be_created(config, table):
    store, conn = _store_with(MagicMock())
    store.bucket.collections.return_value.create_collection.side_effect = (
        TimeoutException("create timed out")
    )

    with conn, pytest.raises(TimeoutException):
        store.update(config, [], [table], [], [], partial=False)


def test_update_accepts_scope_and_collection_that_already_exist(config, table):
    store, conn = _store_with(MagicMock())
    manager = store.bucket.collections.return_value
    manager.create_scope.side_effect = ScopeAlreadyExistsException("exists")
    manager.create_collection.side_effect = CollectionAlreadyExistsException("exists")

    with conn:
        store.update(config, [], [table], [], [], partial=False)
