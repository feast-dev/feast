import sqlite3
from datetime import datetime, timedelta, timezone

from feast import Entity, FeatureView, Field, FileSource, RepoConfig
from feast.infra.online_stores.sqlite import SqliteOnlineStore, SqliteOnlineStoreConfig
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.types import Int64


def _entity_key(driver_id: int) -> EntityKeyProto:
    return EntityKeyProto(
        join_keys=["driver_id"], entity_values=[ValueProto(int64_val=driver_id)]
    )


def test_online_read_with_more_keys_than_sqlite_variable_limit(tmp_path):
    config = RepoConfig(
        project="read_limit",
        registry=str(tmp_path / "registry.db"),
        provider="local",
        online_store=SqliteOnlineStoreConfig(path=str(tmp_path / "online.db")),
        offline_store={"type": "dask"},
        entity_key_serialization_version=3,
    )
    driver = Entity(name="driver_id", join_keys=["driver_id"])
    view = FeatureView(
        name="driver_stats",
        entities=[driver],
        ttl=timedelta(days=1),
        schema=[Field(name="trips", dtype=Int64)],
        source=FileSource(path=str(tmp_path / "unused.parquet"), timestamp_field="ts"),
    )
    store = SqliteOnlineStore()
    store.update(config, [], [view], [], [driver], partial=False)

    now = datetime.now(tz=timezone.utc)
    num_keys = 120
    store.online_write_batch(
        config,
        view,
        [
            (_entity_key(i), {"trips": ValueProto(int64_val=i * 10)}, now, None)
            for i in range(num_keys)
        ],
        None,
    )

    # Lower the per-statement variable limit so that one IN (...) list over all
    # keys no longer fits, whatever limit this SQLite build was compiled with.
    store._get_conn(config).setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 50)
    # An id that was never written, and a repeated id, ride along.
    keys = [_entity_key(i) for i in range(num_keys)] + [
        _entity_key(10_000),
        _entity_key(3),
    ]
    result = store.online_read(config, view, keys)

    assert len(result) == num_keys + 2
    for i in range(num_keys):
        ts, features = result[i]
        assert ts is not None and features is not None
        assert features["trips"].int64_val == i * 10
    assert result[num_keys] == (None, None)
    assert result[num_keys + 1][1]["trips"].int64_val == 30
    assert store.online_read(config, view, []) == []
