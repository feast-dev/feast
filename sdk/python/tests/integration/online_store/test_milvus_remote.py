"""Integration tests for the Milvus online store against a Milvus server or Zilliz Cloud.

These tests only run when ``ZILLIZ_URI`` and ``ZILLIZ_TOKEN`` are set, e.g.::

    ZILLIZ_URI=https://<cluster>.zillizcloud.com ZILLIZ_TOKEN=<user:password> \
        pytest --integration sdk/python/tests/integration/online_store/test_milvus_remote.py

They also run against a self-hosted Milvus server, e.g.
``ZILLIZ_URI=http://localhost:19530 ZILLIZ_TOKEN=root:Milvus``.
"""

import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, TypeVar
from urllib.parse import urlparse

import pytest

from feast import Entity, FeatureView
from feast.field import Field
from feast.infra.online_stores.milvus_online_store.milvus import MilvusOnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RepoConfig
from feast.types import Float32, Int64, String
from feast.value_type import ValueType

T = TypeVar("T")

ZILLIZ_URI = os.environ.get("ZILLIZ_URI")
ZILLIZ_TOKEN = os.environ.get("ZILLIZ_TOKEN")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not (ZILLIZ_URI and ZILLIZ_TOKEN),
        reason="ZILLIZ_URI and ZILLIZ_TOKEN must be set to run Milvus server tests",
    ),
]


def _connection_config() -> Dict[str, Any]:
    assert ZILLIZ_URI and ZILLIZ_TOKEN
    parsed = urlparse(ZILLIZ_URI)
    default_port = 443 if parsed.scheme == "https" else 19530
    username, _, password = ZILLIZ_TOKEN.partition(":")
    return {
        "host": f"{parsed.scheme}://{parsed.hostname}",
        "port": parsed.port or default_port,
        "username": username,
        "password": password,
    }


def _repo_config(tmp_path: Path, project: str, **online_store: Any) -> RepoConfig:
    return RepoConfig(
        project=project,
        provider="local",
        registry=str(tmp_path / "registry.db"),
        online_store={
            "type": "milvus",
            "embedding_dim": 2,
            **_connection_config(),
            **online_store,
        },
        entity_key_serialization_version=3,
        repo_path=tmp_path,
    )


@pytest.fixture
def project() -> str:
    return f"feast_it_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def store() -> Iterator[MilvusOnlineStore]:
    store = MilvusOnlineStore()
    yield store
    if store.client is not None:
        for name in list(store._collections):
            store.client.drop_collection(name)


def _eventually(fn: Callable[[], T], check: Callable[[T], bool]) -> T:
    """Retry ``fn`` until ``check`` passes, tolerating Bounded consistency."""
    deadline = time.monotonic() + 15
    while True:
        result = fn()
        if check(result) or time.monotonic() > deadline:
            return result
        time.sleep(0.5)


def _entity_key(driver_id: int) -> EntityKeyProto:
    return EntityKeyProto(
        join_keys=["driver_id"], entity_values=[ValueProto(int64_val=driver_id)]
    )


def _write_rows(
    store: MilvusOnlineStore,
    config: RepoConfig,
    fv: FeatureView,
    rows: Dict[int, Dict[str, ValueProto]],
) -> None:
    now = datetime.now(timezone.utc)
    store.online_write_batch(
        config,
        fv,
        [(_entity_key(k), dict(v), now, now) for k, v in rows.items()],
        progress=None,
    )


def _read(
    store: MilvusOnlineStore,
    config: RepoConfig,
    fv: FeatureView,
    driver_ids: List[int],
    features: List[str],
) -> List[Optional[Dict[str, ValueProto]]]:
    results = store.online_read(
        config, fv, [_entity_key(d) for d in driver_ids], features
    )
    return [values for _, values in results]


def _scalar_feature_view() -> FeatureView:
    return FeatureView(
        name="driver_stats",
        entities=[
            Entity(
                name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
            )
        ],
        ttl=timedelta(days=1),
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="trips_today", dtype=Float32),
            Field(name="city", dtype=String),
        ],
    )


def test_scalar_feature_view_round_trip(
    tmp_path: Path, project: str, store: MilvusOnlineStore
) -> None:
    """Feature views without vectors rely on the placeholder vector."""
    config = _repo_config(tmp_path, project)
    fv = _scalar_feature_view()
    store.update(config, [], [fv], [], [], partial=False)

    _write_rows(
        store,
        config,
        fv,
        {
            1: {
                "trips_today": ValueProto(float_val=3.0),
                "city": ValueProto(string_val="Paris"),
            }
        },
    )
    rows = _eventually(
        lambda: _read(store, config, fv, [1], ["trips_today", "city"]),
        lambda rows: rows[0] is not None,
    )

    assert rows[0] is not None and rows[0]["city"].string_val == "Paris"
