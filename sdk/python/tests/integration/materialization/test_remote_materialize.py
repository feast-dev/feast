"""Integration tests for remote materialization.

Runs a real feature server in a background thread, configures a client
FeatureStore with materialize_mode=remote, and verifies the full flow:
  client.materialize() → POST /materialize-async → server runs locally →
  FV state transitions → client polls registry → returns success
"""

import socket
import threading
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest
import uvicorn

from feast import Entity, FeatureStore, FeatureView, Field, RepoConfig
from feast.feature_server import get_app
from feast.feature_view import FeatureViewState
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.types import Float32, Int64


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def e2e_repo(tmp_path):
    """Set up a minimal feature repo with parquet data, registry, and online store."""
    registry_path = str(tmp_path / "registry.db")
    online_store_path = str(tmp_path / "online_store.db")
    data_path = str(tmp_path / "data.parquet")

    now = datetime.now(tz=timezone.utc)
    df = pd.DataFrame(
        {
            "entity_id": [1, 2, 3, 4, 5],
            "feature_a": [10.0, 20.0, 30.0, 40.0, 50.0],
            "feature_b": [100, 200, 300, 400, 500],
            "event_timestamp": [now - timedelta(hours=i) for i in range(5)],
            "created": [now] * 5,
        }
    )
    df.to_parquet(data_path)

    config = RepoConfig(
        project="test_remote_mat",
        provider="local",
        registry=registry_path,
        online_store=SqliteOnlineStoreConfig(path=online_store_path),
        entity_key_serialization_version=3,
    )

    store = FeatureStore(config=config)

    entity = Entity(name="entity_id", join_keys=["entity_id"])

    source = FileSource(
        path=data_path,
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )

    fv = FeatureView(
        name="test_feature_view",
        entities=[entity],
        schema=[
            Field(name="feature_a", dtype=Float32),
            Field(name="feature_b", dtype=Int64),
        ],
        source=source,
        ttl=timedelta(days=7),
    )

    store.apply([entity, fv])

    return store, config, registry_path, online_store_path


@pytest.fixture
def feature_server(e2e_repo):
    """Start a real feature server in a background thread on a free port."""
    store, config, registry_path, online_store_path = e2e_repo

    app = get_app(store)
    server_port = _find_free_port()

    server_config = uvicorn.Config(
        app, host="127.0.0.1", port=server_port, log_level="warning"
    )
    server = uvicorn.Server(server_config)

    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    import httpx

    for _ in range(50):
        try:
            resp = httpx.get(f"http://127.0.0.1:{server_port}/health")
            if resp.status_code == 200:
                break
        except Exception:
            pass
        time.sleep(0.1)
    else:
        pytest.fail("Feature server did not start in time")

    yield store, config, server_port

    server.should_exit = True
    thread.join(timeout=5)


def _make_client_store(server_config, server_port):
    """Create a client FeatureStore configured for remote materialization."""
    from feast.infra.feature_servers.local_process.config import (
        LocalFeatureServerConfig,
    )

    client_config = RepoConfig(
        project="test_remote_mat",
        provider="local",
        registry=server_config.registry,
        online_store=server_config.online_store,
        entity_key_serialization_version=3,
        feature_server=LocalFeatureServerConfig(
            enabled=True,
            materialize_mode="remote",
            url=f"http://127.0.0.1:{server_port}",
            materialize_timeout=30.0,
            materialize_poll_interval=0.5,
        ),
    )
    return FeatureStore(config=client_config)


def test_remote_materialize_e2e(feature_server):
    """Full E2E: client with remote mode materializes through server."""
    server_store, server_config, server_port = feature_server
    client_store = _make_client_store(server_config, server_port)

    now = datetime.now(tz=timezone.utc)
    start_date = now - timedelta(days=7)
    end_date = now

    client_store.materialize(
        start_date=start_date,
        end_date=end_date,
        feature_views=["test_feature_view"],
    )

    fv = client_store.registry.get_feature_view("test_feature_view", "test_remote_mat")
    assert fv.state == FeatureViewState.AVAILABLE_ONLINE

    online_features = client_store.get_online_features(
        features=["test_feature_view:feature_a", "test_feature_view:feature_b"],
        entity_rows=[{"entity_id": 1}],
    ).to_dict()

    assert online_features["feature_a"][0] == pytest.approx(10.0)
    assert online_features["feature_b"][0] == 100


def test_remote_materialize_incremental_e2e(feature_server):
    """E2E for materialize_incremental through remote path."""
    server_store, server_config, server_port = feature_server
    client_store = _make_client_store(server_config, server_port)

    now = datetime.now(tz=timezone.utc)

    client_store.materialize_incremental(
        end_date=now,
        feature_views=["test_feature_view"],
    )

    fv = client_store.registry.get_feature_view("test_feature_view", "test_remote_mat")
    assert fv.state == FeatureViewState.AVAILABLE_ONLINE

    online_features = client_store.get_online_features(
        features=["test_feature_view:feature_a"],
        entity_rows=[{"entity_id": 2}],
    ).to_dict()

    assert online_features["feature_a"][0] == pytest.approx(20.0)


def test_remote_materialize_force_local_bypasses_remote(feature_server):
    """_force_local=True runs locally even with materialize_mode=remote."""
    server_store, server_config, server_port = feature_server
    client_store = _make_client_store(server_config, server_port)

    now = datetime.now(tz=timezone.utc)

    # With _force_local, it should run locally (same effect, no HTTP call)
    client_store.materialize(
        start_date=now - timedelta(days=7),
        end_date=now,
        feature_views=["test_feature_view"],
        _force_local=True,
    )

    fv = client_store.registry.get_feature_view("test_feature_view", "test_remote_mat")
    assert fv.state == FeatureViewState.AVAILABLE_ONLINE


def test_remote_materialize_server_error_propagates(feature_server):
    """Client gets an error when the server rejects the request.

    A non-existent feature view causes a server-side error during authz
    validation, which returns HTTP 500. The client wraps this as an
    Exception with a descriptive message.
    """
    server_store, server_config, server_port = feature_server
    client_store = _make_client_store(server_config, server_port)

    now = datetime.now(tz=timezone.utc)

    with pytest.raises(Exception, match="Failed to trigger remote materialization"):
        client_store.materialize(
            start_date=now - timedelta(days=1),
            end_date=now,
            feature_views=["nonexistent_fv"],
        )
