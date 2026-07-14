"""Integration tests for remote materialization.

Runs a real feature server in a background thread, configures a client
FeatureStore with online_store pointing at the server, and verifies the
full flow using remote=True on the materialize() SDK call:
  client.materialize(remote=True) → POST /materialize-async → server runs locally →
  FV state transitions → client polls registry → returns success

Uses SQL registry (SQLite) to match the production path and exercise
the state transitions that the file-based registry handles differently.
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
from feast.infra.registry.sql import SqlRegistryConfig
from feast.types import Float32, Int64


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def e2e_repo(tmp_path):
    """Set up a minimal feature repo with SQL registry and SQLite online store."""
    registry_db = str(tmp_path / "registry.db")
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
        registry=SqlRegistryConfig(
            registry_type="sql",
            path=f"sqlite:///{registry_db}",
            cache_ttl_seconds=1,
        ),
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

    return store, config, registry_db, online_store_path


@pytest.fixture
def feature_server(e2e_repo):
    """Start a real feature server in a background thread on a free port."""
    store, config, registry_db, online_store_path = e2e_repo

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
    """Create a client FeatureStore with online_store.path pointing at the server."""
    from feast.infra.online_stores.remote import RemoteOnlineStoreConfig

    client_config = RepoConfig(
        project="test_remote_mat",
        provider="local",
        registry=server_config.registry,
        online_store=RemoteOnlineStoreConfig(
            type="remote",
            path=f"http://127.0.0.1:{server_port}",
        ),
        entity_key_serialization_version=3,
    )
    return FeatureStore(config=client_config)


def test_remote_materialize_e2e(feature_server):
    """Full E2E: client with remote=True materializes through server."""
    server_store, server_config, server_port = feature_server
    client_store = _make_client_store(server_config, server_port)

    now = datetime.now(tz=timezone.utc)

    client_store.materialize(
        start_date=now - timedelta(days=7),
        end_date=now,
        feature_views=["test_feature_view"],
        remote=True,
        timeout=30.0,
        poll_interval=0.5,
    )

    fv = client_store.registry.get_feature_view(
        "test_feature_view", "test_remote_mat", allow_cache=False
    )
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
        remote=True,
        timeout=30.0,
        poll_interval=0.5,
    )

    fv = client_store.registry.get_feature_view(
        "test_feature_view", "test_remote_mat", allow_cache=False
    )
    assert fv.state == FeatureViewState.AVAILABLE_ONLINE

    online_features = client_store.get_online_features(
        features=["test_feature_view:feature_a"],
        entity_rows=[{"entity_id": 2}],
    ).to_dict()

    assert online_features["feature_a"][0] == pytest.approx(20.0)


def test_remote_materialize_re_materialize(feature_server):
    """Re-materialization on already AVAILABLE_ONLINE FVs waits for completion.

    Validates the polling race fix: server sets MATERIALIZING before 202,
    so the client doesn't short-circuit on stale AVAILABLE_ONLINE.
    """
    server_store, server_config, server_port = feature_server
    client_store = _make_client_store(server_config, server_port)

    now = datetime.now(tz=timezone.utc)

    # First materialization
    client_store.materialize(
        start_date=now - timedelta(days=7),
        end_date=now,
        feature_views=["test_feature_view"],
        remote=True,
        timeout=30.0,
        poll_interval=0.5,
    )

    fv = client_store.registry.get_feature_view(
        "test_feature_view", "test_remote_mat", allow_cache=False
    )
    assert fv.state == FeatureViewState.AVAILABLE_ONLINE

    # Second materialization — must NOT return instantly
    start_time = time.monotonic()
    client_store.materialize(
        start_date=now - timedelta(days=7),
        end_date=now,
        feature_views=["test_feature_view"],
        remote=True,
        timeout=30.0,
        poll_interval=0.5,
    )
    elapsed = time.monotonic() - start_time

    # Should have waited at least one poll cycle (0.5s), not returned in <0.1s
    assert elapsed >= 0.3, (
        f"Re-materialization returned in {elapsed:.2f}s — polling race not fixed"
    )

    fv = client_store.registry.get_feature_view(
        "test_feature_view", "test_remote_mat", allow_cache=False
    )
    assert fv.state == FeatureViewState.AVAILABLE_ONLINE


def test_remote_materialize_force_local_bypasses_remote(feature_server):
    """_force_local=True runs locally even when remote=True.

    Uses the server_store (local online store) since _force_local skips
    the remote HTTP call entirely and writes to the local online store.
    """
    server_store, server_config, server_port = feature_server

    now = datetime.now(tz=timezone.utc)

    server_store.materialize(
        start_date=now - timedelta(days=7),
        end_date=now,
        feature_views=["test_feature_view"],
        remote=True,
        _force_local=True,
    )

    fv = server_store.registry.get_feature_view(
        "test_feature_view", "test_remote_mat", allow_cache=False
    )
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
            remote=True,
            timeout=10.0,
            poll_interval=0.5,
        )


def test_remote_materialize_failure_resets_state(feature_server):
    """When server-side materialization fails, FV state resets to GENERATED.

    The client's seen_materializing logic detects MATERIALIZING → GENERATED
    as a failure and reports it promptly (within one poll cycle).
    """
    server_store, server_config, server_port = feature_server
    client_store = _make_client_store(server_config, server_port)

    now = datetime.now(tz=timezone.utc)

    # Break the data source so materialization fails in the background thread
    fv = server_store.registry.get_feature_view(
        "test_feature_view", "test_remote_mat", allow_cache=False
    )
    fv.batch_source = FileSource(
        path="/nonexistent/path/that/will/fail.parquet",
        timestamp_field="event_timestamp",
    )
    server_store.apply([fv])

    # Client triggers remote materialization — server will fail in background
    with pytest.raises(Exception, match="Remote materialization failed"):
        client_store.materialize(
            start_date=now - timedelta(days=7),
            end_date=now,
            feature_views=["test_feature_view"],
            remote=True,
            timeout=30.0,
            poll_interval=0.5,
        )

    # FV state should be back to GENERATED (not stuck at MATERIALIZING)
    fv = client_store.registry.get_feature_view(
        "test_feature_view", "test_remote_mat", allow_cache=False
    )
    assert fv.state == FeatureViewState.GENERATED
