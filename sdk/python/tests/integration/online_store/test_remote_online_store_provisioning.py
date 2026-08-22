"""
Regression test for feast-dev/feast#6693.

  'feast apply' does not create online store tables in remote mode.

Contract under test (holds regardless of how the fix is implemented):

    Applying a NEW feature view through a client whose online store is
    `type: remote` must leave that feature view writable. Today it does not:
    the feature view lands in the registry, but no table is ever provisioned
    server-side, so the write that `feast materialize` performs returns 500.

Note on the existing coverage: `test_remote_online_store_read_write` in
sdk/python/tests/integration/online_store/test_remote_online_store.py already
applies a feature view through a remote client and writes to it, and passes.
It passes because it applies `driver_hourly_stats` -- a name the server had
already provisioned during its own `feast apply` at setup. The table exists
before the client ever touches it, which masks the bug. This test applies a
feature view the server has never seen.
"""

import os
import shutil
import socket
import sqlite3
import subprocess
import tempfile
import time
from datetime import timedelta
from textwrap import dedent
from typing import Iterator, List

import pandas as pd
import pytest

from feast import Entity, FeatureStore, FeatureView, FileSource
from feast.driver_test_data import create_driver_hourly_stats_df
from feast.field import Field
from feast.types import Float32, Int64
from feast.utils import _utc_now

FEAST_BIN = "feast"
PROJECT = "remote_provisioning"

# A feature view name the server has never applied. This is the whole point.
NEW_FV_NAME = "never_applied_server_side"


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _port_open(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1.0)
        return s.connect_ex((host, port)) == 0


def _server_online_tables(repo_path: str) -> List[str]:
    """Table names in the server's own SQLite online store.

    Read directly rather than through any Feast API: the point of this test is
    that the table physically exists server-side, so asking Feast about it
    would beg the question. Opened read-only -- the server process has the
    file open too.
    """
    db_path = os.path.join(repo_path, "data", "online_store.db")
    # SqliteOnlineStore.teardown() unlinks the whole file rather than dropping
    # tables one by one, so a missing file means "nothing left", not an error.
    if not os.path.exists(db_path):
        return []
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        rows = con.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return [r[0] for r in rows]
    finally:
        con.close()


@pytest.fixture(scope="module")
def remote_server():
    """`feast init` + `feast apply` + `feast serve`, backed by SQLite."""
    tmp = tempfile.mkdtemp(prefix="feast_6693_server_")

    subprocess.run(
        [FEAST_BIN, "init", PROJECT], cwd=tmp, capture_output=True, check=True
    )
    repo_path = os.path.join(tmp, PROJECT, "feature_repo")
    subprocess.run(
        [FEAST_BIN, "-c", repo_path, "apply"], cwd=tmp, capture_output=True, check=True
    )

    registry_path = os.path.join(repo_path, "data", "registry.db")
    assert os.path.exists(registry_path), "server registry was not created"

    port = _free_port()
    # Log to files, NOT subprocess.PIPE: `feast serve` is chatty, and an
    # undrained pipe fills its 64K OS buffer and deadlocks the server.
    log_path = os.path.join(tmp, "server.log")
    log_file = open(log_path, "w")
    proc = subprocess.Popen(
        [
            FEAST_BIN,
            "-c",
            repo_path,
            "serve",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        stdout=log_file,
        stderr=subprocess.STDOUT,
        text=True,
    )

    def _server_log() -> str:
        log_file.flush()
        with open(log_path) as f:
            return f.read()

    deadline = time.time() + 90
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"feast serve exited early:\n{_server_log()}")
        if _port_open("127.0.0.1", port):
            break
        time.sleep(1)
    else:
        proc.kill()
        raise RuntimeError(f"feast serve did not come up in 90s:\n{_server_log()}")

    yield {
        "url": f"http://127.0.0.1:{port}",
        "registry_path": registry_path,
        "repo_path": repo_path,
        "server_log": _server_log,
    }

    proc.kill()
    proc.wait(timeout=30)
    log_file.close()
    # ignore_errors: on Windows the just-killed server can still hold a handle
    # to the SQLite files for a moment.
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.fixture(scope="module")
def client_store(remote_server) -> Iterator[FeatureStore]:
    """A client whose online store is the remote feature server."""
    tmp = tempfile.mkdtemp(prefix="feast_6693_client_")
    with open(os.path.join(tmp, "feature_store.yaml"), "w") as f:
        f.write(
            dedent(
                f"""
                project: {PROJECT}
                registry: {remote_server["registry_path"]}
                provider: local
                entity_key_serialization_version: 3
                auth:
                    type: no_auth
                online_store:
                    type: remote
                    path: {remote_server["url"]}
                """
            ).strip()
        )
    yield FeatureStore(repo_path=tmp)

    shutil.rmtree(tmp, ignore_errors=True)


def _new_feature_view(data_path: str) -> tuple:
    driver = Entity(name="driver_id", description="Driver id")
    source = FileSource(
        path=data_path,
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )
    fv = FeatureView(
        name=NEW_FV_NAME,
        entities=[driver],
        ttl=timedelta(days=1),
        schema=[
            Field(name="conv_rate", dtype=Float32),
            Field(name="avg_daily_trips", dtype=Int64),
        ],
        source=source,
    )
    return driver, source, fv


@pytest.mark.integration
def test_remote_apply_provisions_the_table_and_teardown_drops_it(
    client_store, remote_server
):
    """The full remote lifecycle: apply provisions the table, teardown drops it.

    Apply fails before the fix with HTTP 500 / `no such table`, because
    RemoteOnlineStore.update() is a no-op and nothing provisions server-side.
    Teardown is the mirror: without RemoteOnlineStore.teardown() the table is
    left behind. Both halves are asserted against the server's own SQLite file,
    so this covers /update-infra and /teardown-infra end to end.
    """
    # A real parquet file, so nothing fails for want of a source on disk.
    data_dir = os.path.join(client_store.repo_path, "data")
    os.makedirs(data_dir, exist_ok=True)
    data_path = os.path.join(data_dir, "driver_stats.parquet")
    end = _utc_now().replace(microsecond=0, second=0, minute=0)
    create_driver_hourly_stats_df([1001], end - timedelta(days=1), end).to_parquet(
        path=data_path, allow_truncated_timestamps=True
    )

    driver, source, fv = _new_feature_view(data_path)
    client_store.apply([driver, fv])

    # The registry accepted it -- the feature view genuinely exists.
    assert NEW_FV_NAME in [v.name for v in client_store.list_feature_views()]

    now = pd.Timestamp(_utc_now()).round("ms")
    df = pd.DataFrame(
        {
            "driver_id": [1001],
            "conv_rate": [0.75],
            "avg_daily_trips": [42],
            "event_timestamp": [now],
            "created": [now],
        }
    )

    # This is what `feast materialize` does through the feature server.
    try:
        client_store.write_to_online_store(feature_view_name=NEW_FV_NAME, df=df)
    except Exception:
        print("\n------------------- server log (tail) -------------------")
        print(remote_server["server_log"]()[-4000:])
        print("---------------------------------------------------------")
        raise

    features = client_store.get_online_features(
        features=[f"{NEW_FV_NAME}:conv_rate", f"{NEW_FV_NAME}:avg_daily_trips"],
        entity_rows=[{"driver_id": 1001}],
    ).to_dict()

    assert features["avg_daily_trips"] == [42]
    assert round(features["conv_rate"][0], 2) == 0.75

    # The table is physically there in the server's store, not just implied by
    # the write having succeeded.
    tables = _server_online_tables(remote_server["repo_path"])
    assert any(NEW_FV_NAME in t for t in tables), (
        f"{NEW_FV_NAME} was never provisioned server-side; tables={tables}"
    )

    # And the mirror image: teardown must drop it again.
    client_store.teardown()

    tables = _server_online_tables(remote_server["repo_path"])
    assert not any(NEW_FV_NAME in t for t in tables), (
        f"{NEW_FV_NAME} survived teardown; tables={tables}"
    )
