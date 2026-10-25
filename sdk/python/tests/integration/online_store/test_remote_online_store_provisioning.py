"""Remote apply must provision a previously unseen feature view (#6693)."""

from __future__ import annotations

import sqlite3
import subprocess
import time
from datetime import timedelta
from pathlib import Path

import pandas as pd
import pytest
from click.testing import CliRunner

from feast import Entity, FeatureStore, FeatureView, Field, FileSource, RepoConfig
from feast.cli.cli import cli
from feast.types import Float32, Int64
from feast.utils import _utc_now
from feast.value_type import ValueType
from tests.utils.http_server import check_port_open, free_port

PROJECT = "remote_provisioning"
NEW_FV_NAME = "never_applied_server_side"


def _server_online_tables(repo: Path) -> set[str]:
    db = repo / "data" / "online_store.db"
    if not db.exists():
        return set()  # SQLite teardown unlinks the database.
    con = sqlite3.connect(f"{db.as_uri()}?mode=ro", uri=True)
    try:
        return {
            row[0]
            for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
    finally:
        con.close()


@pytest.fixture
async def remote_server(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    result = CliRunner().invoke(cli, ["init", PROJECT])
    assert result.exit_code == 0, result.output
    repo = tmp_path / PROJECT / "feature_repo"
    subprocess.run(["feast", "-c", str(repo), "apply"], capture_output=True, check=True)
    port = free_port()
    log_path = tmp_path / "server.log"
    # An undrained subprocess.PIPE can deadlock a chatty server.
    with log_path.open("w") as log:
        proc = subprocess.Popen(
            [
                "feast",
                "-c",
                str(repo),
                "serve",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + 90
            while not check_port_open("127.0.0.1", port):
                if proc.poll() is not None or time.monotonic() >= deadline:
                    pytest.fail(
                        f"Feature server failed to start:\n{log_path.read_text()}"
                    )
                time.sleep(0.2)
            client = FeatureStore(
                config=RepoConfig(
                    project=PROJECT,
                    registry=str(repo / "data" / "registry.db"),
                    provider="local",
                    entity_key_serialization_version=3,
                    online_store={"type": "remote", "path": f"http://127.0.0.1:{port}"},
                )
            )
            try:
                yield client, repo
            except Exception:
                print(log_path.read_text()[-4000:])
                raise
            finally:
                await client.close()
        finally:
            proc.kill()
            proc.wait(timeout=30)


@pytest.mark.integration
def test_remote_apply_materialize_and_teardown(remote_server, tmp_path):
    client, repo = remote_server
    now = pd.Timestamp(_utc_now()).round("ms")
    source_path = tmp_path / "new_features.parquet"
    pd.DataFrame(
        {
            "driver_id": [1001],
            "conv_rate": [0.75],
            "avg_daily_trips": [42],
            "event_timestamp": [now],
        }
    ).to_parquet(source_path)
    driver = Entity(name="driver_id", value_type=ValueType.INT64)
    view = FeatureView(
        name=NEW_FV_NAME,
        entities=[driver],
        ttl=timedelta(days=1),
        schema=[
            Field(name="conv_rate", dtype=Float32),
            Field(name="avg_daily_trips", dtype=Int64),
        ],
        source=FileSource(path=str(source_path), timestamp_field="event_timestamp"),
    )
    table = f"{PROJECT}_{NEW_FV_NAME}"
    assert table not in _server_online_tables(repo)
    client.apply([driver, view])
    assert NEW_FV_NAME in [fv.name for fv in client.list_feature_views()]
    assert table in _server_online_tables(repo)

    client.materialize(
        now - timedelta(hours=1), now + timedelta(seconds=1), [NEW_FV_NAME]
    )
    features = client.get_online_features(
        features=[f"{NEW_FV_NAME}:conv_rate", f"{NEW_FV_NAME}:avg_daily_trips"],
        entity_rows=[{"driver_id": 1001}],
    ).to_dict()
    assert features["conv_rate"] == pytest.approx([0.75])
    assert features["avg_daily_trips"] == [42]

    client.apply([], objects_to_delete=[view], partial=False)
    assert table not in _server_online_tables(repo)
    client.apply([driver, view])
    assert table in _server_online_tables(repo)
    client.teardown()
    assert table not in _server_online_tables(repo)
