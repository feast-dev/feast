import os
import subprocess
import sys
import time
from pathlib import Path
from textwrap import dedent
from typing import Iterator

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

from feast.cli import cli
from tests.utils.cli_repo_creator import CliRunner
from tests.utils.http_server import free_port

pytestmark = pytest.mark.integration


@pytest.fixture
def offline_push_repo(tmp_path: Path) -> tuple[Path, Path]:
    repo_path = tmp_path / "feature_repo"
    data_path = repo_path / "data"
    data_path.mkdir(parents=True)

    (repo_path / "feature_store.yaml").write_text(
        dedent(
            """
            project: batching_repro
            registry: data/registry.db
            provider: local
            online_store:
              type: sqlite
              path: data/online_store.db
            feature_server:
              type: local
              offline_push_batching_enabled: true
              offline_push_batching_batch_size: 100
              offline_push_batching_batch_interval_seconds: 1
            auth:
              type: no_auth
            """
        ).lstrip()
    )
    (repo_path / "feature_definitions.py").write_text(
        dedent(
            """
            from datetime import timedelta

            from feast import Entity, FeatureView, Field, FileSource, Project, PushSource
            from feast.types import Float32
            from feast.value_type import ValueType

            project = Project(name="batching_repro")
            driver = Entity(
                name="driver", join_keys=["driver_id"], value_type=ValueType.INT64
            )
            batch_source = FileSource(
                name="driver_features_source",
                path="data/driver_features.parquet",
                timestamp_field="event_timestamp",
            )
            push_source = PushSource(
                name="driver_push_source", batch_source=batch_source
            )
            driver_features = FeatureView(
                name="driver_features",
                entities=[driver],
                ttl=timedelta(days=1),
                schema=[Field(name="feature_value", dtype=Float32)],
                online=True,
                source=push_source,
            )
            """
        ).lstrip()
    )
    pq.write_table(
        pa.table(
            {
                "driver_id": pa.array([], type=pa.int64()),
                "event_timestamp": pa.array([], type=pa.timestamp("us")),
                "feature_value": pa.array([], type=pa.float32()),
            }
        ),
        data_path / "driver_features.parquet",
    )

    result = CliRunner().run(["apply"], cwd=repo_path)
    assert result.returncode == 0, (
        f"feast apply failed\nstdout:\n{result.stdout.decode()}\n"
        f"stderr:\n{result.stderr.decode()}"
    )

    return repo_path, data_path / "driver_features.parquet"


@pytest.fixture
def running_feature_server(
    offline_push_repo: tuple[Path, Path], tmp_path: Path
) -> Iterator[tuple[str, Path]]:
    repo_path, offline_path = offline_push_repo
    port = free_port()
    log_path = tmp_path / "feature-server.log"
    log_file = log_path.open("w+")

    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(cli.__file__).resolve().parents[2])
    process = subprocess.Popen(
        [
            sys.executable,
            cli.__file__,
            "-c",
            str(repo_path),
            "serve",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--workers",
            "2",
            "--registry_ttl_sec",
            "0",
            "--no-access-log",
        ],
        env=env,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        text=True,
    )

    endpoint = f"http://127.0.0.1:{port}"
    try:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if process.poll() is not None:
                break
            try:
                response = requests.get(f"{endpoint}/health", timeout=1)
            except requests.RequestException:
                time.sleep(0.1)
            else:
                if response.status_code == 200:
                    break
        else:
            pytest.fail("feature server did not become healthy within 30 seconds")

        if process.poll() is not None:
            log_file.flush()
            pytest.fail(
                f"feature server exited with code {process.returncode}:\n"
                f"{log_path.read_text()}"
            )

        yield endpoint, offline_path
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
        log_file.close()


def test_low_volume_offline_push_flushes_after_interval(
    running_feature_server: tuple[str, Path],
):
    endpoint, offline_path = running_feature_server
    response = requests.post(
        f"{endpoint}/push",
        json={
            "push_source_name": "driver_push_source",
            "df": {
                "driver_id": [1001],
                "event_timestamp": ["2026-09-13T00:00:00"],
                "feature_value": [1.25],
            },
            "to": "offline",
        },
        timeout=10,
    )
    assert response.status_code == 202

    deadline = time.monotonic() + 5
    rows = 0
    while time.monotonic() < deadline:
        try:
            rows = len(pd.read_parquet(offline_path))
        except (OSError, pa.ArrowException):
            rows = 0
        if rows == 1:
            break
        time.sleep(0.1)

    assert rows == 1
