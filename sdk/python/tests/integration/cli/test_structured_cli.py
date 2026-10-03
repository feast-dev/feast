"""Local subprocess contract tests: never merge stdout and stderr."""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
import yaml

from feast.cli import cli as cli_module


def run_cli(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ, FEAST_USAGE="False", PYTHONDONTWRITEBYTECODE="1")
    env["PYTHONPATH"] = os.pathsep.join(path for path in sys.path if path)
    return subprocess.run(
        [sys.executable, cli_module.__file__, "-c", str(repo), *args],
        cwd=repo,
        env=env,
        capture_output=True,
        text=True,
        timeout=90,
    )


def payload(result: subprocess.CompletedProcess[str]) -> dict[str, Any]:
    assert result.returncode == 0, (result.stdout, result.stderr)
    assert "\x1b" not in result.stdout
    assert "incidental" not in result.stdout
    return yaml.safe_load(result.stdout)


@pytest.mark.integration
def test_local_structured_plan_apply_and_reads(tmp_path: Path) -> None:
    pd.DataFrame(
        {
            "driver_id": [1],
            "conv_rate": [0.5],
            "event_timestamp": [datetime(2026, 1, 1, tzinfo=timezone.utc)],
        }
    ).to_parquet(tmp_path / "source.parquet")
    (tmp_path / "feature_store.yaml").write_text(
        "project: agent_test\nprovider: local\nregistry: data/registry.db\n"
        "online_store:\n  type: sqlite\n  path: data/online.db\n"
        "offline_store:\n  type: file\nentity_key_serialization_version: 3\n"
    )
    (tmp_path / "definitions.py").write_text(
        "import os, sys\n"
        "print('incidental Python stdout')\n"
        "os.write(1, b'incidental native stdout\\n')\n"
        "print('diagnostic on stderr', file=sys.stderr)\n"
        "from datetime import timedelta\n"
        "from feast import Entity, FeatureView, FeatureService, Field, FileSource\n"
        "from feast.types import Float32\n"
        "from feast.value_type import ValueType\n"
        "driver = Entity(name='driver', join_keys=['driver_id'], value_type=ValueType.INT64)\n"
        "source = FileSource(name='driver_source', path='source.parquet', timestamp_field='event_timestamp')\n"
        "driver_stats = FeatureView(name='driver_stats', entities=[driver], ttl=timedelta(days=1), "
        "schema=[Field(name='conv_rate', dtype=Float32)], source=source)\n"
        "service = FeatureService(name='driver_service', features=[driver_stats])\n"
    )
    planned = run_cli(tmp_path, "--output", "json", "plan")
    changes = payload(planned)["data"]["projects"][0]
    assert changes["changed"] is True
    assert "diagnostic on stderr" in planned.stderr
    # Planning may open/create SQLite while inspecting existing infrastructure,
    # but must not deploy the feature table described by the plan.
    with sqlite3.connect(tmp_path / "data/online.db") as connection:
        tables = connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    assert not any("driver_stats" in name for (name,) in tables)

    applied = payload(run_cli(tmp_path, "--output", "json", "apply"))
    assert applied["data"]["projects"][0]["changed"] is True
    assert applied["data"]["projects"][0]["infrastructure_changes"]
    unchanged = payload(run_cli(tmp_path, "--output", "yaml", "apply"))
    assert unchanged["data"]["projects"][0]["changed"] is False

    listed = payload(run_cli(tmp_path, "--output", "json", "entities", "list"))
    assert any(item["name"] == "driver" for item in listed["data"]["items"])
    described = payload(
        run_cli(tmp_path, "--output", "yaml", "entities", "describe", "driver")
    )
    assert described["data"]["join_keys"] == ["driver_id"]
    missing = run_cli(tmp_path, "--output", "json", "entities", "describe", "missing")
    assert missing.returncode == 1
    assert json.loads(missing.stdout)["error"]["code"] == "NOT_FOUND"

    legacy = run_cli(tmp_path, "entities", "list")
    assert legacy.returncode == 0
    assert "NAME" in legacy.stdout
    assert "driver" in legacy.stdout
    for resource, name in [
        ("feature-views", "driver_stats"),
        ("feature-services", "driver_service"),
        ("data-sources", "driver_source"),
    ]:
        result = payload(
            run_cli(tmp_path, "--output", "json", resource, "describe", name)
        )
        assert result["data"]["name"] == name
    features = payload(run_cli(tmp_path, "--output", "json", "features", "list"))
    assert features["data"]["items"][0]["feature_name"] == "conv_rate"
    assert features["data"]["items"][0]["feature_view"] == "driver_stats"


@pytest.mark.integration
@pytest.mark.parametrize("command", ["plan", "apply"])
def test_unsupported_provider_does_not_import_repository(
    tmp_path: Path, command: str
) -> None:
    (tmp_path / "feature_store.yaml").write_text(
        "project: agent_test\nprovider: gcp\nregistry: data/registry.db\n"
        "online_store:\n  type: sqlite\n  path: data/online.db\n"
        "offline_store:\n  type: file\nentity_key_serialization_version: 3\n"
    )
    (tmp_path / "definitions.py").write_text(
        "from pathlib import Path\nPath('imported').write_text('unsafe')\n"
    )
    result = run_cli(tmp_path, "--output", "json", command)
    assert result.returncode == 1, (result.stdout, result.stderr)
    assert json.loads(result.stdout)["error"]["code"] == "UNSUPPORTED_CAPABILITY"
    assert not (tmp_path / "imported").exists()


@pytest.mark.integration
def test_subprocess_discovery_and_usage_without_config(tmp_path: Path) -> None:
    assert payload(run_cli(tmp_path, "--output", "json", "commands"))["data"][
        "commands"
    ]
    invalid = run_cli(tmp_path, "--output", "yaml", "entities", "describe")
    assert invalid.returncode == 2
    assert yaml.safe_load(invalid.stdout)["error"]["code"] == "INVALID_ARGUMENT"
    assert "Usage:" not in invalid.stdout
