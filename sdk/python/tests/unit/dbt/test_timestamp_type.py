"""`feast dbt import` warns when the timestamp field is not a timestamp type."""

import json

import pytest
from click.testing import CliRunner

pytest.importorskip("dbt_artifacts_parser", reason="dbt-artifacts-parser not installed")

from feast.cli.cli import cli  # noqa: E402

MISSING = object()


def _column(name, data_type):
    column = {
        "name": name,
        "description": "",
        "meta": {},
        "constraints": [],
        "quote": None,
        "tags": [],
    }
    if data_type is not MISSING:
        column["data_type"] = data_type
    return column


def _manifest(tmp_path, timestamp_type):
    manifest = {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v9.json",
            "dbt_version": "1.5.0",
            "generated_at": "2024-01-10T00:00:00Z",
            "invocation_id": "test-invocation-id",
            "env": {},
            "adapter_type": "bigquery",
        },
        "nodes": {
            "model.test_project.driver_stats": {
                "database": "test_db",
                "schema": "analytics",
                "name": "driver_stats",
                "resource_type": "model",
                "package_name": "test_project",
                "path": "models/driver_stats.sql",
                "original_file_path": "models/driver_stats.sql",
                "unique_id": "model.test_project.driver_stats",
                "fqn": ["test_project", "driver_stats"],
                "alias": "driver_stats",
                "checksum": {"name": "sha256", "checksum": "abc"},
                "config": {"enabled": True, "materialized": "table", "tags": []},
                "tags": ["feast"],
                "description": "",
                "columns": {
                    "driver_id": _column("driver_id", "INT64"),
                    "event_timestamp": _column("event_timestamp", timestamp_type),
                    "trip_count": _column("trip_count", "INT64"),
                },
                "meta": {},
                "depends_on": {"macros": [], "nodes": []},
            }
        },
        "sources": {},
        "macros": {},
        "docs": {},
        "exposures": {},
        "metrics": {},
        "groups": {},
        "selectors": {},
        "disabled": {},
        "parent_map": {},
        "child_map": {},
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    return str(path)


def _import_dry_run(manifest_path):
    return CliRunner().invoke(
        cli,
        [
            "dbt",
            "import",
            "-m",
            manifest_path,
            "-e",
            "driver_id",
            "-d",
            "file",
            "--dry-run",
        ],
        catch_exceptions=False,
    )


@pytest.mark.parametrize(
    "timestamp_type, reported_type",
    [
        ("STRING", "STRING"),
        ("INT64", "INT64"),
        pytest.param(MISSING, "STRING", id="undeclared"),
        pytest.param(None, "STRING", id="null"),
    ],
)
def test_import_warns_when_timestamp_field_is_not_a_timestamp(
    tmp_path, timestamp_type, reported_type
):
    result = _import_dry_run(_manifest(tmp_path, timestamp_type))

    assert result.exit_code == 0, result.output
    assert (
        f"Warning: Model 'driver_stats' timestamp field 'event_timestamp' "
        f"has type '{reported_type}', which does not map to a timestamp type. "
        f"Importing anyway." in result.output
    )
    assert "driver_stats: DataSource + FeatureView" in result.output


@pytest.mark.parametrize("timestamp_type", ["TIMESTAMP", "TIMESTAMP_NTZ(9)"])
def test_import_accepts_timestamp_types_silently(tmp_path, timestamp_type):
    result = _import_dry_run(_manifest(tmp_path, timestamp_type))

    assert result.exit_code == 0, result.output
    assert "Warning" not in result.output
    assert "driver_stats: DataSource + FeatureView" in result.output
