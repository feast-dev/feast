"""
Unit tests for the entity column type checks of `feast dbt import`.
"""

import json

import pytest
from click.testing import CliRunner

# Skip all tests in this module if dbt-artifacts-parser is not installed
pytest.importorskip("dbt_artifacts_parser", reason="dbt-artifacts-parser not installed")

from feast.cli.cli import cli  # noqa: E402

MISSING = object()


def _create_column(name: str, data_type=MISSING):
    """Helper to create a column definition, optionally without a data_type."""
    column = {"name": name, "description": "", "tags": [], "meta": {}}
    if data_type is not MISSING:
        column["data_type"] = data_type
    return column


def _create_model_node(name: str, entity_columns: dict):
    """Helper to create a model node with a timestamp, a feature and the given
    entity columns."""
    return {
        "name": name,
        "unique_id": f"model.test_project.{name}",
        "resource_type": "model",
        "package_name": "test_project",
        "path": f"models/{name}.sql",
        "original_file_path": f"models/{name}.sql",
        "fqn": ["test_project", name],
        "alias": name,
        "checksum": {"name": "sha256", "checksum": "abc123"},
        "database": "my_database",
        "schema": "analytics",
        "description": "",
        "columns": {
            "event_timestamp": _create_column("event_timestamp", "TIMESTAMP"),
            "trip_count": _create_column("trip_count", "INT64"),
            **entity_columns,
        },
        "tags": ["feast"],
        "meta": {},
        "config": {"enabled": True, "materialized": "table", "tags": []},
        "depends_on": {"nodes": [], "macros": []},
    }


def _write_manifest(tmp_path, models: dict) -> str:
    """Write a manifest with one model per entry of `models`, which maps model
    names to their entity columns, and return its path."""
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
            f"model.test_project.{name}": _create_model_node(name, entity_columns)
            for name, entity_columns in models.items()
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
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    return str(manifest_path)


def _import(manifest_path: str, entity_columns: list, *extra_args: str):
    args = ["dbt", "import", "-m", manifest_path, "-d", "file", *extra_args]
    for entity_column in entity_columns:
        args += ["-e", entity_column]
    return CliRunner().invoke(cli, args, catch_exceptions=False)


@pytest.mark.parametrize(
    "data_type, feast_type",
    [
        ("FLOAT64", "Float64"),
        ("NUMBER(38,0)", "Float64"),
        ("BOOLEAN", "Bool"),
        ("BYTES", "Bytes"),
        ("TIMESTAMP", "UnixTimestamp"),
    ],
)
def test_import_warns_on_unsuitable_entity_column_type(tmp_path, data_type, feast_type):
    manifest_path = _write_manifest(
        tmp_path,
        {"driver_stats": {"driver_id": _create_column("driver_id", data_type)}},
    )

    result = _import(manifest_path, ["driver_id"], "--dry-run")

    assert result.exit_code == 0, result.output
    assert (
        f"Warning: Model 'driver_stats' entity column 'driver_id' has type "
        f"'{data_type}' (Feast type {feast_type}), which may not be suitable "
        f"as an entity key." in result.output
    )
    # The model is still imported
    assert "driver_stats: DataSource + FeatureView" in result.output


@pytest.mark.parametrize(
    "data_type",
    [
        "STRING",
        "VARCHAR(36)",
        "INT64",
        "INTEGER",
        "NUMBER(10,0)",
        pytest.param(MISSING, id="undeclared"),
    ],
)
def test_import_accepts_string_and_integer_entity_columns(tmp_path, data_type):
    manifest_path = _write_manifest(
        tmp_path,
        {"driver_stats": {"driver_id": _create_column("driver_id", data_type)}},
    )

    result = _import(manifest_path, ["driver_id"], "--dry-run")

    assert result.exit_code == 0, result.output
    assert "Warning" not in result.output
    assert "driver_stats: DataSource + FeatureView" in result.output


def test_import_warns_only_for_unsuitable_entity_columns(tmp_path):
    manifest_path = _write_manifest(
        tmp_path,
        {
            "driver_stats": {
                "driver_id": _create_column("driver_id", "INT64"),
                "vehicle_id": _create_column("vehicle_id", "FLOAT64"),
            }
        },
    )

    result = _import(manifest_path, ["driver_id", "vehicle_id"], "--dry-run")

    assert result.exit_code == 0, result.output
    assert "entity column 'vehicle_id' has type 'FLOAT64'" in result.output
    assert "entity column 'driver_id'" not in result.output
    assert "driver_stats: DataSource + FeatureView" in result.output


def test_import_skips_models_with_array_entity_columns(tmp_path):
    manifest_path = _write_manifest(
        tmp_path,
        {
            "driver_stats": {"driver_id": _create_column("driver_id", "INT64")},
            "driver_lists": {"driver_id": _create_column("driver_id", "ARRAY<INT64>")},
        },
    )
    output_path = tmp_path / "features.py"

    result = _import(manifest_path, ["driver_id"], "--output", str(output_path))

    assert result.exit_code == 0, result.output
    assert (
        "Warning: Model 'driver_lists' has array entity column(s): driver_id. "
        "Skipping." in result.output
    )
    assert "driver_stats: DataSource + FeatureView" in result.output
    assert "driver_lists: DataSource + FeatureView" not in result.output
    assert "FeatureViews: 1" in result.output

    generated_code = output_path.read_text()
    assert "driver_stats" in generated_code
    assert "driver_lists" not in generated_code
