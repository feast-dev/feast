from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
import yaml
from click.testing import CliRunner

pytest.importorskip("dbt_artifacts_parser", reason="dbt-artifacts-parser not installed")

from feast import FeatureStore  # noqa: E402
from feast.cli.cli import cli  # noqa: E402
from feast.dbt.codegen import DbtCodeGenerator, generate_feast_code  # noqa: E402
from feast.dbt.mapper import DbtToFeastMapper  # noqa: E402
from feast.dbt.parser import DbtColumn, DbtModel  # noqa: E402


def _execute_generated(code: str) -> dict[str, Any]:
    namespace: dict[str, Any] = {}
    with pytest.warns(DeprecationWarning, match="Entity value_type will be mandatory"):
        exec(compile(code, "<dbt-generated>", "exec"), namespace)
    return namespace


@pytest.fixture
def model() -> DbtModel:
    return DbtModel(
        name="Driver-stats",
        unique_id="model.demo.Driver-stats",
        database="warehouse",
        schema="analytics",
        alias="physical_stats",
        columns=[
            DbtColumn(name="driver_id", data_type="INT64"),
            DbtColumn(name="event_timestamp", data_type="TIMESTAMP"),
            DbtColumn(name="value", data_type="INT64"),
        ],
    )


@pytest.fixture
def import_project(tmp_path: Path) -> tuple[Path, Path, Path]:
    repo = tmp_path / "repo"
    repo.mkdir()
    data = tmp_path / "warehouse data"
    data.mkdir()
    nodes = {}
    for name, value in (("driver_stats", 42), ("merchant_stats", 84)):
        pd.DataFrame(
            {
                "driver_id": [1],
                "event_timestamp": pd.to_datetime(["2026-10-14T00:00:00Z"]),
                "value": [value],
            }
        ).to_parquet(data / f"{name}.parquet")
        unique_id = f"model.demo.{name}"
        nodes[unique_id] = {
            "name": name,
            "unique_id": unique_id,
            "resource_type": "model",
            "package_name": "demo",
            "path": f"models/{name}.sql",
            "original_file_path": f"models/{name}.sql",
            "fqn": ["demo", name],
            "alias": f"physical_{name}",
            "checksum": {"name": "sha256", "checksum": "abc123"},
            "database": "demo",
            "schema": "analytics",
            "description": "",
            "columns": {
                column: {"name": column, "data_type": dtype, "description": ""}
                for column, dtype in (
                    ("driver_id", "INT64"),
                    ("event_timestamp", "TIMESTAMP"),
                    ("value", "INT64"),
                )
            },
            "tags": [],
            "meta": {},
            "config": {"enabled": True, "materialized": "table"},
            "depends_on": {"nodes": [], "macros": []},
            "refs": [],
            "sources": [],
            "metrics": [],
        }
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "metadata": {
                    "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v9.json",
                    "dbt_version": "1.5.0",
                    "generated_at": "2026-10-14T00:00:00Z",
                    "invocation_id": "test-invocation-id",
                    "env": {},
                    "adapter_type": "duckdb",
                },
                "nodes": nodes,
                **{
                    name: {}
                    for name in (
                        "sources",
                        "macros",
                        "docs",
                        "exposures",
                        "metrics",
                        "groups",
                        "selectors",
                        "disabled",
                        "parent_map",
                        "child_map",
                    )
                },
            }
        )
    )
    (repo / "feature_store.yaml").write_text(
        yaml.safe_dump(
            {
                "project": "dbt_paths",
                "provider": "local",
                "registry": str(repo / "registry.db"),
                "online_store": {"type": "sqlite", "path": str(repo / "online.db")},
                "offline_store": {"type": "file"},
                "entity_key_serialization_version": 3,
            }
        )
    )
    return repo, manifest, data


def _args(repo: Path, manifest: Path) -> list[str]:
    return [
        "-c",
        str(repo),
        "dbt",
        "import",
        "-m",
        str(manifest),
        "-e",
        "driver_id",
        "-d",
        "file",
    ]


@pytest.mark.parametrize("generated", [False, True], ids=["direct", "generated"])
@pytest.mark.parametrize("relative", [False, True], ids=["absolute", "relative"])
def test_file_template_retrieves_each_models_data(
    import_project: tuple[Path, Path, Path],
    generated: bool,
    relative: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, manifest, data = import_project
    monkeypatch.chdir(repo.parent)
    output = repo / "definitions" / "features.py"
    output.parent.mkdir()
    path_root = Path("../warehouse data") if relative else data
    args = [
        *_args(repo, manifest),
        "--file-path-template",
        str(path_root / "{model}.parquet"),
    ]
    if generated:
        args.extend(["--output", str(output)])
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 0, (result.output, repr(result.exception))

    store = FeatureStore(repo_path=str(repo))
    if generated:
        definitions = _execute_generated(output.read_text())
        store.apply(
            [
                definitions["driver_id"],
                definitions["driver_stats_source"],
                definitions["driver_stats_fv"],
                definitions["merchant_stats_source"],
                definitions["merchant_stats_fv"],
            ]
        )
    store = FeatureStore(repo_path=str(repo))
    for name, expected in (("driver_stats", 42), ("merchant_stats", 84)):
        assert store.get_feature_view(name).batch_source.path == str(
            path_root / f"{name}.parquet"
        )
        frame = store.get_historical_features(
            entity_df=pd.DataFrame(
                {
                    "driver_id": [1],
                    "event_timestamp": pd.to_datetime(["2026-10-14T01:00:00Z"]),
                }
            ),
            features=[f"{name}:value"],
        ).to_df()
        values = frame["value"].tolist()
        assert values == [expected]


@pytest.mark.parametrize("generated", [False, True], ids=["direct", "generated"])
def test_omitting_file_template_preserves_default(
    import_project: tuple[Path, Path, Path], generated: bool
) -> None:
    repo, manifest, _ = import_project
    output = repo / "features.py"
    args = _args(repo, manifest)
    if generated:
        args.extend(["--output", str(output)])
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 0, (result.output, repr(result.exception))
    if generated:
        definitions = _execute_generated(output.read_text())
        paths = [
            definitions[f"{name}_source"].path
            for name in ("driver_stats", "merchant_stats")
        ]
    else:
        store = FeatureStore(repo_path=str(repo))
        paths = [
            store.get_feature_view(name).batch_source.path
            for name in ("driver_stats", "merchant_stats")
        ]
    assert paths == ["/data/driver_stats.parquet", "/data/merchant_stats.parquet"]


@pytest.mark.parametrize(
    ("template", "expected"),
    [
        (None, "/data/Driver-stats.parquet"),
        ("/warehouse/{model}.parquet", "/warehouse/Driver-stats.parquet"),
        ("data/{model}.parquet", "data/Driver-stats.parquet"),
        (
            "s3://bucket/{model}/{model}.parquet",
            "s3://bucket/Driver-stats/Driver-stats.parquet",
        ),
        ("shared.parquet", "shared.parquet"),
        (
            "{{model}}/{model}/{{partition}}.parquet",
            "{model}/Driver-stats/{partition}.parquet",
        ),
        ("{{model:}}/{model}.parquet", "{model:}/Driver-stats.parquet"),
        (r'C:\data\"quoted"\{model}.parquet', r'C:\data\"quoted"\Driver-stats.parquet'),
        (
            "a'b\"c\\n\\t/\n\t雪😀/{model}.parquet",
            "a'b\"c\\n\\t/\n\t雪😀/Driver-stats.parquet",
        ),
    ],
)
def test_mapper_and_generated_paths_round_trip(
    model: DbtModel, template: str | None, expected: str
) -> None:
    objects = DbtToFeastMapper(
        data_source_type="file", file_path_template=template
    ).create_all_from_model(model, "driver_id")
    assert objects["data_source"].path == expected
    assert objects["feature_view"].batch_source.path == expected
    code = DbtCodeGenerator(
        data_source_type="file", file_path_template=template
    ).generate([model], "driver_id")
    definitions = _execute_generated(code)
    assert definitions["Driver_stats_source"].path == expected
    assert definitions["Driver_stats_fv"].batch_source.path == expected


@pytest.mark.parametrize("api", ["mapper", "generator", "wrapper"])
@pytest.mark.parametrize(
    "template",
    [
        "",
        "{schema}",
        "{}",
        "{0}",
        "{model.name}",
        "{model[0]}",
        "{model!r}",
        "{model:>10}",
        "{model:}",
        "{model:{schema}}",
        "{model",
        "model}",
        "{{{model}}",
    ],
)
def test_python_apis_reject_invalid_templates(api: str, template: str) -> None:
    with pytest.raises(ValueError, match="file_path_template") as error:
        if api == "mapper":
            DbtToFeastMapper(data_source_type="file", file_path_template=template)
        elif api == "generator":
            DbtCodeGenerator(data_source_type="file", file_path_template=template)
        else:
            generate_feast_code([], "driver_id", "file", file_path_template=template)
    if template:
        assert repr(template) in str(error.value)
    else:
        assert "must not be empty" in str(error.value)


@pytest.mark.parametrize("backend", ["bigquery", "snowflake"])
@pytest.mark.parametrize("template", ["", "/data/{model}.parquet"])
@pytest.mark.parametrize("api", ["mapper", "generator", "wrapper"])
def test_non_file_python_apis_reject_explicit_template(
    api: str, backend: str, template: str
) -> None:
    with pytest.raises(ValueError, match="only supported for file data sources"):
        if api == "mapper":
            DbtToFeastMapper(data_source_type=backend, file_path_template=template)
        elif api == "generator":
            DbtCodeGenerator(data_source_type=backend, file_path_template=template)
        else:
            generate_feast_code([], "driver_id", backend, file_path_template=template)


@pytest.mark.parametrize("generated", [False, True], ids=["direct", "generated"])
@pytest.mark.parametrize(
    ("backend", "template", "message"),
    [
        ("file", "", "must not be empty"),
        ("file", "{unknown}", "{unknown}"),
        ("file", "data/{model", "data/{model"),
        ("bigquery", "/data/{model}.parquet", "only supported for file data sources"),
        ("snowflake", "/data/{model}.parquet", "only supported for file data sources"),
        (None, "/data/{model}.parquet", "only supported for file data sources"),
    ],
)
def test_cli_rejects_invalid_template_before_filtering_or_writing(
    import_project: tuple[Path, Path, Path],
    generated: bool,
    backend: str | None,
    template: str,
    message: str,
) -> None:
    repo, manifest, _ = import_project
    output = repo / "features.py"
    args = _args(repo, manifest)[:-2]
    if backend is not None:
        args.extend(["-d", backend])
    args.extend(["--file-path-template", template, "--model", "does_not_exist"])
    if generated:
        output.write_text("original contents")
        args.extend(["--output", str(output)])
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 2, result.output
    assert "--file-path-template" in result.output
    assert message in result.output
    assert "No such option" not in result.output
    assert not (repo / "registry.db").exists()
    assert not (repo / "online.db").exists()
    if generated:
        assert output.read_text() == "original contents"
    else:
        assert not output.exists()


@pytest.mark.parametrize("backend", ["bigquery", "snowflake"])
def test_omitted_template_preserves_other_backends(
    model: DbtModel, backend: str
) -> None:
    source = DbtToFeastMapper(backend, "event_timestamp", 7).create_data_source(model)
    # Exercise the existing positional signature of the public convenience function.
    code = generate_feast_code(
        [model], "driver_id", backend, "event_timestamp", 7, "", "", None, False
    )
    definitions = _execute_generated(code)
    generated_source = definitions["Driver_stats_source"]
    if backend == "bigquery":
        assert (
            source.table
            == generated_source.table
            == "warehouse.analytics.physical_stats"
        )
    else:
        assert source.database == generated_source.database == "warehouse"
        assert source.schema == generated_source.schema == "analytics"
        assert source.table == generated_source.table == "physical_stats"
    assert definitions["Driver_stats_fv"].ttl.days == 7
    assert definitions["Driver_stats_fv"].online is False
