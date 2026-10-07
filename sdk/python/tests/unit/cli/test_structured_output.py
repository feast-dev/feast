from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock, patch

import click
import pytest
import yaml
from click.testing import CliRunner

from feast import Entity, FeatureService, FeatureView, Field, FileSource
from feast.cli.cli import cli
from feast.cli.discovery import command_inventory
from feast.cli.output import _requested_output, invocation
from feast.cli.structured_read import describe_object
from feast.diff.infra_diff import InfraDiff
from feast.diff.property_diff import PropertyDiff, TransitionType
from feast.diff.registry_diff import FeastObjectDiff, RegistryDiff
from feast.errors import FeastObjectNotFoundException, FeastProviderLoginError
from feast.infra.registry.registry import FeastObjectType
from feast.operation_report import OperationReport, StructuredOperationUnsupported
from feast.repo_operations import _check_structured_operation
from feast.types import Float32
from feast.value_type import ValueType


@pytest.mark.parametrize("output", ["json", "yaml"])
def test_version_envelope(output: str) -> None:
    result = CliRunner().invoke(cli, ["--output", output, "version"])
    assert result.exit_code == 0, result.output
    body = yaml.safe_load(result.output)
    assert body["schema_version"] == "1"
    assert body["command"] == "feast version"
    assert body["status"] == "success"
    assert body["data"]["version"]
    assert body["error"] is None
    assert "\x1b" not in result.output
    assert invocation.get() is None


def test_discovery_without_store(tmp_path: Path) -> None:
    with patch("feast.cli.structured_read.create_feature_store") as create:
        result = CliRunner().invoke(
            cli, ["--output", "json", "-c", str(tmp_path), "commands"]
        )
    assert result.exit_code == 0, result.output
    create.assert_not_called()
    entries = json.loads(result.output)["data"]["commands"]
    paths = [entry["command"] for entry in entries]
    assert "feast entities describe" in paths
    assert "feast dbt import" in paths
    if "feast mlflow sync-dataset" in paths:
        sync = next(
            entry
            for entry in entries
            if entry["command"] == "feast mlflow sync-dataset"
        )
        assert sync["capabilities"]["effect"] == "write"
        assert sync["capabilities"]["structured_output"] is False
    for entry in entries:
        if not entry["group"]:
            assert entry["capabilities"]["effect"] != "unknown", entry
    describe = next(
        entry for entry in entries if entry["command"] == "feast entities describe"
    )
    assert describe["parameters"][0]["required"] is True
    delete = next(
        entry for entry in entries if entry["command"] == "feast projects delete"
    )
    assert delete["capabilities"]["may_delete_resources"] is True
    assert delete["capabilities"]["confirmation"] == "--yes"
    assert command_inventory(cli) == command_inventory(cli)


@pytest.mark.parametrize(
    "args",
    [
        ["entities", "describe"],
        ["entities", "list", "--unknown"],
        ["unknown-command"],
        ["--log-level", "not-a-level", "version"],
    ],
)
def test_parse_errors_are_structured(args: list[str]) -> None:
    result = CliRunner().invoke(cli, ["--output", "json", *args])
    assert result.exit_code == 2, result.output
    assert json.loads(result.output)["error"]["code"] == "INVALID_ARGUMENT"


@pytest.mark.parametrize(
    "args", [["teardown"], ["delete", "driver"], ["materialize"], ["init"]]
)
def test_unsupported_commands_do_not_execute(args: list[str]) -> None:
    with (
        patch("feast.cli.cli.load_repo_config") as load,
        patch("feast.cli.cli.init_repo") as init,
    ):
        result = CliRunner().invoke(cli, ["--output", "json", *args])
    assert result.exit_code == 2, result.output
    assert json.loads(result.output)["error"]["code"] == "UNSUPPORTED_OUTPUT"
    load.assert_not_called()
    init.assert_not_called()


def test_missing_configuration(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        cli, ["--output", "json", "-c", str(tmp_path), "entities", "list"]
    )
    assert result.exit_code == 1, result.output
    assert json.loads(result.output)["error"]["code"] == "CONFIGURATION_ERROR"


@pytest.mark.parametrize(
    "error,code",
    [
        (FeastObjectNotFoundException("secret"), "NOT_FOUND"),
        (FeastProviderLoginError("secret"), "AUTHENTICATION_FAILED"),
        (PermissionError("secret"), "PERMISSION_DENIED"),
        (RuntimeError("password=secret"), "OPERATION_FAILED"),
        (click.Abort(), "CANCELLED"),
        (KeyboardInterrupt(), "CANCELLED"),
    ],
)
def test_safe_errors(error: BaseException, code: str) -> None:
    with patch("feast.cli.structured_read.create_feature_store", side_effect=error):
        result = CliRunner().invoke(
            cli, ["--output", "json", "entities", "describe", "missing"]
        )
    assert result.exit_code == 1, result.output
    assert json.loads(result.output)["error"]["code"] == code
    assert "secret" not in result.output


def feature_store() -> Mock:
    store = Mock()
    store.list_batch_feature_views.return_value = [
        SimpleNamespace(
            name="driver_stats",
            features=[SimpleNamespace(name="conv_rate", dtype="Float32")],
        )
    ]
    store.list_on_demand_feature_views.return_value = []
    store.list_stream_feature_views.return_value = []
    return store


def test_legacy_json_name_mapping() -> None:
    with patch("feast.cli.features.create_feature_store", return_value=feature_store()):
        result = CliRunner().invoke(cli, ["features", "list", "--output", "json"])
    assert result.exit_code == 0
    assert json.loads(result.output) == [
        {
            "feature_name": "conv_rate",
            "feature_view": "driver_stats",
            "dtype": "Float32",
        }
    ]


def test_structured_features_and_precedence() -> None:
    with patch(
        "feast.cli.structured_read.create_feature_store", return_value=feature_store()
    ):
        result = CliRunner().invoke(
            cli, ["--output", "yaml", "features", "list", "--output", "json"]
        )
    assert result.exit_code == 0, result.output
    assert (
        yaml.safe_load(result.output)["data"]["items"][0]["feature_name"] == "conv_rate"
    )


@pytest.mark.parametrize(
    "args",
    [
        [
            "get-online-features",
            "--entities",
            "malformed",
            "--features",
            "view:feature",
        ],
        [
            "get-online-features",
            "-e",
            "id=1",
            "-e",
            "id=2",
            "-e",
            "other=3",
            "-f",
            "v:f",
        ],
        ["get-historical-features"],
        ["get-historical-features", "--dataframe", "invalid", "-f", "v:f"],
        ["get-historical-features", "--start-date", "invalid", "-f", "v:f"],
    ],
)
def test_invalid_input_fails_before_store_creation(args: list[str]) -> None:
    with patch("feast.cli.features.create_feature_store") as create:
        result = CliRunner().invoke(cli, args)
    assert result.exit_code == 2, result.output
    create.assert_not_called()


@pytest.mark.parametrize(
    "command,operation", [("plan", "plan"), ("apply", "apply_total")]
)
def test_provider_login_failure_is_not_success(command: str, operation: str) -> None:
    with (
        patch("feast.cli.cli.cli_check_repo"),
        patch("feast.cli.cli.load_repo_config"),
        patch(
            f"feast.cli.cli.{operation}",
            side_effect=FeastProviderLoginError("login failed"),
        ),
    ):
        result = CliRunner().invoke(cli, [command])
    assert result.exit_code == 1
    assert "login failed" in result.output


def test_partial_apply_reports_uncertainty() -> None:
    def fail(*args: Any, report: OperationReport, **kwargs: Any) -> None:
        report.mutation_started = True
        raise RuntimeError("credential=secret")

    with (
        patch("feast.cli.cli.cli_check_repo"),
        patch("feast.cli.cli.load_repo_config"),
        patch("feast.cli.cli.apply_total", side_effect=fail),
    ):
        result = CliRunner().invoke(cli, ["--output", "json", "apply"])
    assert result.exit_code == 1, result.output
    body = json.loads(result.output)
    assert body["data"]["remaining_outcome"] == "unknown"
    assert body["error"]["retry_safe"] is False
    assert "secret" not in result.output


def test_incidental_prints_do_not_contaminate_output() -> None:
    def create(ctx: click.Context) -> Mock:
        print("password=secret")
        store = Mock()
        store.list_entities.return_value = []
        return store

    with patch("feast.cli.structured_read.create_feature_store", side_effect=create):
        result = CliRunner().invoke(cli, ["--output", "json", "entities", "list"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["data"] == {"items": []}
    assert "secret" not in result.output


def test_legacy_invocation_after_structured_invocation() -> None:
    runner = CliRunner()
    runner.invoke(cli, ["--output", "json", "entities", "describe"])
    result = runner.invoke(cli, ["version"])
    assert result.exit_code == 0
    assert "Feast SDK Version" in result.output
    assert invocation.get() is None


@pytest.mark.parametrize("args", [[], ["entities"], ["entities", "list"]])
def test_explicit_help(args: list[str]) -> None:
    result = CliRunner().invoke(cli, ["--output", "json", *args, "--help"])
    assert result.exit_code == 0, result.output
    assert "Usage:" in result.output
    assert "schema_version" not in result.output


def test_help_as_argument_does_not_bypass_guard() -> None:
    with patch("feast.cli.cli.create_feature_store") as create:
        result = CliRunner().invoke(cli, ["--output", "json", "delete", "--", "--help"])
    assert result.exit_code == 2, result.output
    assert json.loads(result.output)["error"]["code"] == "UNSUPPORTED_OUTPUT"
    create.assert_not_called()


@pytest.mark.parametrize(
    "args,expected",
    [
        (["--output", "json", "version"], "json"),
        (["--output=YAML", "version"], "yaml"),
        (["dbt", "import", "--output", "json"], None),
        (["-c", "--output", "version"], None),
        (["--output", "json", "--output", "yaml", "version"], "yaml"),
        (["--", "--output", "json"], None),
    ],
)
def test_root_option_detection(args: list[str], expected: str | None) -> None:
    assert _requested_output(args) == expected


def real_objects() -> tuple[Entity, FileSource, FeatureView, FeatureService]:
    entity = Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64)
    source = FileSource(
        name="source", path="password-secret.parquet", timestamp_field="event_timestamp"
    )
    view = FeatureView(
        name="driver_stats",
        entities=[entity],
        ttl=timedelta(hours=1),
        schema=[Field(name="conv_rate", dtype=Float32)],
        source=source,
        tags={"api_key": "secret"},
    )
    service = FeatureService(name="driver_service", features=[view])
    return entity, source, view, service


@pytest.mark.parametrize(
    "resource", ["entities", "data-sources", "feature-views", "feature-services"]
)
@pytest.mark.parametrize("action", ["list", "describe"])
@pytest.mark.parametrize("output", ["json", "yaml"])
def test_pilot_commands_with_real_objects(
    resource: str, action: str, output: str
) -> None:
    entity, source, view, service = real_objects()
    objects = {
        "entities": entity,
        "data-sources": source,
        "feature-views": view,
        "feature-services": service,
    }
    obj = objects[resource]
    store = Mock()
    for kind, value in [
        ("entity", entity),
        ("data_source", source),
        ("feature_view", view),
        ("feature_service", service),
    ]:
        getattr(store, f"get_{kind}").return_value = value
        plural = "entities" if kind == "entity" else kind + "s"
        getattr(store, f"list_{plural}").return_value = [value]
    store.list_batch_feature_views.return_value = [view]
    store.list_on_demand_feature_views.return_value = []
    args = ["--output", output, resource, action]
    if action == "describe":
        args.append(obj.name)
    with patch("feast.cli.structured_read.create_feature_store", return_value=store):
        result = CliRunner().invoke(cli, args)
    assert result.exit_code == 0, result.output
    data = yaml.safe_load(result.output)["data"]
    item = data["items"][0] if action == "list" else data
    assert item["name"] == obj.name
    assert "secret" not in result.output
    assert "api_key" not in result.output
    if resource == "entities":
        assert item["join_keys"] == ["driver_id"]
    if resource == "feature-views":
        assert item["ttl_seconds"] == 3600
        assert item["online"] is True
    if resource == "feature-services":
        assert item["feature_references"] == ["driver_stats:conv_rate"]


def test_pinned_feature_service_references() -> None:
    _, _, _, service = real_objects()
    service.feature_view_projections[0].version_tag = 2
    assert describe_object(service)["feature_references"] == [
        "driver_stats@v2:conv_rate"
    ]


def test_report_excludes_sensitive_values() -> None:
    registry = RegistryDiff()
    registry.add_feast_object_diff(
        FeastObjectDiff(
            "source",
            FeastObjectType.DATA_SOURCE,
            None,
            None,
            [PropertyDiff("connection", "old-secret", "new-secret")],
            TransitionType.UPDATE,
        )
    )
    report = OperationReport()
    report.record("project", registry, InfraDiff())
    serialized = json.dumps(report.projects)
    assert "secret" not in serialized
    assert report.projects[0]["changed"] is True
    assert report.projects[0]["registry_changes"][0]["changed_fields"] == ["connection"]


def test_auto_baseline_is_rejected() -> None:
    config = Mock(provider="local", online_store=Mock(type="sqlite"))
    config.data_quality_monitoring_config.auto_baseline = True
    with pytest.raises(StructuredOperationUnsupported):
        _check_structured_operation(config)


def test_serialization_failure_is_structured() -> None:
    with patch(
        "feast.cli.structured_read.read_command", return_value={"bad": object()}
    ):
        result = CliRunner().invoke(cli, ["--output", "json", "entities", "list"])
    assert result.exit_code == 1, result.output
    assert json.loads(result.output)["error"]["code"] == "OPERATION_FAILED"


def test_exit_before_result_is_not_success() -> None:
    with patch("feast.cli.structured_read.read_command", side_effect=SystemExit(0)):
        result = CliRunner().invoke(cli, ["--output", "json", "entities", "list"])
    assert result.exit_code == 1
    assert json.loads(result.output)["status"] == "error"


def test_click_exit_before_result_is_not_success() -> None:
    with patch(
        "feast.cli.structured_read.read_command", side_effect=click.exceptions.Exit(0)
    ):
        result = CliRunner().invoke(cli, ["--output", "json", "entities", "list"])
    assert result.exit_code == 1
    assert json.loads(result.output)["status"] == "error"


def test_tags_and_ordering() -> None:
    store = Mock()
    first, _, _, _ = real_objects()
    second = Entity(name="alpha", value_type=ValueType.INT64)
    store.list_entities.return_value = [first, second]
    with patch("feast.cli.structured_read.create_feature_store", return_value=store):
        result = CliRunner().invoke(
            cli, ["--output", "json", "entities", "list", "--tags", "team:ml"]
        )
    assert result.exit_code == 0, result.output
    store.list_entities.assert_called_once_with(tags={"team": "ml"})
    assert [item["name"] for item in json.loads(result.output)["data"]["items"]] == [
        "alpha",
        "driver",
    ]


def test_invalid_yaml_is_safe(tmp_path: Path) -> None:
    (tmp_path / "feature_store.yaml").write_text("project: [password=secret")
    result = CliRunner().invoke(
        cli, ["--output", "json", "-c", str(tmp_path), "entities", "list"]
    )
    assert result.exit_code == 1, result.output
    assert json.loads(result.output)["error"]["code"] == "CONFIGURATION_ERROR"
    assert "secret" not in result.output
