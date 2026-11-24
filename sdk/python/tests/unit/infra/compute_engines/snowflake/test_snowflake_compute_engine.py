"""Regression tests for SnowflakeComputeEngine.update().

See https://github.com/feast-dev/feast/pull/6608#discussion_r3602461959: a
maintainer pointed out that once materialization UDFs were deployed for a
project, a `SHOW USER FUNCTIONS` check made `update()` return early on every
subsequent call, and the SQL template used `CREATE FUNCTION IF NOT EXISTS`.
Together, this meant a user changing `python_udf_runtime_version` (e.g. after
Snowflake decommissions a runtime) would never actually get the new runtime
version deployed -- the stale, already-deployed UDFs were left untouched.
"""

import re
import shutil
import tempfile
from typing import Tuple
from unittest.mock import MagicMock, patch

import pytest

from feast.field import Field
from feast.infra.compute_engines.snowflake.snowflake_engine import (
    SnowflakeComputeEngine,
    SnowflakeComputeEngineConfig,
)
from feast.types import Float64, Int64, String

MODULE = "feast.infra.compute_engines.snowflake.snowflake_engine"


def _make_engine(python_udf_runtime_version: str = "3.10") -> SnowflakeComputeEngine:
    repo_config = MagicMock()
    repo_config.offline_store.type = "snowflake.offline"
    repo_config.batch_engine = SnowflakeComputeEngineConfig(
        database="FEAST",
        schema="PUBLIC",
        python_udf_runtime_version=python_udf_runtime_version,
    )

    return SnowflakeComputeEngine(
        repo_config=repo_config,
        offline_store=MagicMock(),
        online_store=MagicMock(),
    )


def _show_functions_cursor(names: Tuple[str, ...]) -> MagicMock:
    cursor = MagicMock()
    name_column = MagicMock()
    name_column.name = "name"
    cursor.description = [name_column]
    cursor.fetchall.return_value = [(name,) for name in names]
    return cursor


def _run_update(
    engine: SnowflakeComputeEngine,
    mock_execute: MagicMock,
    deployed: Tuple[str, ...] = (),
) -> None:
    mock_execute.side_effect = lambda conn, query: (
        _show_functions_cursor(deployed)
        if query.startswith("SHOW USER FUNCTIONS")
        else MagicMock()
    )
    mock_conn_cm = MagicMock()
    mock_conn_cm.__enter__ = MagicMock(return_value=MagicMock())
    mock_conn_cm.__exit__ = MagicMock(return_value=False)

    def _fake_package_snowpark_zip(project_name):
        copy_path = tempfile.mkdtemp()
        return copy_path, f"{copy_path}/feast.zip"

    with (
        patch(f"{MODULE}.GetSnowflakeConnection", return_value=mock_conn_cm),
        patch(f"{MODULE}.execute_snowflake_statement", mock_execute),
        patch(f"{MODULE}.package_snowpark_zip", side_effect=_fake_package_snowpark_zip),
        patch(f"{MODULE}.shutil.rmtree", side_effect=shutil.rmtree),
    ):
        engine.update(
            project="myproject",
            views_to_delete=[],
            views_to_keep=[],
            entities_to_delete=[],
            entities_to_keep=[],
        )


def _executed(mock_execute: MagicMock) -> list:
    return [call.args[1] for call in mock_execute.call_args_list]


def _create_function_queries(mock_execute: MagicMock) -> list:
    return [q for q in _executed(mock_execute) if "CREATE FUNCTION" in q]


def test_update_creates_versioned_udfs_without_replacing():
    engine = _make_engine()
    udf_namespace = engine._udf_namespace("myproject")
    mock_execute = MagicMock()

    _run_update(engine, mock_execute)

    queries = _executed(mock_execute)
    creates = _create_function_queries(mock_execute)
    assert creates
    assert not any("OR REPLACE" in q for q in queries)
    assert all(
        f"CREATE FUNCTION IF NOT EXISTS feast_{udf_namespace}_" in q for q in creates
    )
    stage = '"FEAST"."PUBLIC"."feast_myproject"'
    assert all(
        f"IMPORTS = ('@{stage}/{udf_namespace}/feast.zip')" in q for q in creates
    )
    assert any(
        q.endswith(f"@{stage}/{udf_namespace}") for q in queries if q.startswith("PUT")
    )


def _deployed_names(engine: SnowflakeComputeEngine) -> Tuple[str, ...]:
    mock_execute = MagicMock()
    _run_update(engine, mock_execute)
    return tuple(
        name.upper()
        for q in _create_function_queries(mock_execute)
        for name in re.findall(r"IF NOT EXISTS (\w+)\(", q)
    )


def test_udf_names_match_the_deployed_udfs():
    engine = _make_engine()
    names = engine._udf_names(engine._udf_namespace("myproject"))
    assert names
    assert names == set(_deployed_names(engine))


def test_update_skips_when_the_current_set_is_deployed():
    """CREATE ... IF NOT EXISTS and PUT need CREATE/WRITE privileges even when
    nothing is created, so a deployed set must not be touched at all."""
    engine = _make_engine()
    mock_execute = MagicMock()

    _run_update(engine, mock_execute, deployed=_deployed_names(engine))

    queries = _executed(mock_execute)
    assert len(queries) == 1
    assert queries[0].startswith("SHOW USER FUNCTIONS LIKE 'FEAST_MYPROJECT_")


def test_update_skips_when_other_names_also_match():
    engine = _make_engine()
    names = _deployed_names(engine)
    mock_execute = MagicMock()

    _run_update(engine, mock_execute, deployed=(*names, "FEAST_MYPROJECT1X"))

    assert len(_executed(mock_execute)) == 1


def test_update_redeploys_a_partial_set():
    engine = _make_engine()
    mock_execute = MagicMock()

    _run_update(engine, mock_execute, deployed=_deployed_names(engine)[:-1])

    assert _create_function_queries(mock_execute)


def test_update_ignores_another_versions_set():
    old, new = _make_engine("3.10"), _make_engine("3.11")
    mock_execute = MagicMock()

    _run_update(new, mock_execute, deployed=_deployed_names(old))

    assert _create_function_queries(mock_execute)


def test_runtime_version_change_deploys_a_new_set():
    """Changing `python_udf_runtime_version` must deploy UDFs with the new
    runtime under new names, rather than reusing the stale ones."""
    old, new = _make_engine("3.10"), _make_engine("3.11")
    assert old._udf_namespace("p") != new._udf_namespace("p")
    assert old._udf_namespace("p") == _make_engine("3.10")._udf_namespace("p")
    assert re.fullmatch(r"p_[0-9a-f]{8}", old._udf_namespace("p"))

    mock_execute = MagicMock()
    _run_update(new, mock_execute)

    creates = _create_function_queries(mock_execute)
    assert all("RUNTIME_VERSION = '3.11'" in q for q in creates)
    assert all(f"feast_{new._udf_namespace('myproject')}_" in q for q in creates)


def test_materialization_query_calls_the_deployed_udfs():
    engine = _make_engine()
    mock_execute = MagicMock()
    _run_update(engine, mock_execute)
    deployed = {
        name.upper()
        for q in _create_function_queries(mock_execute)
        for name in re.findall(r"IF NOT EXISTS (\w+)\(", q)
    }

    feature_view = MagicMock()
    feature_view.batch_source.created_timestamp_column = None
    feature_view.batch_source.timestamp_field = "ts"
    feature_view.entity_columns = [Field(name="userid", dtype=Int64)]
    engine.repo_config.online_store.type = "redis"
    query = engine.generate_snowflake_materialization_query(
        engine.repo_config,
        "SELECT 1",
        feature_view,
        [Field(name="score", dtype=Float64), Field(name="label", dtype=String)],
        "myproject",
    )

    called = {name.upper() for name in re.findall(r"(feast_\w+?)\(", query, re.I)}
    assert called and called <= deployed


def test_materialization_deploys_udfs_once_per_engine():
    engine = _make_engine()
    with patch.object(engine, "update") as mock_update:
        engine._ensure_udfs("myproject")
        engine._ensure_udfs("myproject")
    mock_update.assert_called_once_with("myproject", [], [], [], [])


def test_teardown_drops_the_current_set():
    engine = _make_engine()
    mock_execute = MagicMock()
    mock_conn_cm = MagicMock()
    mock_conn_cm.__enter__ = MagicMock(return_value=MagicMock())
    mock_conn_cm.__exit__ = MagicMock(return_value=False)
    with (
        patch(f"{MODULE}.GetSnowflakeConnection", return_value=mock_conn_cm),
        patch(f"{MODULE}.execute_snowflake_statement", mock_execute),
    ):
        engine.teardown_infra(project="myproject", fvs=[], entities=[])

    drops = [q for q in _executed(mock_execute) if "DROP FUNCTION" in q]
    assert drops
    assert all(f"feast_{engine._udf_namespace('myproject')}_" in q for q in drops)


def _signature(name: str, args: str) -> Tuple[str, Tuple[str, ...]]:
    # Creates declare "df ARRAY", drops just "ARRAY"; keep only the types.
    return name.upper(), tuple(a.split()[-1].upper() for a in args.split(","))


def test_teardown_drops_every_deployed_udf():
    """The deletion SQL is maintained separately from the creation SQL; a UDF
    missing from it is silently left behind by `feast teardown`."""
    engine = _make_engine()
    create_execute = MagicMock()
    _run_update(engine, create_execute)
    created = {
        _signature(name, args)
        for q in _create_function_queries(create_execute)
        for name, args in re.findall(r"IF NOT EXISTS (\w+)\(([^)]*)\)", q)
    }

    drop_execute = MagicMock()
    mock_conn_cm = MagicMock()
    mock_conn_cm.__enter__ = MagicMock(return_value=MagicMock())
    mock_conn_cm.__exit__ = MagicMock(return_value=False)
    with (
        patch(f"{MODULE}.GetSnowflakeConnection", return_value=mock_conn_cm),
        patch(f"{MODULE}.execute_snowflake_statement", drop_execute),
    ):
        engine.teardown_infra(project="myproject", fvs=[], entities=[])
    dropped = {
        _signature(name, args)
        for q in _executed(drop_execute)
        for name, args in re.findall(r"DROP FUNCTION IF EXISTS (\w+)\(([^)]*)\)", q)
    }

    assert created
    assert created == dropped


def test_teardown_warns_and_continues_when_drop_fails():
    """Dropping needs ownership; without it, teardown must warn instead of
    raising so the rest of `feast teardown` (e.g. the registry) still runs."""
    engine = _make_engine()
    mock_conn_cm = MagicMock()
    mock_conn_cm.__enter__ = MagicMock(return_value=MagicMock())
    mock_conn_cm.__exit__ = MagicMock(return_value=False)
    denied = MagicMock(side_effect=RuntimeError("Insufficient privileges"))
    with (
        patch(f"{MODULE}.GetSnowflakeConnection", return_value=mock_conn_cm),
        patch(f"{MODULE}.execute_snowflake_statement", denied),
        pytest.warns(RuntimeWarning, match="Insufficient privileges"),
    ):
        engine.teardown_infra(project="myproject", fvs=[], entities=[])
