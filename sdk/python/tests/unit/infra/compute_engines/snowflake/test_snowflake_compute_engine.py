"""Regression tests for SnowflakeComputeEngine.update().

See https://github.com/feast-dev/feast/pull/6608#discussion_r3602461959: a
maintainer pointed out that once materialization UDFs were deployed for a
project, a `SHOW USER FUNCTIONS` check made `update()` return early on every
subsequent call, and the SQL template used `CREATE FUNCTION IF NOT EXISTS`.
Together, this meant a user changing `python_udf_runtime_version` (e.g. after
Snowflake decommissions a runtime) would never actually get the new runtime
version deployed -- the stale, already-deployed UDFs were left untouched.

The fix removes the early return and switches the SQL template to
`CREATE OR REPLACE FUNCTION`, so `update()` always (re)deploys the UDFs using
the currently configured runtime version.
"""

import shutil
import tempfile
from unittest.mock import MagicMock, patch

from feast.infra.compute_engines.snowflake.snowflake_engine import (
    SnowflakeComputeEngine,
    SnowflakeComputeEngineConfig,
)

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


def _run_update(engine: SnowflakeComputeEngine, mock_execute: MagicMock) -> None:
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


def test_update_never_short_circuits_on_existing_functions():
    """update() must not query `SHOW USER FUNCTIONS` and skip deployment --
    that early-return path is what caused stale runtime versions to stick
    around after a config change.
    """
    engine = _make_engine()
    mock_execute = MagicMock()

    _run_update(engine, mock_execute)

    executed_queries = [call.args[1] for call in mock_execute.call_args_list]
    assert not any("SHOW USER FUNCTIONS" in q for q in executed_queries), (
        "update() should no longer perform a SHOW USER FUNCTIONS check that "
        "causes it to skip (re)deploying the UDFs"
    )
    assert any("CREATE OR REPLACE FUNCTION" in q for q in executed_queries), (
        "update() should deploy UDFs using CREATE OR REPLACE FUNCTION so "
        "redeploys pick up a changed python_udf_runtime_version"
    )


def test_update_redeploys_with_current_runtime_version_on_every_call():
    """Calling update() twice (simulating two `feast apply` runs, e.g. before
    and after a user bumps `python_udf_runtime_version`) must deploy the UDFs
    both times, each time using whatever runtime version is currently
    configured.
    """
    engine = _make_engine(python_udf_runtime_version="3.11")
    mock_execute = MagicMock()

    _run_update(engine, mock_execute)
    first_call_count = mock_execute.call_count
    assert first_call_count > 0

    _run_update(engine, mock_execute)
    second_call_count = mock_execute.call_count
    assert second_call_count == 2 * first_call_count, (
        "the second update() call should re-run the full deployment (no "
        "early return), just like the first"
    )

    executed_queries = [call.args[1] for call in mock_execute.call_args_list]
    create_function_queries = [
        q for q in executed_queries if "CREATE OR REPLACE FUNCTION" in q
    ]
    assert create_function_queries, "expected CREATE OR REPLACE FUNCTION queries"
    assert all("RUNTIME_VERSION = '3.11'" in q for q in create_function_queries), (
        "every deployed UDF should use the currently configured "
        "python_udf_runtime_version ('3.11'), including on redeploy"
    )
