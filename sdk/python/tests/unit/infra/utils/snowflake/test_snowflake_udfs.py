import os
import re

import pytest

DECOMMISSIONED_RUNTIME_VERSION = "3.9"

# Snowflake requires the Python UDF RUNTIME_VERSION to match a Python runtime
# that Snowflake still supports, and Feast should never request a runtime
# below the minimum Python version Feast itself supports
# (see `requires-python` in pyproject.toml).
MIN_SUPPORTED_RUNTIME_VERSION = "3.10"

# Placeholder substituted into snowflake_python_udfs_creation.sql at deploy time by
# SnowflakeComputeEngine.update(), using
# SnowflakeComputeEngineConfig.python_udf_runtime_version. See
# https://github.com/feast-dev/feast/issues/6606 and the follow-up request to make
# the runtime version user-configurable on PR #6608.
RUNTIME_VERSION_PLACEHOLDER = "RUNTIME_VERSION_HOLDER"


def _snowpark_dir() -> str:
    import feast

    return os.path.join(
        feast.__path__[0],
        "infra",
        "utils",
        "snowflake",
        "snowpark",
    )


def _render_udf_creation_sql(runtime_version: str) -> str:
    """Reproduce the substitution SnowflakeComputeEngine.update() performs on
    snowflake_python_udfs_creation.sql, using a given runtime version, without
    needing a live Snowflake connection.
    """
    sql_path = os.path.join(_snowpark_dir(), "snowflake_python_udfs_creation.sql")
    with open(sql_path) as f:
        sql = f.read()

    sql = sql.replace("STAGE_HOLDER", "feast_stage")
    sql = sql.replace("PROJECT_NAME", "myproject")
    sql = sql.replace(RUNTIME_VERSION_PLACEHOLDER, runtime_version)
    return sql


def test_udf_creation_sql_template_uses_runtime_version_placeholder():
    """The CREATE FUNCTION statements Feast executes against Snowflake must not
    hardcode a specific (and eventually decommissioned) Python runtime version.
    Instead, the SQL template should carry a placeholder that
    SnowflakeComputeEngine.update() fills in from the user-configurable
    `python_udf_runtime_version` setting (see
    https://github.com/feast-dev/feast/issues/6606).
    """
    sql_path = os.path.join(_snowpark_dir(), "snowflake_python_udfs_creation.sql")
    with open(sql_path) as f:
        sql = f.read()

    versions = re.findall(r"RUNTIME_VERSION\s*=\s*'([^']+)'", sql)
    assert versions, (
        "expected to find RUNTIME_VERSION declarations in the UDF creation SQL"
    )
    assert DECOMMISSIONED_RUNTIME_VERSION not in versions, (
        f"snowflake_python_udfs_creation.sql still requests the decommissioned "
        f"Snowflake Python runtime {DECOMMISSIONED_RUNTIME_VERSION!r}"
    )
    assert all(v == RUNTIME_VERSION_PLACEHOLDER for v in versions), (
        "expected all RUNTIME_VERSION declarations to use the "
        f"{RUNTIME_VERSION_PLACEHOLDER!r} placeholder (filled in at deploy time "
        f"from the configurable python_udf_runtime_version), got "
        f"{sorted(set(versions))}"
    )


def test_udf_creation_sql_renders_default_runtime_version():
    """When rendered with the default `python_udf_runtime_version`
    ("3.10"), the UDF creation SQL should request Feast's default runtime and
    never the decommissioned 3.9 runtime.
    """
    from feast.infra.compute_engines.snowflake.snowflake_engine import (
        SnowflakeComputeEngineConfig,
    )

    default_runtime_version = SnowflakeComputeEngineConfig(
        database="FEAST"
    ).python_udf_runtime_version
    assert default_runtime_version == MIN_SUPPORTED_RUNTIME_VERSION

    rendered_sql = _render_udf_creation_sql(default_runtime_version)
    versions = re.findall(r"RUNTIME_VERSION\s*=\s*'([^']+)'", rendered_sql)
    assert versions, "expected to find rendered RUNTIME_VERSION declarations"
    assert DECOMMISSIONED_RUNTIME_VERSION not in versions
    assert all(v == MIN_SUPPORTED_RUNTIME_VERSION for v in versions)


def test_udf_creation_sql_renders_custom_runtime_version():
    """A custom `python_udf_runtime_version` should flow through to every
    rendered RUNTIME_VERSION declaration, so users aren't blocked on a Feast
    release the next time Snowflake decommissions a runtime.
    """
    custom_runtime_version = "3.12"
    rendered_sql = _render_udf_creation_sql(custom_runtime_version)

    versions = re.findall(r"RUNTIME_VERSION\s*=\s*'([^']+)'", rendered_sql)
    assert versions, "expected to find rendered RUNTIME_VERSION declarations"
    assert RUNTIME_VERSION_PLACEHOLDER not in versions
    assert all(v == custom_runtime_version for v in versions), (
        f"expected all RUNTIME_VERSION declarations to be "
        f"{custom_runtime_version!r}, got {sorted(set(versions))}"
    )


def test_udf_module_docstrings_do_not_reference_decommissioned_runtime_version():
    """The reference `CREATE OR REPLACE FUNCTION` docstrings embedded in
    snowflake_udfs.py document the same UDFs and should stay consistent with
    the SQL template that is actually executed.

    Note: we read the file as text rather than importing the module, since
    `snowflake_udfs.py` imports `_snowflake`, which is only available inside
    Snowflake's own Python UDF runtime.
    """
    udfs_path = os.path.join(_snowpark_dir(), "snowflake_udfs.py")
    with open(udfs_path) as f:
        source = f.read()

    versions = re.findall(r"RUNTIME_VERSION\s*=\s*'([^']+)'", source)
    assert versions, (
        "expected to find RUNTIME_VERSION declarations in snowflake_udfs.py"
    )
    assert DECOMMISSIONED_RUNTIME_VERSION not in versions, (
        f"snowflake_udfs.py docstrings still reference the decommissioned "
        f"Snowflake Python runtime {DECOMMISSIONED_RUNTIME_VERSION!r}"
    )


def test_python_udf_runtime_version_defaults_to_3_10():
    """The Snowflake compute engine config should default
    `python_udf_runtime_version` to "3.10", matching Feast's own minimum
    supported Python version, without requiring users to set it explicitly.
    """
    from feast.infra.compute_engines.snowflake.snowflake_engine import (
        SnowflakeComputeEngineConfig,
    )

    config = SnowflakeComputeEngineConfig(database="FEAST")
    assert config.python_udf_runtime_version == MIN_SUPPORTED_RUNTIME_VERSION


def test_python_udf_runtime_version_accepts_custom_value():
    """Per maintainer feedback on
    https://github.com/feast-dev/feast/pull/6608 (@ntkathole: "is it possible
    to make it user configurable?"), users must be able to override the
    runtime version Feast requests, e.g. once Snowflake decommissions 3.10.
    """
    from feast.infra.compute_engines.snowflake.snowflake_engine import (
        SnowflakeComputeEngineConfig,
    )

    config = SnowflakeComputeEngineConfig(
        database="FEAST", python_udf_runtime_version="3.11"
    )
    assert config.python_udf_runtime_version == "3.11"


@pytest.mark.parametrize("bad_version", ["", "three-ten", "3", "3.x", "3.10.", "v3.10"])
def test_python_udf_runtime_version_rejects_invalid_values(bad_version):
    """python_udf_runtime_version should validate that it looks like a Python
    version string (e.g. "3.10" or "3.11.2"), so misconfiguration is caught
    early rather than surfacing as an opaque Snowflake SQL compilation error.
    """
    from pydantic import ValidationError

    from feast.infra.compute_engines.snowflake.snowflake_engine import (
        SnowflakeComputeEngineConfig,
    )

    with pytest.raises(ValidationError):
        SnowflakeComputeEngineConfig(
            database="FEAST", python_udf_runtime_version=bad_version
        )
