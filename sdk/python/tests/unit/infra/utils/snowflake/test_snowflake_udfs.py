import os
import re

DECOMMISSIONED_RUNTIME_VERSION = "3.9"

# Snowflake requires the Python UDF RUNTIME_VERSION to match a Python runtime
# that Snowflake still supports, and Feast should never request a runtime
# below the minimum Python version Feast itself supports
# (see `requires-python` in pyproject.toml).
MIN_SUPPORTED_RUNTIME_VERSION = "3.10"


def _snowpark_dir() -> str:
    import feast

    return os.path.join(
        feast.__path__[0],
        "infra",
        "utils",
        "snowflake",
        "snowpark",
    )


def test_udf_creation_sql_does_not_use_decommissioned_runtime_version():
    """Snowflake decommissioned the Python 3.9 UDF runtime, so the
    CREATE FUNCTION statements Feast executes against Snowflake must not
    request it (see https://github.com/feast-dev/feast/issues/6606).
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
    assert all(v == MIN_SUPPORTED_RUNTIME_VERSION for v in versions), (
        f"expected all RUNTIME_VERSION declarations to be "
        f"{MIN_SUPPORTED_RUNTIME_VERSION!r}, got {sorted(set(versions))}"
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
