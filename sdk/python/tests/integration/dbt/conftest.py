"""
Conftest for dbt integration tests.

This is a standalone conftest that doesn't depend on the main Feast test infrastructure.
"""

from pathlib import Path

import pytest

# This conftest is minimal and doesn't import the main feast conftest
# to avoid complex dependency chains for dbt-specific tests

# Path to the test dbt project manifest
TEST_DBT_PROJECT_DIR = Path(__file__).parent / "test_dbt_project"
TEST_MANIFEST_PATH = TEST_DBT_PROJECT_DIR / "target" / "manifest.json"


def pytest_collection_modifyitems(config, items):  # noqa: ARG001
    """
    Skip dbt integration tests if manifest.json doesn't exist.

    These tests require running 'dbt build' first to generate the manifest.
    The dbt-integration-test workflow handles this, but regular unit test
    runs don't, so we skip them to avoid failures.
    """
    if not TEST_MANIFEST_PATH.exists():
        skip_marker = pytest.mark.skip(
            reason="dbt manifest.json not found - run 'dbt build' first or use dbt-integration-test workflow"
        )
        for item in items:
            item.add_marker(skip_marker)
