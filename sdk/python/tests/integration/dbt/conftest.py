"""
Conftest for dbt integration tests.

This is a standalone conftest that doesn't depend on the main Feast test infrastructure.
"""

import pytest

# This conftest is minimal and doesn't import the main feast conftest
# to avoid complex dependency chains for dbt-specific tests
