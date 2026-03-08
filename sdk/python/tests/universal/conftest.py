"""Universal test fixtures.

Load shared integration fixtures for universal tests.
After moving tests from `integration/*` to `universal/*`, pytest no longer
discovers `tests/integration/conftest.py` through parent-directory traversal.
"""

pytest_plugins = ["tests.integration.conftest"]
