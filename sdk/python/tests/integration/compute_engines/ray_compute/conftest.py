"""Pytest configuration and fixtures for Ray compute engine tests.

This module exposes fixtures from ray_shared_utils.py so they can be
auto-discovered by pytest.
"""

from tests.integration.compute_engines.ray_compute.ray_shared_utils import (
    entity_df,
    feature_dataset,
    ray_environment,
    temp_dir,
)


def pytest_configure(config):
    """Configure pytest for Ray tests."""
    config.addinivalue_line("markers", "ray: mark test as requiring Ray compute engine")


__all__ = [
    "entity_df",
    "feature_dataset",
    "ray_environment",
    "temp_dir",
    "pytest_configure",
]
