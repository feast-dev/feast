"""Pytest configuration and fixtures for Ray compute engine tests.

This module exposes fixtures from ray_shared_utils.py so they can be
auto-discovered by pytest.
"""

import pytest
import ray

from tests.component.ray.ray_shared_utils import (
    entity_df,
    feature_dataset,
    ray_environment,
    temp_dir,
)


@pytest.fixture(scope="session")
def ray_session():
    """Shared Ray session for all Ray component tests."""
    if not ray.is_initialized():
        ray.init(num_cpus=2, ignore_reinit_error=True, include_dashboard=False)
    yield ray
    ray.shutdown()


def pytest_configure(config):
    """Configure pytest for Ray tests."""
    config.addinivalue_line("markers", "ray: mark test as requiring Ray compute engine")


__all__ = [
    "entity_df",
    "feature_dataset",
    "ray_environment",
    "ray_session",
    "temp_dir",
    "pytest_configure",
]
