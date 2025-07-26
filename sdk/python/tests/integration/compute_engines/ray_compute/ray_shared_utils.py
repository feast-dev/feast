"""Shared fixtures and utilities for Ray compute engine tests."""

import os
import tempfile
import time
import uuid
from datetime import timedelta
from typing import Generator

import pandas as pd
import pytest
import ray

from feast import Entity, FileSource
from feast.data_source import DataSource
from feast.utils import _utc_now
from tests.integration.feature_repos.repo_configuration import (
    construct_test_environment,
)

from .repo_configuration import get_ray_compute_engine_test_config

now = _utc_now().replace(microsecond=0, second=0, minute=0)
today = now.replace(hour=0, minute=0, second=0, microsecond=0)


def get_test_date_range(days_back: int = 7) -> tuple:
    """Get a standard test date range (start_date, end_date) for testing."""
    end_date = now
    start_date = now - timedelta(days=days_back)
    return start_date, end_date


driver = Entity(
    name="driver_id",
    description="driver id",
)


def create_feature_dataset(ray_environment) -> DataSource:
    """Create a test dataset for feature views."""
    yesterday = today - timedelta(days=1)
    last_week = today - timedelta(days=7)
    df = pd.DataFrame(
        [
            {
                "driver_id": 1001,
                "event_timestamp": yesterday,
                "created": now - timedelta(hours=2),
                "conv_rate": 0.8,
                "acc_rate": 0.5,
                "avg_daily_trips": 15,
            },
            {
                "driver_id": 1001,
                "event_timestamp": last_week,
                "created": now - timedelta(hours=3),
                "conv_rate": 0.75,
                "acc_rate": 0.9,
                "avg_daily_trips": 14,
            },
            {
                "driver_id": 1002,
                "event_timestamp": yesterday,
                "created": now - timedelta(hours=2),
                "conv_rate": 0.7,
                "acc_rate": 0.4,
                "avg_daily_trips": 12,
            },
            {
                "driver_id": 1002,
                "event_timestamp": yesterday - timedelta(days=1),
                "created": now - timedelta(hours=2),
                "conv_rate": 0.3,
                "acc_rate": 0.6,
                "avg_daily_trips": 12,
            },
        ]
    )
    ds = ray_environment.data_source_creator.create_data_source(
        df,
        ray_environment.feature_store.project,
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )
    return ds


def create_entity_df() -> pd.DataFrame:
    """Create entity dataframe for testing."""
    entity_df = pd.DataFrame(
        [
            {"driver_id": 1001, "event_timestamp": today},
            {"driver_id": 1002, "event_timestamp": today},
        ]
    )
    return entity_df


def create_unique_sink_source(temp_dir: str, base_name: str) -> FileSource:
    """Create a unique sink source to avoid path collisions during parallel test execution."""
    timestamp = int(time.time() * 1000)
    process_id = os.getpid()
    unique_id = str(uuid.uuid4())[:8]

    # Create a unique directory for this sink - Ray needs directory paths for materialization
    sink_dir = os.path.join(
        temp_dir, f"{base_name}_{timestamp}_{process_id}_{unique_id}"
    )
    os.makedirs(sink_dir, exist_ok=True)

    return FileSource(
        name=f"{base_name}_sink_source",
        path=sink_dir,  # Use directory path - Ray will create files inside
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )


def cleanup_ray_environment(ray_environment):
    """Safely cleanup Ray environment and resources."""
    try:
        ray_environment.teardown()
    except Exception as e:
        print(f"Warning: Ray environment teardown failed: {e}")

    # Ensure Ray is shut down completely
    try:
        if ray.is_initialized():
            ray.shutdown()
        time.sleep(0.2)  # Brief pause to ensure clean shutdown
    except Exception as e:
        print(f"Warning: Ray shutdown failed: {e}")


def create_ray_environment():
    """Create Ray test environment using the standardized config."""
    ray_config = get_ray_compute_engine_test_config()
    ray_environment = construct_test_environment(
        ray_config, None, entity_key_serialization_version=3
    )
    ray_environment.setup()
    return ray_environment


@pytest.fixture(scope="function")
def ray_environment() -> Generator:
    """Pytest fixture to provide a Ray environment for tests with automatic cleanup."""
    try:
        if ray.is_initialized():
            ray.shutdown()
            time.sleep(0.2)
    except Exception:
        pass

    environment = create_ray_environment()
    yield environment
    cleanup_ray_environment(environment)


@pytest.fixture
def feature_dataset(ray_environment) -> DataSource:
    """Fixture that provides a feature dataset for testing."""
    return create_feature_dataset(ray_environment)


@pytest.fixture
def entity_df() -> pd.DataFrame:
    """Fixture that provides an entity dataframe for testing."""
    return create_entity_df()


@pytest.fixture
def temp_dir() -> Generator[str, None, None]:
    """Fixture that provides a temporary directory for test artifacts."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir
