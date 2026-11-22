"""Unit tests for the timestamps of a SavedDataset that is re-applied."""

from datetime import datetime, timezone

import pytest

from feast.infra.offline_stores.file_source import SavedDatasetFileStorage
from feast.infra.registry.registry import Registry
from feast.repo_config import RegistryConfig
from feast.saved_dataset import SavedDataset

PROJECT = "test_project"


def _day(day):
    return datetime(2026, 1, day)


def _saved_dataset(min_event_timestamp=None, max_event_timestamp=None):
    dataset = SavedDataset(
        name="ds",
        features=["fv:f1"],
        join_keys=["e_id"],
        storage=SavedDatasetFileStorage(path="ds.parquet"),
    )
    dataset.min_event_timestamp = min_event_timestamp
    dataset.max_event_timestamp = max_event_timestamp
    return dataset


@pytest.fixture
def registry(tmp_path):
    config = RegistryConfig(path=str(tmp_path / "registry.db"), cache_ttl_seconds=0)
    return Registry(PROJECT, config, None)


@pytest.mark.parametrize("tzinfo", [None, timezone.utc])
def test_reapplied_saved_dataset_stores_its_new_event_range(registry, tzinfo):
    registry.apply_saved_dataset(_saved_dataset(_day(1), _day(3)), PROJECT)
    registry.apply_saved_dataset(
        _saved_dataset(
            _day(11).replace(tzinfo=tzinfo), _day(13).replace(tzinfo=tzinfo)
        ),
        PROJECT,
    )

    stored = registry.get_saved_dataset("ds", PROJECT)
    assert stored.min_event_timestamp == _day(11)
    assert stored.max_event_timestamp == _day(13)


def test_reapplied_saved_dataset_without_event_range_keeps_the_stored_one(registry):
    registry.apply_saved_dataset(_saved_dataset(_day(1), _day(3)), PROJECT)
    registry.apply_saved_dataset(_saved_dataset(), PROJECT)

    stored = registry.get_saved_dataset("ds", PROJECT)
    assert stored.min_event_timestamp == _day(1)
    assert stored.max_event_timestamp == _day(3)


def test_reapplied_saved_dataset_does_not_invent_an_event_range(registry):
    registry.apply_saved_dataset(_saved_dataset(), PROJECT)
    registry.apply_saved_dataset(_saved_dataset(), PROJECT)

    stored = registry.get_saved_dataset("ds", PROJECT)
    assert stored.min_event_timestamp is None
    assert stored.max_event_timestamp is None


def test_saved_dataset_stores_last_updated_timestamp(registry):
    registry.apply_saved_dataset(_saved_dataset(_day(1), _day(3)), PROJECT)
    first = registry.get_saved_dataset("ds", PROJECT)
    assert first.last_updated_timestamp is not None

    registry.apply_saved_dataset(_saved_dataset(_day(1), _day(3)), PROJECT)
    second = registry.get_saved_dataset("ds", PROJECT)
    assert second.created_timestamp == first.created_timestamp
    assert second.last_updated_timestamp >= first.last_updated_timestamp
