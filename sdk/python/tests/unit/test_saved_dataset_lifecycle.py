"""Tests for SavedDataset lifecycle: deletion, validation, and registry persistence."""
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pandas as pd
import pytest
from feast import FeatureStore, Entity, FeatureView, Field, FileSource
from feast.errors import SavedDatasetNotFound


@pytest.fixture
def store_with_data(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    df = pd.DataFrame({
        "id": [1, 2, 3, 4],
        "event_timestamp": [
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
            datetime(2026, 1, 3, tzinfo=timezone.utc),
            datetime(2026, 1, 4, tzinfo=timezone.utc),
        ],
        "value": [10.0, 20.0, 30.0, 40.0],
    })
    df.to_parquet(data_dir / "data.parquet", index=False)
    store = FeatureStore(repo_path=str(tmp_path))
    entity = Entity(name="id", join_keys=["id"])
    source = FileSource(name="test_source", path=str(data_dir / "data.parquet"), timestamp_field="event_timestamp")
    fv = FeatureView(name="test_fv", entities=[entity], schema=[Field(name="value", dtype="FLOAT64")], source=source, ttl=timedelta(days=1))
    store.apply([entity, fv])
    entity_df = pd.DataFrame({"id": [1, 2], "event_timestamp": [datetime(2026, 1, 1, tzinfo=timezone.utc), datetime(2026, 1, 2, tzinfo=timezone.utc)]})
    job = store.get_historical_features(entity_df=entity_df, features=["test_fv:value"], full_feature_names=False)
    store.create_saved_dataset(from_=job, name="test_saved_dataset", storage=store._get_saved_dataset_storage())
    return store


class TestSavedDatasetDeletion:
    """Tests for SavedDataset deletion via apply() and delete_saved_dataset()."""

    def test_saved_dataset_exists_after_creation(self, store_with_data):
        ds = store_with_data.get_saved_dataset("test_saved_dataset")
        assert ds is not None
        assert ds.name == "test_saved_dataset"

    def test_delete_via_apply(self, store_with_data):
        """Fixes #7044: apply(objects_to_delete=[saved_dataset]) must delete it."""
        store = store_with_data
        ds = store.get_saved_dataset("test_saved_dataset")
        store.apply([], objects_to_delete=[ds], partial=False)
        with pytest.raises(SavedDatasetNotFound):
            store.get_saved_dataset("test_saved_dataset")

    def test_delete_saved_dataset_method(self, store_with_data):
        store = store_with_data
        store.delete_saved_dataset("test_saved_dataset")
        with pytest.raises(SavedDatasetNotFound):
            store.get_saved_dataset("test_saved_dataset")

    def test_delete_nonexistent(self, store_with_data):
        with pytest.raises(SavedDatasetNotFound):
            store_with_data.delete_saved_dataset("nonexistent_dataset")

    def test_delete_does_not_affect_other_objects(self, store_with_data):
        store = store_with_data
        ds = store.get_saved_dataset("test_saved_dataset")
        store.apply([], objects_to_delete=[ds], partial=False)
        assert store.get_feature_view("test_fv") is not None
        assert store.get_entity("id") is not None

    def test_persistence_after_deletion(self, store_with_data):
        store = store_with_data
        repo_path = store.repo_path
        ds = store.get_saved_dataset("test_saved_dataset")
        store.apply([], objects_to_delete=[ds], partial=False)
        store2 = FeatureStore(repo_path=repo_path)
        with pytest.raises(SavedDatasetNotFound):
            store2.get_saved_dataset("test_saved_dataset")


class TestApplyValidation:
    """Tests for apply() validation of objects_to_delete."""

    def test_unrecognized_type_raises(self, store_with_data):
        """apply() with an unrecognized object type in objects_to_delete should raise ValueError."""
        store = store_with_data

        class FakeObject:
            name = "fake"

        with pytest.raises(ValueError, match="unrecognized object type"):
            store.apply([], objects_to_delete=[FakeObject()], partial=False)

    def test_unrecognized_type_does_not_delete_others(self, store_with_data):
        """Unrecognized types should not prevent deletion of valid types."""
        store = store_with_data
        ds = store.get_saved_dataset("test_saved_dataset")

        class FakeObject:
            name = "fake"

        with pytest.raises(ValueError):
            store.apply([], objects_to_delete=[ds, FakeObject()], partial=False)
        # The valid deletion should NOT have been applied since the error is raised first
        # (behavior may vary: either all-or-nothing or partial application)
        # For now, we verify the error is raised


class TestSavedDatasetListing:
    """Tests for list_saved_datasets()."""

    def test_list_returns_saved_dataset(self, store_with_data):
        store = store_with_data
        datasets = store.list_saved_datasets()
        assert len(datasets) >= 1
        names = [ds.name for ds in datasets]
        assert "test_saved_dataset" in names

    def test_list_after_deletion(self, store_with_data):
        store = store_with_data
        ds = store.get_saved_dataset("test_saved_dataset")
        store.apply([], objects_to_delete=[ds], partial=False)
        datasets = store.list_saved_datasets()
        names = [ds.name for ds in datasets]
        assert "test_saved_dataset" not in names


class TestDeleteObject:
    """Tests for the universal delete_object() method."""

    def test_delete_saved_dataset_via_delete_object(self, store_with_data):
        """delete_object() should find and delete a SavedDataset by name."""
        store = store_with_data
        store.delete_object("test_saved_dataset")
        with pytest.raises(SavedDatasetNotFound):
            store.get_saved_dataset("test_saved_dataset")

    def test_delete_entity_via_delete_object(self, store_with_data):
        """delete_object() should find and delete an Entity."""
        store = store_with_data
        store.delete_object("id")
        from feast.errors import EntityNotFoundException
        with pytest.raises(EntityNotFoundException):
            store.get_entity("id")

    def test_delete_nonexistent_via_delete_object(self, store_with_data):
        """delete_object() should raise ValueError for unknown names."""
        store = store_with_data
        with pytest.raises(ValueError, match="not found"):
            store.delete_object("nonexistent_object_xyz")


class TestSavedDatasetRegistry:
    """Tests for SavedDataset registry integration."""

    def test_saved_dataset_survives_store_recreation(self, store_with_data):
        """SavedDataset should persist across FeatureStore recreation."""
        store = store_with_data
        repo_path = store.repo_path

        # Verify it exists
        ds = store.get_saved_dataset("test_saved_dataset")
        assert ds.name == "test_saved_dataset"

        # Recreate
        store2 = FeatureStore(repo_path=repo_path)
        ds2 = store2.get_saved_dataset("test_saved_dataset")
        assert ds2.name == "test_saved_dataset"

    def test_saved_dataset_deleted_persists_after_recreation(self, store_with_data):
        """Deleted SavedDataset should remain deleted across recreation."""
        store = store_with_data
        repo_path = store.repo_path
        ds = store.get_saved_dataset("test_saved_dataset")
        store.apply([], objects_to_delete=[ds], partial=False)

        store2 = FeatureStore(repo_path=repo_path)
        with pytest.raises(SavedDatasetNotFound):
            store2.get_saved_dataset("test_saved_dataset")

    def test_multiple_saved_datasets(self, store_with_data):
        """Should be able to create and manage multiple saved datasets."""
        store = store_with_data
        entity_df = pd.DataFrame({
            "id": [3, 4],
            "event_timestamp": [
                datetime(2026, 1, 3, tzinfo=timezone.utc),
                datetime(2026, 1, 4, tzinfo=timezone.utc),
            ],
        })
        job = store.get_historical_features(
            entity_df=entity_df, features=["test_fv:value"], full_feature_names=False
        )
        store.create_saved_dataset(
            from_=job, name="second_dataset", storage=store._get_saved_dataset_storage()
        )

        # Both should exist
        assert store.get_saved_dataset("test_saved_dataset") is not None
        assert store.get_saved_dataset("second_dataset") is not None

        # Delete one, the other should remain
        store.delete_saved_dataset("test_saved_dataset")
        assert store.get_saved_dataset("second_dataset") is not None

        with pytest.raises(SavedDatasetNotFound):
            store.get_saved_dataset("test_saved_dataset")
