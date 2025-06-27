from feast.infra.registry.proto_registry_utils import list_saved_datasets
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.protos.feast.core.SavedDataset_pb2 import SavedDataset as SavedDatasetProto
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetSpec,
    SavedDatasetStorage,
)
from feast.saved_dataset import SavedDataset


class TestRegistryProto:
    """Test class for proto_registry_utils functions"""

    def test_list_saved_datasets_uses_spec_tags(self):
        """Test that list_saved_datasets correctly uses saved_dataset.spec.tags for tag filtering"""
        registry = RegistryProto()
        registry.version_id = "test_version"
        saved_dataset = SavedDatasetProto()
        spec = SavedDatasetSpec()
        spec.name = "test_dataset"
        spec.project = "test_project"
        spec.features.extend(["feature1", "feature2"])
        spec.join_keys.extend(["entity1"])
        spec.full_feature_names = False
        spec.tags["environment"] = "production"
        spec.tags["team"] = "ml-team"
        storage = SavedDatasetStorage()
        file_options = storage.file_storage
        file_options.uri = "test_path.parquet"
        spec.storage.CopyFrom(storage)

        saved_dataset.spec.CopyFrom(spec)
        registry.saved_datasets.append(saved_dataset)

        # Test that filtering by tags works correctly using spec.tags
        result = list_saved_datasets(
            registry, "test_project", {"environment": "production"}
        )

        assert len(result) == 1
        assert isinstance(result[0], SavedDataset)
        assert result[0].name == "test_dataset"
        assert result[0].tags == {"environment": "production", "team": "ml-team"}

        # Test that non-matching tags filter correctly
        result = list_saved_datasets(
            registry, "test_project", {"environment": "staging"}
        )
        assert len(result) == 0

        # Test that multiple tag filtering works
        result = list_saved_datasets(
            registry, "test_project", {"environment": "production", "team": "ml-team"}
        )
        assert len(result) == 1
        assert result[0].name == "test_dataset"
