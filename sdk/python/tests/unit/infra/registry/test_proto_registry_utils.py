from unittest.mock import patch

from feast import Entity
from feast.infra.registry.proto_registry_utils import list_entities, list_saved_datasets
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.protos.feast.core.SavedDataset_pb2 import SavedDataset as SavedDatasetProto
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetSpec,
    SavedDatasetStorage,
)
from feast.saved_dataset import SavedDataset
from feast.value_type import ValueType


def test_equivalent_tag_filters_reuse_cached_entities() -> None:
    registry = RegistryProto(version_id="test_version")
    entity = Entity(
        name="driver",
        value_type=ValueType.INT64,
        tags={"environment": "production", "team": "ml"},
    ).to_proto()
    entity.spec.project = "test_project"
    registry.entities.append(entity)

    with patch.object(Entity, "from_proto", wraps=Entity.from_proto) as from_proto:
        first = list_entities(
            registry, "test_project", {"environment": "production", "team": "ml"}
        )
        second = list_entities(
            registry, "test_project", {"team": "ml", "environment": "production"}
        )

        assert [obj.name for obj in first] == ["driver"]
        assert [obj.name for obj in second] == ["driver"]
        from_proto.assert_called_once()


def test_a_new_registry_that_reuses_a_freed_id_is_not_served_from_cache() -> None:
    def registry(name: str) -> RegistryProto:
        # SqlRegistry leaves version_id empty, so only the proto tells them apart.
        proto = RegistryProto()
        entity = Entity(name=name, value_type=ValueType.INT64).to_proto()
        entity.spec.project = "test_project"
        proto.entities.append(entity)
        return proto

    first = registry("old")
    assert [obj.name for obj in list_entities(first, "test_project", None)] == ["old"]
    del first
    second = registry("new")
    assert [obj.name for obj in list_entities(second, "test_project", None)] == ["new"]


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
