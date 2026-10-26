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


def _registry_with_entity(version_id: str) -> RegistryProto:
    registry = RegistryProto(version_id=version_id)
    entity = Entity(name="driver", value_type=ValueType.INT64).to_proto()
    entity.spec.project = "test_project"
    registry.entities.append(entity)
    return registry


def test_reparsed_registry_with_same_version_reuses_cache() -> None:
    first_parse = _registry_with_entity("v1")
    second_parse = _registry_with_entity("v1")
    assert first_parse is not second_parse

    with patch.object(Entity, "from_proto", wraps=Entity.from_proto) as from_proto:
        list_entities(first_parse, "test_project", None)
        list_entities(second_parse, "test_project", None)
        from_proto.assert_called_once()

        list_entities(_registry_with_entity("v2"), "test_project", None)
        assert from_proto.call_count == 2


def test_registry_without_version_id_is_cached_per_object() -> None:
    first = _registry_with_entity("")
    second = _registry_with_entity("")

    with patch.object(Entity, "from_proto", wraps=Entity.from_proto) as from_proto:
        list_entities(first, "test_project", None)
        list_entities(second, "test_project", None)
        assert from_proto.call_count == 2


def test_get_feature_service_is_cached_per_version() -> None:
    from feast.feature_service import FeatureService
    from feast.infra.registry.proto_registry_utils import get_feature_service

    def registry(version_id: str) -> RegistryProto:
        proto = RegistryProto(version_id=version_id)
        service = FeatureService(name="svc", features=[]).to_proto()
        service.spec.project = "test_project"
        proto.feature_services.append(service)
        return proto

    with patch.object(
        FeatureService, "from_proto", wraps=FeatureService.from_proto
    ) as from_proto:
        first = get_feature_service(registry("v1"), "svc", "test_project")
        second = get_feature_service(registry("v1"), "svc", "test_project")
        assert first is second
        from_proto.assert_called_once()

        get_feature_service(registry("v2"), "svc", "test_project")
        assert from_proto.call_count == 2
