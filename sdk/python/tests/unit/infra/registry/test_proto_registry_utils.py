from typing import Callable
from unittest.mock import patch

import pytest

from feast import Entity, FeatureView, FileSource
from feast.infra.registry import proto_registry_utils
from feast.infra.registry.proto_registry_utils import (
    get_any_feature_view,
    list_entities,
    list_project_metadata,
    list_saved_datasets,
)
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


def _registry(marker: str) -> RegistryProto:
    # SqlRegistry leaves version_id empty, so only the proto object tells them apart.
    proto = RegistryProto()
    proto.project_metadata.add(project="test_project", project_uuid=marker)
    entity = Entity(name="driver", value_type=ValueType.INT64, description=marker)
    proto.entities.append(entity.to_proto())
    proto.entities[0].spec.project = "test_project"
    view = FeatureView(
        name="driver_stats",
        source=FileSource(path="driver.parquet"),
        description=marker,
    )
    proto.feature_views.append(view.to_proto())
    proto.feature_views[0].spec.project = "test_project"
    return proto


@pytest.mark.parametrize(
    "read",
    [
        lambda proto: list_project_metadata(proto, "test_project")[0].project_uuid,
        lambda proto: list_entities(proto, "test_project", None)[0].description,
        lambda proto: (
            get_any_feature_view(proto, "driver_stats", "test_project").description
        ),
    ],
    ids=[
        "registry_proto_cache",
        "registry_proto_cache_with_tags",
        "registry_proto_cache_name",
    ],
)
def test_registries_that_share_an_id_are_not_served_from_each_others_cache(
    monkeypatch: pytest.MonkeyPatch, read: Callable[[RegistryProto], str]
) -> None:
    # Give both protos the same id(), as CPython does when a freed address is reused.
    monkeypatch.setattr(proto_registry_utils, "id", lambda _proto: 0, raising=False)
    first, second = _registry("old"), _registry("new")

    assert read(first) == "old"
    assert read(second) == "new"


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
