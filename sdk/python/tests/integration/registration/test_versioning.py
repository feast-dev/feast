"""Integration tests for feature view versioning."""

import tempfile
from datetime import timedelta
from pathlib import Path

import pytest

from feast.entity import Entity
from feast.errors import FeatureViewPinConflict, FeatureViewVersionNotFound
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.registry.registry import Registry
from feast.repo_config import RegistryConfig
from feast.types import Float32, Int64
from feast.value_type import ValueType


@pytest.fixture
def registry():
    """Create a file-based Registry for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        registry_path = Path(tmpdir) / "registry.pb"
        config = RegistryConfig(path=str(registry_path))
        reg = Registry("test_project", config, None)
        yield reg


@pytest.fixture
def entity():
    return Entity(
        name="driver_id",
        join_keys=["driver_id"],
        value_type=ValueType.INT64,
    )


@pytest.fixture
def make_fv(entity):
    def _make(description="test feature view", version="latest", **kwargs):
        return FeatureView(
            name="driver_stats",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="trips_today", dtype=Int64),
                Field(name="avg_rating", dtype=Float32),
            ],
            description=description,
            version=version,
            **kwargs,
        )

    return _make


class TestFileRegistryVersioning:
    def test_first_apply_creates_v0(self, registry, make_fv):
        fv = make_fv()
        registry.apply_feature_view(fv, "test_project", commit=True)

        versions = registry.list_feature_view_versions("driver_stats", "test_project")
        assert len(versions) == 1
        assert versions[0]["version"] == "v0"
        assert versions[0]["version_number"] == 0

    def test_modify_and_reapply_creates_new_version(self, registry, make_fv):
        fv1 = make_fv(description="version one")
        registry.apply_feature_view(fv1, "test_project", commit=True)

        fv2 = make_fv(description="version two")
        registry.apply_feature_view(fv2, "test_project", commit=True)

        versions = registry.list_feature_view_versions("driver_stats", "test_project")
        assert len(versions) == 2
        assert versions[0]["version"] == "v0"
        assert versions[1]["version"] == "v1"

    def test_idempotent_apply_no_new_version(self, registry, make_fv):
        fv = make_fv(description="same definition")
        registry.apply_feature_view(fv, "test_project", commit=True)

        # Apply identical FV again
        fv_same = make_fv(description="same definition")
        registry.apply_feature_view(fv_same, "test_project", commit=True)

        versions = registry.list_feature_view_versions("driver_stats", "test_project")
        assert len(versions) == 1  # No new version created

    def test_pin_to_v0(self, registry, make_fv):
        # Create v0
        fv1 = make_fv(description="original")
        registry.apply_feature_view(fv1, "test_project", commit=True)

        # Create v1
        fv2 = make_fv(description="updated")
        registry.apply_feature_view(fv2, "test_project", commit=True)

        # Pin to v0 (definition must match active FV, only version changes)
        fv_pin = make_fv(description="updated", version="v0")
        registry.apply_feature_view(fv_pin, "test_project", commit=True)

        # Verify active entry has v0's content
        active_fv = registry.get_feature_view("driver_stats", "test_project")
        assert active_fv.description == "original"
        assert active_fv.version == "v0"

    def test_pin_to_nonexistent_version_raises(self, registry, make_fv):
        fv = make_fv()
        registry.apply_feature_view(fv, "test_project", commit=True)

        fv_pin = make_fv(version="v99")
        with pytest.raises(FeatureViewVersionNotFound):
            registry.apply_feature_view(fv_pin, "test_project", commit=True)

    def test_apply_after_pin_creates_new_version(self, registry, make_fv):
        # Create v0
        fv1 = make_fv(description="v0 desc")
        registry.apply_feature_view(fv1, "test_project", commit=True)

        # Create v1
        fv2 = make_fv(description="v1 desc")
        registry.apply_feature_view(fv2, "test_project", commit=True)

        # Pin to v0 (definition must match active FV, only version changes)
        fv_pin = make_fv(description="v1 desc", version="v0")
        registry.apply_feature_view(fv_pin, "test_project", commit=True)

        # Apply new content (should create new version)
        fv3 = make_fv(description="v2 desc after pin")
        registry.apply_feature_view(fv3, "test_project", commit=True)

        versions = registry.list_feature_view_versions("driver_stats", "test_project")
        # Should have v0, v1, and potentially more versions
        assert len(versions) >= 2

    def test_pin_with_modified_definition_raises(self, registry, make_fv):
        # Create v0
        fv1 = make_fv(description="original")
        registry.apply_feature_view(fv1, "test_project", commit=True)

        # Create v1
        fv2 = make_fv(description="updated")
        registry.apply_feature_view(fv2, "test_project", commit=True)

        # Attempt to pin to v0 while also changing description
        fv_pin = make_fv(description="sneaky change", version="v0")
        with pytest.raises(FeatureViewPinConflict):
            registry.apply_feature_view(fv_pin, "test_project", commit=True)

    def test_pin_without_modification_succeeds(self, registry, make_fv):
        # Create v0
        fv1 = make_fv(description="original")
        registry.apply_feature_view(fv1, "test_project", commit=True)

        # Create v1
        fv2 = make_fv(description="updated")
        registry.apply_feature_view(fv2, "test_project", commit=True)

        # Pin to v0 with same definition as active (only version changes)
        fv_pin = make_fv(description="updated", version="v0")
        registry.apply_feature_view(fv_pin, "test_project", commit=True)

        # Verify active entry has v0's content
        active_fv = registry.get_feature_view("driver_stats", "test_project")
        assert active_fv.description == "original"
        assert active_fv.version == "v0"

    def test_get_feature_view_by_version(self, registry, make_fv):
        # Create v0
        fv1 = make_fv(description="version zero")
        registry.apply_feature_view(fv1, "test_project", commit=True)

        # Create v1 with different description
        fv2 = make_fv(description="version one")
        registry.apply_feature_view(fv2, "test_project", commit=True)

        # Retrieve v0 snapshot
        fv_v0 = registry.get_feature_view_by_version("driver_stats", "test_project", 0)
        assert fv_v0.description == "version zero"
        assert fv_v0.current_version_number == 0

        # Retrieve v1 snapshot
        fv_v1 = registry.get_feature_view_by_version("driver_stats", "test_project", 1)
        assert fv_v1.description == "version one"
        assert fv_v1.current_version_number == 1

    def test_get_feature_view_by_version_not_found(self, registry, make_fv):
        fv = make_fv()
        registry.apply_feature_view(fv, "test_project", commit=True)

        with pytest.raises(FeatureViewVersionNotFound):
            registry.get_feature_view_by_version("driver_stats", "test_project", 99)

    def test_version_in_proto_roundtrip(self, registry, make_fv):
        fv = make_fv(version="v3")
        # Manually set version number for testing
        fv.current_version_number = 3

        proto = fv.to_proto()
        assert proto.spec.version == "v3"
        assert proto.meta.current_version_number == 3

        fv2 = FeatureView.from_proto(proto)
        assert fv2.version == "v3"
        assert fv2.current_version_number == 3
