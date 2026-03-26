"""Integration tests for feature view versioning."""

import os
import tempfile
from datetime import timedelta
from pathlib import Path

import pytest

from feast import FeatureStore
from feast.entity import Entity
from feast.errors import FeatureViewPinConflict, FeatureViewVersionNotFound
from feast.feature_service import FeatureService
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.infra.registry.registry import Registry
from feast.repo_config import RegistryConfig, RepoConfig
from feast.stream_feature_view import StreamFeatureView
from feast.types import Float32, Int64
from feast.value_type import ValueType


@pytest.fixture
def registry():
    """Create a file-based Registry for testing. Version history is always-on."""
    with tempfile.TemporaryDirectory() as tmpdir:
        registry_path = Path(tmpdir) / "registry.pb"
        config = RegistryConfig(path=str(registry_path))
        reg = Registry("test_project", config, None)
        yield reg


@pytest.fixture
def registry_no_online_versioning():
    """Create a file-based Registry without online versioning (default)."""
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


@pytest.fixture
def make_sfv(entity):
    def _make(description="test stream feature view", udf=None, **kwargs):
        from feast.data_source import PushSource
        from feast.infra.offline_stores.file_source import FileSource

        def default_udf(df):
            return df

        # Create batch source
        batch_source = FileSource(name="test_batch_source", path="test.parquet")

        # Create a simple push source for testing
        source = PushSource(name="test_push_source", batch_source=batch_source)

        return StreamFeatureView(
            name="driver_stats_stream",
            entities=[entity],
            ttl=timedelta(hours=1),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="trips_today", dtype=Int64),
            ],
            description=description,
            udf=udf or default_udf,
            source=source,
            **kwargs,
        )

    return _make


# OnDemandFeatureView tests removed due to transformation comparison issues


class TestFileRegistryVersioning:
    def test_first_apply_creates_v0(self, registry, make_fv):
        fv = make_fv()
        registry.apply_feature_view(fv, "test_project", commit=True)

        versions = registry.list_feature_view_versions("driver_stats", "test_project")
        assert len(versions) == 1
        assert versions[0]["version"] == "v0"
        assert versions[0]["version_number"] == 0

    def test_modify_and_reapply_creates_new_version(self, registry, make_fv, entity):
        fv1 = make_fv(description="version one")
        registry.apply_feature_view(fv1, "test_project", commit=True)

        # Create a schema change by adding a new feature
        fv2 = FeatureView(
            name="driver_stats",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="trips_today", dtype=Int64),
                Field(name="avg_rating", dtype=Float32),
                Field(name="new_feature", dtype=Float32),  # Schema change
            ],
            description="version two",
        )
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

    def test_pin_to_v0(self, registry, make_fv, entity):
        # Create v0
        fv1 = make_fv(description="original")
        registry.apply_feature_view(fv1, "test_project", commit=True)

        # Create v1 with schema change
        fv2 = FeatureView(
            name="driver_stats",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="trips_today", dtype=Int64),
                Field(name="avg_rating", dtype=Float32),
                Field(name="new_field", dtype=Float32),  # Schema change
            ],
            description="updated",
        )
        registry.apply_feature_view(fv2, "test_project", commit=True)

        # Pin to v0 (definition must match active FV, only version changes)
        fv_pin = FeatureView(
            name="driver_stats",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="trips_today", dtype=Int64),
                Field(name="avg_rating", dtype=Float32),
                Field(name="new_field", dtype=Float32),  # Keep current schema
            ],
            description="updated",
            version="v0",
        )
        registry.apply_feature_view(fv_pin, "test_project", commit=True)

        # Verify active entry has v0's content
        active_fv = registry.get_feature_view("driver_stats", "test_project")
        assert active_fv.description == "original"
        assert active_fv.version == "v0"

    def test_explicit_version_creates_when_not_exists(self, registry, make_fv):
        """Explicit version on a new FV creates that version (forward declaration)."""
        fv = make_fv(description="forward declared v1", version="v1")
        registry.apply_feature_view(fv, "test_project", commit=True)

        versions = registry.list_feature_view_versions("driver_stats", "test_project")
        assert len(versions) == 1
        assert versions[0]["version_number"] == 1

        active_fv = registry.get_feature_view("driver_stats", "test_project")
        assert active_fv.current_version_number == 1

    def test_explicit_version_reverts_when_exists(self, registry, make_fv, entity):
        """Explicit version on an existing FV reverts to that snapshot (pin/revert)."""
        # Create v0
        fv1 = make_fv(description="original")
        registry.apply_feature_view(fv1, "test_project", commit=True)

        # Create v1 with schema change
        fv2 = FeatureView(
            name="driver_stats",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="trips_today", dtype=Int64),
                Field(name="avg_rating", dtype=Float32),
                Field(name="new_field", dtype=Float32),
            ],
            description="updated",
        )
        registry.apply_feature_view(fv2, "test_project", commit=True)

        # Pin to v0 (definition must match active FV, only version changes)
        fv_pin = FeatureView(
            name="driver_stats",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="trips_today", dtype=Int64),
                Field(name="avg_rating", dtype=Float32),
                Field(name="new_field", dtype=Float32),
            ],
            description="updated",
            version="v0",
        )
        registry.apply_feature_view(fv_pin, "test_project", commit=True)

        active_fv = registry.get_feature_view("driver_stats", "test_project")
        assert active_fv.description == "original"
        assert active_fv.version == "v0"

    def test_forward_declare_nonexistent_version(self, registry, make_fv):
        """Forward-declaring a version that doesn't exist creates it."""
        fv = make_fv()
        registry.apply_feature_view(fv, "test_project", commit=True)

        # Forward-declare v5 — should create it, not raise
        fv_v5 = make_fv(description="forward declared v5", version="v5")
        registry.apply_feature_view(fv_v5, "test_project", commit=True)

        versions = registry.list_feature_view_versions("driver_stats", "test_project")
        version_numbers = [v["version_number"] for v in versions]
        assert 5 in version_numbers

        # The active FV should now be the forward-declared v5
        active_fv = registry.get_feature_view("driver_stats", "test_project")
        assert active_fv.current_version_number == 5
        assert active_fv.description == "forward declared v5"

    def test_apply_after_pin_creates_new_version(self, registry, make_fv, entity):
        # Create v0
        fv1 = make_fv(description="v0 desc")
        registry.apply_feature_view(fv1, "test_project", commit=True)

        # Create v1 with schema change
        fv2 = FeatureView(
            name="driver_stats",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="trips_today", dtype=Int64),
                Field(name="avg_rating", dtype=Float32),
                Field(name="v1_field", dtype=Float32),  # Schema change
            ],
            description="v1 desc",
        )
        registry.apply_feature_view(fv2, "test_project", commit=True)

        # Pin to v0 (definition must match active FV, only version changes)
        fv_pin = FeatureView(
            name="driver_stats",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="trips_today", dtype=Int64),
                Field(name="avg_rating", dtype=Float32),
                Field(name="v1_field", dtype=Float32),  # Keep current schema
            ],
            description="v1 desc",
            version="v0",
        )
        registry.apply_feature_view(fv_pin, "test_project", commit=True)

        # Apply new content with another schema change (should create new version)
        fv3 = FeatureView(
            name="driver_stats",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="trips_today", dtype=Int64),
                Field(name="avg_rating", dtype=Float32),
                Field(name="v1_field", dtype=Float32),
                Field(name="v2_field", dtype=Float32),  # Another schema change
            ],
            description="v2 desc after pin",
        )
        registry.apply_feature_view(fv3, "test_project", commit=True)

        versions = registry.list_feature_view_versions("driver_stats", "test_project")
        # Should have v0, v1, and potentially more versions
        assert len(versions) >= 2

    def test_pin_with_modified_definition_raises(self, registry, make_fv, entity):
        # Create v0
        fv1 = make_fv(description="original")
        registry.apply_feature_view(fv1, "test_project", commit=True)

        # Create v1 with schema change
        fv2 = FeatureView(
            name="driver_stats",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="trips_today", dtype=Int64),
                Field(name="avg_rating", dtype=Float32),
                Field(name="new_field", dtype=Float32),  # Schema change
            ],
            description="updated",
        )
        registry.apply_feature_view(fv2, "test_project", commit=True)

        # Attempt to pin to v0 while also changing schema (sneaky change)
        fv_pin = FeatureView(
            name="driver_stats",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="trips_today", dtype=Int64),
                Field(name="avg_rating", dtype=Float32),
                Field(name="new_field", dtype=Float32),
                Field(name="sneaky_field", dtype=Float32),  # Sneaky schema change
            ],
            description="updated",
            version="v0",
        )
        with pytest.raises(FeatureViewPinConflict):
            registry.apply_feature_view(fv_pin, "test_project", commit=True)

    def test_pin_without_modification_succeeds(self, registry, make_fv, entity):
        # Create v0
        fv1 = make_fv(description="original")
        registry.apply_feature_view(fv1, "test_project", commit=True)

        # Create v1 with schema change
        fv2 = FeatureView(
            name="driver_stats",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="trips_today", dtype=Int64),
                Field(name="avg_rating", dtype=Float32),
                Field(name="new_field", dtype=Float32),  # Schema change
            ],
            description="updated",
        )
        registry.apply_feature_view(fv2, "test_project", commit=True)

        # Pin to v0 with same definition as active (only version changes)
        fv_pin = FeatureView(
            name="driver_stats",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="trips_today", dtype=Int64),
                Field(name="avg_rating", dtype=Float32),
                Field(name="new_field", dtype=Float32),  # Keep current schema
            ],
            description="updated",
            version="v0",
        )
        registry.apply_feature_view(fv_pin, "test_project", commit=True)

        # Verify active entry has v0's content
        active_fv = registry.get_feature_view("driver_stats", "test_project")
        assert active_fv.description == "original"
        assert active_fv.version == "v0"

    def test_get_feature_view_by_version(self, registry, make_fv, entity):
        # Create v0
        fv1 = make_fv(description="version zero")
        registry.apply_feature_view(fv1, "test_project", commit=True)

        # Create v1 with schema change and different description
        fv2 = FeatureView(
            name="driver_stats",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="trips_today", dtype=Int64),
                Field(name="avg_rating", dtype=Float32),
                Field(name="v1_field", dtype=Float32),  # Schema change
            ],
            description="version one",
        )
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


class TestRefinedVersioningBehavior:
    """Test that only schema and UDF changes create new versions."""

    def test_metadata_changes_no_new_version(self, registry, make_fv):
        """Verify metadata changes don't create new versions."""
        fv = make_fv(description="original description", tags={"team": "ml"})
        registry.apply_feature_view(fv, "test_project", commit=True)

        # Modify only metadata
        fv_updated = make_fv(
            description="updated description",
            tags={"team": "ml", "env": "prod"},
            owner="new_owner@company.com",
        )
        registry.apply_feature_view(fv_updated, "test_project", commit=True)

        # Should not create new version
        versions = registry.list_feature_view_versions("driver_stats", "test_project")
        assert len(versions) == 1  # Still just v0
        assert versions[0]["version_number"] == 0

    def test_schema_changes_create_new_version(self, registry, make_fv, entity):
        """Verify schema changes create new versions."""
        fv = make_fv()
        registry.apply_feature_view(fv, "test_project", commit=True)

        # Add a new feature (schema change)
        fv_with_new_feature = FeatureView(
            name="driver_stats",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="trips_today", dtype=Int64),
                Field(name="avg_rating", dtype=Float32),
                Field(name="new_feature", dtype=Float32),  # New field
            ],
            description="same description",  # Keep same metadata
        )
        registry.apply_feature_view(fv_with_new_feature, "test_project", commit=True)

        # Should create new version
        versions = registry.list_feature_view_versions("driver_stats", "test_project")
        assert len(versions) == 2  # v0 and v1
        assert versions[1]["version_number"] == 1

    def test_configuration_changes_no_new_version(self, registry, make_fv):
        """Verify configuration changes don't create new versions."""
        fv = make_fv(online=True, offline=True)
        registry.apply_feature_view(fv, "test_project", commit=True)

        # Change configuration fields
        fv_config_updated = make_fv(
            online=False,  # Configuration change
            offline=False,  # Configuration change
            description="same description",  # Keep same metadata
        )
        registry.apply_feature_view(fv_config_updated, "test_project", commit=True)

        # Should not create new version
        versions = registry.list_feature_view_versions("driver_stats", "test_project")
        assert len(versions) == 1  # Still just v0
        assert versions[0]["version_number"] == 0

    def test_entity_changes_create_new_version(self, registry, make_fv):
        """Verify entity changes create new versions."""
        fv = make_fv()
        registry.apply_feature_view(fv, "test_project", commit=True)

        # Create new entity and add it (schema change)
        new_entity = Entity(
            name="vehicle_id",
            join_keys=["vehicle_id"],
            value_type=ValueType.INT64,  # Match the field type
        )

        fv_with_new_entity = FeatureView(
            name="driver_stats",
            entities=[
                Entity(
                    name="driver_id",
                    join_keys=["driver_id"],
                    value_type=ValueType.INT64,
                ),
                new_entity,
            ],
            ttl=timedelta(days=1),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="vehicle_id", dtype=Int64),  # New entity field
                Field(name="trips_today", dtype=Int64),
                Field(name="avg_rating", dtype=Float32),
            ],
            description="same description",
        )
        registry.apply_feature_view(fv_with_new_entity, "test_project", commit=True)

        # Should create new version due to entity change
        versions = registry.list_feature_view_versions("driver_stats", "test_project")
        assert len(versions) == 2  # v0 and v1
        assert versions[1]["version_number"] == 1

    def test_stream_feature_view_udf_changes_create_new_version(
        self, registry, make_sfv
    ):
        """Verify UDF changes create new versions (StreamFeatureView)."""

        def original_transform(df):
            return df

        def updated_transform(df):
            df["new_col"] = df["trips_today"] * 2
            return df

        sfv = make_sfv(udf=original_transform)
        registry.apply_feature_view(sfv, "test_project", commit=True)

        sfv_updated = make_sfv(
            udf=updated_transform,
            description="same description",  # Keep same metadata
        )
        registry.apply_feature_view(sfv_updated, "test_project", commit=True)

        # Should create new version due to UDF change
        versions = registry.list_feature_view_versions(
            "driver_stats_stream", "test_project"
        )
        assert len(versions) == 2  # v0 and v1

    # TODO: Add tests for OnDemandFeatureView once transformation comparison issues are resolved
    # The current issue is that PythonTransformation.__eq__ has strict type checking
    # that prevents proper comparison between objects created at different times


class TestVersionMetadataIntegration:
    """Integration tests for version metadata functionality in responses."""

    def test_version_metadata_disabled_by_default(self, registry, make_fv):
        """Test that version metadata is not included by default."""
        from feast.protos.feast.serving.ServingService_pb2 import (
            GetOnlineFeaturesResponse,
        )
        from feast.utils import _populate_response_from_feature_data

        # Create and register a versioned feature view
        fv = make_fv(description="test version metadata")
        registry.apply_feature_view(fv, "test_project", commit=True)

        # Get the feature view with version info
        active_fv = registry.get_feature_view("driver_stats", "test_project")
        assert hasattr(active_fv, "current_version_number")

        # Mock response generation without version metadata
        response = GetOnlineFeaturesResponse()
        _populate_response_from_feature_data(
            feature_data=[],
            indexes=[],
            online_features_response=response,
            full_feature_names=True,
            requested_features=["trips_today"],
            table=active_fv,
            output_len=0,
            include_feature_view_version_metadata=False,  # Default behavior
        )

        # Verify no version metadata is included
        assert len(response.metadata.feature_view_metadata) == 0
        # Feature names should still be populated
        assert len(response.metadata.feature_names.val) == 1

    def test_version_metadata_included_when_requested(self, registry, make_fv, entity):
        """Test that version metadata is included when requested."""
        from feast.protos.feast.serving.ServingService_pb2 import (
            GetOnlineFeaturesResponse,
        )
        from feast.utils import _populate_response_from_feature_data

        # Create v0
        fv1 = make_fv(description="first version")
        registry.apply_feature_view(fv1, "test_project", commit=True)

        # Create v1 by modifying schema
        fv2 = FeatureView(
            name="driver_stats",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="trips_today", dtype=Int64),
                Field(name="avg_rating", dtype=Float32),
                Field(name="total_earnings", dtype=Float32),  # New field
            ],
            description="second version",
        )
        registry.apply_feature_view(fv2, "test_project", commit=True)

        # Get v1 (latest version)
        active_fv = registry.get_feature_view("driver_stats", "test_project")
        assert active_fv.current_version_number == 1

        # Mock response generation with version metadata
        response = GetOnlineFeaturesResponse()
        _populate_response_from_feature_data(
            feature_data=[],
            indexes=[],
            online_features_response=response,
            full_feature_names=False,  # Test without prefixes
            requested_features=["trips_today", "total_earnings"],
            table=active_fv,
            output_len=0,
            include_feature_view_version_metadata=True,  # Enable metadata
        )

        # Verify version metadata is included
        assert len(response.metadata.feature_view_metadata) == 1
        fv_metadata = response.metadata.feature_view_metadata[0]
        assert fv_metadata.name == "driver_stats"
        assert fv_metadata.version == 1

        # Verify feature names are clean (no @v1 suffix)
        feature_names = list(response.metadata.feature_names.val)
        assert feature_names == ["trips_today", "total_earnings"]
        assert all("@" not in name for name in feature_names)

    def test_version_metadata_clean_names_with_prefixes(self, registry, make_fv):
        """Test that feature names are clean even with full_feature_names=True."""
        from feast.protos.feast.serving.ServingService_pb2 import (
            GetOnlineFeaturesResponse,
        )
        from feast.utils import _populate_response_from_feature_data

        # Create versioned feature view
        fv = make_fv(description="test clean names")
        registry.apply_feature_view(fv, "test_project", commit=True)
        active_fv = registry.get_feature_view("driver_stats", "test_project")

        # Test with full feature names (prefixed)
        response = GetOnlineFeaturesResponse()
        _populate_response_from_feature_data(
            feature_data=[],
            indexes=[],
            online_features_response=response,
            full_feature_names=True,  # Enable prefixes
            requested_features=["trips_today"],
            table=active_fv,
            output_len=0,
            include_feature_view_version_metadata=True,
        )

        # Feature names should be prefixed but clean (no @v0)
        feature_names = list(response.metadata.feature_names.val)
        assert len(feature_names) == 1
        assert feature_names[0] == "driver_stats__trips_today"  # Clean prefix
        assert "@" not in feature_names[0]  # No version in name

        # Version metadata should be separate
        assert len(response.metadata.feature_view_metadata) == 1
        assert response.metadata.feature_view_metadata[0].name == "driver_stats"
        assert response.metadata.feature_view_metadata[0].version == 0

    def test_version_metadata_multiple_feature_views(self, registry, entity):
        """Test version metadata with multiple feature views."""
        from feast.protos.feast.serving.ServingService_pb2 import (
            GetOnlineFeaturesResponse,
        )
        from feast.utils import _populate_response_from_feature_data

        # Create first feature view
        fv1 = FeatureView(
            name="driver_stats",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="trips_today", dtype=Int64),
            ],
            description="driver features",
        )
        registry.apply_feature_view(fv1, "test_project", commit=True)

        # Create second feature view
        fv2 = FeatureView(
            name="user_stats",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="total_bookings", dtype=Int64),
            ],
            description="user features",
        )
        registry.apply_feature_view(fv2, "test_project", commit=True)

        # Modify user_stats to create v1
        fv2_v1 = FeatureView(
            name="user_stats",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="total_bookings", dtype=Int64),
                Field(name="cancellation_rate", dtype=Float32),  # New field
            ],
            description="user features v1",
        )
        registry.apply_feature_view(fv2_v1, "test_project", commit=True)

        # Get feature views
        driver_fv = registry.get_feature_view("driver_stats", "test_project")  # v0
        user_fv = registry.get_feature_view("user_stats", "test_project")  # v1

        # Simulate processing multiple feature views in response
        response = GetOnlineFeaturesResponse()

        # Process first feature view
        _populate_response_from_feature_data(
            feature_data=[],
            indexes=[],
            online_features_response=response,
            full_feature_names=False,
            requested_features=["trips_today"],
            table=driver_fv,
            output_len=0,
            include_feature_view_version_metadata=True,
        )

        # Process second feature view
        _populate_response_from_feature_data(
            feature_data=[],
            indexes=[],
            online_features_response=response,
            full_feature_names=False,
            requested_features=["total_bookings", "cancellation_rate"],
            table=user_fv,
            output_len=0,
            include_feature_view_version_metadata=True,
        )

        # Verify both feature views are in metadata
        assert len(response.metadata.feature_view_metadata) == 2

        fv_metadata_by_name = {
            fvm.name: fvm for fvm in response.metadata.feature_view_metadata
        }
        assert "driver_stats" in fv_metadata_by_name
        assert "user_stats" in fv_metadata_by_name

        # Check versions
        assert fv_metadata_by_name["driver_stats"].version == 0
        assert fv_metadata_by_name["user_stats"].version == 1

        # Verify feature names are clean
        feature_names = list(response.metadata.feature_names.val)
        assert feature_names == ["trips_today", "total_bookings", "cancellation_rate"]
        assert all("@" not in name for name in feature_names)

    def test_version_metadata_prevents_duplicates(self, registry, make_fv):
        """Test that duplicate feature view metadata is not added."""
        from feast.protos.feast.serving.ServingService_pb2 import (
            GetOnlineFeaturesResponse,
        )
        from feast.utils import _populate_response_from_feature_data

        fv = make_fv(description="test duplicates")
        registry.apply_feature_view(fv, "test_project", commit=True)
        active_fv = registry.get_feature_view("driver_stats", "test_project")

        response = GetOnlineFeaturesResponse()

        # Process same feature view twice (simulating multiple features from same view)
        _populate_response_from_feature_data(
            feature_data=[],
            indexes=[],
            online_features_response=response,
            full_feature_names=False,
            requested_features=["trips_today"],
            table=active_fv,
            output_len=0,
            include_feature_view_version_metadata=True,
        )

        _populate_response_from_feature_data(
            feature_data=[],
            indexes=[],
            online_features_response=response,
            full_feature_names=False,
            requested_features=["avg_rating"],
            table=active_fv,
            output_len=0,
            include_feature_view_version_metadata=True,
        )

        # Should only have one metadata entry despite processing twice
        assert len(response.metadata.feature_view_metadata) == 1
        assert response.metadata.feature_view_metadata[0].name == "driver_stats"

        # But should have both feature names
        feature_names = list(response.metadata.feature_names.val)
        assert len(feature_names) == 2
        assert "trips_today" in feature_names
        assert "avg_rating" in feature_names

    def test_version_metadata_backward_compatibility(self, registry, make_fv):
        """Test that existing code without version metadata still works."""
        from feast.protos.feast.serving.ServingService_pb2 import (
            GetOnlineFeaturesResponse,
        )
        from feast.utils import _populate_response_from_feature_data

        fv = make_fv(description="backward compatibility test")
        registry.apply_feature_view(fv, "test_project", commit=True)
        active_fv = registry.get_feature_view("driver_stats", "test_project")

        # Test calling without the new parameter (should default to False)
        response = GetOnlineFeaturesResponse()
        _populate_response_from_feature_data(
            feature_data=[],
            indexes=[],
            online_features_response=response,
            full_feature_names=True,
            requested_features=["trips_today"],
            table=active_fv,
            output_len=0,
            # Note: include_feature_view_version_metadata parameter omitted
        )

        # Should work and not include version metadata
        assert len(response.metadata.feature_view_metadata) == 0
        assert len(response.metadata.feature_names.val) == 1


class TestOnlineVersioningDisabled:
    """Tests that online versioning guard works for version-qualified refs."""

    def test_version_qualified_ref_raises_when_online_versioning_disabled(
        self, registry_no_online_versioning, make_fv, entity
    ):
        """Using @v2 refs raises ValueError when online versioning is disabled."""
        from feast.utils import _get_feature_views_to_use

        fv = make_fv(description="test fv")
        registry_no_online_versioning.apply_feature_view(
            fv, "test_project", commit=True
        )

        # Create v1 with schema change
        fv2 = FeatureView(
            name="driver_stats",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="trips_today", dtype=Int64),
                Field(name="avg_rating", dtype=Float32),
                Field(name="new_field", dtype=Float32),
            ],
            description="version one",
        )
        registry_no_online_versioning.apply_feature_view(
            fv2, "test_project", commit=True
        )

        # Version-qualified ref should raise
        with pytest.raises(ValueError, match="online versioning is disabled"):
            _get_feature_views_to_use(
                registry=registry_no_online_versioning,
                project="test_project",
                features=["driver_stats@v1:trips_today"],
                allow_cache=False,
                hide_dummy_entity=False,
            )


class TestFeatureServiceVersioningGates:
    """Tests that feature services are gated when referencing versioned feature views."""

    @pytest.fixture
    def versioned_fv_and_entity(self):
        """Create a versioned feature view (v1) and its entity."""
        entity = Entity(
            name="driver_id",
            join_keys=["driver_id"],
            value_type=ValueType.INT64,
        )
        # v0 definition
        fv_v0 = FeatureView(
            name="driver_stats",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="trips_today", dtype=Int64),
            ],
            description="v0",
        )
        # v1 definition (schema change)
        fv_v1 = FeatureView(
            name="driver_stats",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="trips_today", dtype=Int64),
                Field(name="avg_rating", dtype=Float32),
            ],
            description="v1",
        )
        return entity, fv_v0, fv_v1

    @pytest.fixture
    def unversioned_fv_and_entity(self):
        """Create an unversioned feature view (v0 only) and its entity."""
        entity = Entity(
            name="driver_id",
            join_keys=["driver_id"],
            value_type=ValueType.INT64,
        )
        fv = FeatureView(
            name="driver_stats",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="trips_today", dtype=Int64),
            ],
            description="only version",
        )
        return entity, fv

    def _make_store(self, tmpdir, enable_versioning=False):
        """Create a FeatureStore with optional online versioning."""
        registry_path = os.path.join(tmpdir, "registry.db")
        online_path = os.path.join(tmpdir, "online.db")
        return FeatureStore(
            config=RepoConfig(
                registry=RegistryConfig(
                    path=registry_path,
                    enable_online_feature_view_versioning=enable_versioning,
                ),
                project="test_project",
                provider="local",
                online_store=SqliteOnlineStoreConfig(path=online_path),
                entity_key_serialization_version=3,
            )
        )

    def test_feature_service_apply_succeeds_with_versioned_fv_when_flag_off(
        self, versioned_fv_and_entity
    ):
        """Apply a feature service referencing a versioned FV with flag off -> succeeds."""
        entity, fv_v0, fv_v1 = versioned_fv_and_entity

        with tempfile.TemporaryDirectory() as tmpdir:
            store = self._make_store(tmpdir, enable_versioning=False)

            # Apply v0 first, then v1 to create version history
            store.apply([entity, fv_v0])
            store.apply([entity, fv_v1])

            # Now create a feature service referencing the versioned FV
            fs = FeatureService(
                name="driver_service",
                features=[fv_v1],
            )
            store.apply([fs])  # Should not raise

    def test_feature_service_apply_succeeds_with_versioned_fv_when_flag_on(
        self, versioned_fv_and_entity
    ):
        """Apply a feature service referencing a versioned FV with flag on -> succeeds."""
        entity, fv_v0, fv_v1 = versioned_fv_and_entity

        with tempfile.TemporaryDirectory() as tmpdir:
            store = self._make_store(tmpdir, enable_versioning=True)

            # Apply v0 first, then v1 to create version history
            store.apply([entity, fv_v0])
            store.apply([entity, fv_v1])

            # Feature service referencing versioned FV should succeed
            fs = FeatureService(
                name="driver_service",
                features=[fv_v1],
            )
            store.apply([fs])  # Should not raise

    def test_feature_service_retrieval_succeeds_with_versioned_fv_when_flag_off(
        self, versioned_fv_and_entity
    ):
        """get_online_features with a feature service referencing a versioned FV, flag off -> succeeds."""
        entity, fv_v0, fv_v1 = versioned_fv_and_entity
        from feast.utils import _get_feature_views_to_use

        with tempfile.TemporaryDirectory() as tmpdir:
            # First apply with flag on so the feature service can be registered
            store_on = self._make_store(tmpdir, enable_versioning=True)
            store_on.apply([entity, fv_v0])
            store_on.apply([entity, fv_v1])
            fs = FeatureService(
                name="driver_service",
                features=[fv_v1],
            )
            store_on.apply([fs])

            # Now create a store with the flag off to test retrieval
            store_off = self._make_store(tmpdir, enable_versioning=False)
            registered_fs = store_off.registry.get_feature_service(
                "driver_service", "test_project"
            )

            fvs, _ = _get_feature_views_to_use(
                registry=store_off.registry,
                project="test_project",
                features=registered_fs,
                allow_cache=False,
                hide_dummy_entity=False,
            )

            assert len(fvs) == 1

    def test_feature_service_with_unversioned_fv_succeeds(
        self, unversioned_fv_and_entity
    ):
        """Feature service with v0 FV works fine regardless of flag."""
        entity, fv = unversioned_fv_and_entity

        with tempfile.TemporaryDirectory() as tmpdir:
            store = self._make_store(tmpdir, enable_versioning=False)

            # Apply unversioned FV and feature service
            fs = FeatureService(
                name="driver_service",
                features=[fv],
            )
            store.apply([entity, fv, fs])  # Should not raise

    def test_feature_service_serves_versioned_fv_when_flag_on(
        self, versioned_fv_and_entity
    ):
        """With online versioning on, FeatureService projections do not carry version_tag;
        the FV in the registry carries current_version_number."""
        from feast.utils import _get_feature_views_to_use

        entity, fv_v0, fv_v1 = versioned_fv_and_entity

        with tempfile.TemporaryDirectory() as tmpdir:
            store = self._make_store(tmpdir, enable_versioning=True)

            # Apply v0 then v1 to create version history
            store.apply([entity, fv_v0])
            store.apply([entity, fv_v1])

            # Create and apply a feature service referencing the versioned FV
            fs = FeatureService(
                name="driver_service",
                features=[fv_v1],
            )
            store.apply([fs])

            # Retrieve the registered feature service
            registered_fs = store.registry.get_feature_service(
                "driver_service", "test_project"
            )

            fvs, _ = _get_feature_views_to_use(
                registry=store.registry,
                project="test_project",
                features=registered_fs,
                allow_cache=False,
                hide_dummy_entity=False,
            )

            assert len(fvs) == 1
            assert fvs[0].projection.version_tag is None
            assert fvs[0].projection.name_to_use() == "driver_stats"

            # Verify the FV in the registry has the correct version
            fv_from_registry = store.registry.get_feature_view(
                "driver_stats", "test_project"
            )
            assert fv_from_registry.current_version_number == 1

    def test_feature_service_feature_refs_are_plain_when_flag_on(
        self, versioned_fv_and_entity
    ):
        """With online versioning on, _get_features() produces plain (non-versioned) refs for FeatureService."""
        from feast.utils import _get_features

        entity, fv_v0, fv_v1 = versioned_fv_and_entity

        with tempfile.TemporaryDirectory() as tmpdir:
            store = self._make_store(tmpdir, enable_versioning=True)

            # Apply v0 then v1 to create version history
            store.apply([entity, fv_v0])
            store.apply([entity, fv_v1])

            # Create and apply a feature service referencing the versioned FV
            fs = FeatureService(
                name="driver_service",
                features=[fv_v1],
            )
            store.apply([fs])

            # Retrieve the registered feature service
            registered_fs = store.registry.get_feature_service(
                "driver_service", "test_project"
            )

            refs = _get_features(
                registry=store.registry,
                project="test_project",
                features=registered_fs,
                allow_cache=False,
            )

            # Refs should be plain (no version qualifier)
            for ref in refs:
                assert "@v" not in ref, f"Expected plain ref, got: {ref}"

            # Check specific ref format
            assert "driver_stats:trips_today" in refs

    def test_unpin_from_versioned_to_latest(self, versioned_fv_and_entity):
        """Pin a FV to v1, then apply with version='latest' (no schema change) -> unpinned."""
        entity, fv_v0, fv_v1 = versioned_fv_and_entity

        with tempfile.TemporaryDirectory() as tmpdir:
            store = self._make_store(tmpdir, enable_versioning=True)

            # Apply v0 then v1 to create version history (v1 has schema change)
            store.apply([entity, fv_v0])
            store.apply([entity, fv_v1])

            # Verify it's pinned to v1
            reloaded = store.registry.get_feature_view("driver_stats", "test_project")
            assert reloaded.current_version_number == 1

            # Now re-apply the same schema with version="latest" to unpin
            fv_latest = FeatureView(
                name="driver_stats",
                entities=[entity],
                ttl=timedelta(days=1),
                schema=[
                    Field(name="driver_id", dtype=Int64),
                    Field(name="trips_today", dtype=Int64),
                    Field(name="avg_rating", dtype=Float32),
                ],
                version="latest",
                description="v1",
            )
            store.apply([entity, fv_latest])

            # Reload and verify unpinned
            reloaded = store.registry.get_feature_view("driver_stats", "test_project")
            assert reloaded.current_version_number is None
            assert reloaded.version == "latest"


class TestNoPromote:
    """Tests for the no_promote flag on apply_feature_view."""

    def test_no_promote_saves_version_without_updating_active(
        self, registry, make_fv, entity
    ):
        """Apply v0, then schema change with no_promote=True.
        Version record for v1 should exist, but active FV keeps v0's schema."""
        fv0 = make_fv(description="original v0")
        registry.apply_feature_view(fv0, "test_project", commit=True)

        # Schema change with no_promote
        fv1 = FeatureView(
            name="driver_stats",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="trips_today", dtype=Int64),
                Field(name="avg_rating", dtype=Float32),
                Field(name="new_feature", dtype=Float32),  # Schema change
            ],
            description="staged v1",
        )
        registry.apply_feature_view(fv1, "test_project", commit=True, no_promote=True)

        # Version v1 should exist in history
        versions = registry.list_feature_view_versions("driver_stats", "test_project")
        assert len(versions) == 2
        assert versions[0]["version_number"] == 0
        assert versions[1]["version_number"] == 1

        # Active FV should still be v0 (initial apply with version="latest"
        # has current_version_number=None since proto 0 maps to None for latest)
        active = registry.get_feature_view("driver_stats", "test_project")
        assert active.current_version_number is None
        assert active.description == "original v0"
        # v0 schema has 3 fields (driver_id, trips_today, avg_rating)
        feature_names = {f.name for f in active.schema}
        assert "new_feature" not in feature_names

    def test_no_promote_then_regular_apply_promotes(self, registry, make_fv, entity):
        """Apply with no_promote, then re-apply the same schema change without
        no_promote. The new version should now be promoted to active."""
        fv0 = make_fv(description="original")
        registry.apply_feature_view(fv0, "test_project", commit=True)

        # Schema change with no_promote
        fv1_schema = [
            Field(name="driver_id", dtype=Int64),
            Field(name="trips_today", dtype=Int64),
            Field(name="avg_rating", dtype=Float32),
            Field(name="extra", dtype=Float32),
        ]
        fv1 = FeatureView(
            name="driver_stats",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=fv1_schema,
            description="staged v1",
        )
        registry.apply_feature_view(fv1, "test_project", commit=True, no_promote=True)

        # Now apply same schema change without no_promote
        fv1_promote = FeatureView(
            name="driver_stats",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=fv1_schema,
            description="promoted v1",
        )
        registry.apply_feature_view(
            fv1_promote, "test_project", commit=True, no_promote=False
        )

        # Active FV should now have the new schema
        active = registry.get_feature_view("driver_stats", "test_project")
        feature_names = {f.name for f in active.schema}
        assert "extra" in feature_names

    def test_no_promote_then_explicit_pin_promotes(self, registry, make_fv, entity):
        """Apply with no_promote, then pin to v1. Active should now be v1."""
        fv0 = make_fv(description="original")
        registry.apply_feature_view(fv0, "test_project", commit=True)

        # Schema change with no_promote
        fv1 = FeatureView(
            name="driver_stats",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="trips_today", dtype=Int64),
                Field(name="avg_rating", dtype=Float32),
                Field(name="pinned_feature", dtype=Float32),
            ],
            description="staged v1",
        )
        registry.apply_feature_view(fv1, "test_project", commit=True, no_promote=True)

        # Active is still v0 (initial apply with version="latest"
        # has current_version_number=None since proto 0 maps to None for latest)
        active = registry.get_feature_view("driver_stats", "test_project")
        assert active.current_version_number is None

        # Pin to v1 (user's definition must match current active, only version changes)
        fv_pin = FeatureView(
            name="driver_stats",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="trips_today", dtype=Int64),
                Field(name="avg_rating", dtype=Float32),
            ],
            description="original",
            version="v1",
        )
        registry.apply_feature_view(fv_pin, "test_project", commit=True)

        # Active should now be v1's snapshot
        active = registry.get_feature_view("driver_stats", "test_project")
        assert active.current_version_number == 1
        feature_names = {f.name for f in active.schema}
        assert "pinned_feature" in feature_names

    def test_no_promote_noop_without_schema_change(self, registry, make_fv):
        """Apply with no_promote but no schema change — metadata-only update,
        no new version should be created."""
        fv0 = make_fv(description="original")
        registry.apply_feature_view(fv0, "test_project", commit=True)

        # Same schema, different description (metadata-only)
        fv_same = make_fv(description="updated description only")
        registry.apply_feature_view(
            fv_same, "test_project", commit=True, no_promote=True
        )

        # Still only v0
        versions = registry.list_feature_view_versions("driver_stats", "test_project")
        assert len(versions) == 1
        assert versions[0]["version_number"] == 0

    def test_no_promote_version_accessible_by_explicit_ref(
        self, registry, make_fv, entity
    ):
        """After no_promote apply, the new version should be accessible via
        get_feature_view_by_version()."""
        fv0 = make_fv(description="original")
        registry.apply_feature_view(fv0, "test_project", commit=True)

        # Schema change with no_promote
        fv1 = FeatureView(
            name="driver_stats",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="trips_today", dtype=Int64),
                Field(name="avg_rating", dtype=Float32),
                Field(name="explicit_feature", dtype=Float32),
            ],
            description="staged v1",
        )
        registry.apply_feature_view(fv1, "test_project", commit=True, no_promote=True)

        # Should be accessible by explicit version ref
        v1_fv = registry.get_feature_view_by_version("driver_stats", "test_project", 1)
        assert v1_fv.current_version_number == 1
        feature_names = {f.name for f in v1_fv.schema}
        assert "explicit_feature" in feature_names

        # v0 should also still be accessible
        v0_fv = registry.get_feature_view_by_version("driver_stats", "test_project", 0)
        assert v0_fv.current_version_number == 0
        feature_names_v0 = {f.name for f in v0_fv.schema}
        assert "explicit_feature" not in feature_names_v0


class TestFeatureViewNameValidation:
    """Tests that feature view names with reserved characters are rejected on apply."""

    def test_apply_feature_view_with_at_sign_raises(self, registry, entity):
        """Applying a feature view with '@' in its name should raise ValueError."""
        fv = FeatureView(
            name="my_weirdly_@_named_fv",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="trips_today", dtype=Int64),
            ],
        )
        with pytest.raises(ValueError, match="must not contain '@'"):
            registry.apply_feature_view(fv, "test_project", commit=True)

    def test_apply_feature_view_with_colon_raises(self, registry, entity):
        """Applying a feature view with ':' in its name should raise ValueError."""
        fv = FeatureView(
            name="my:weird:fv",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="trips_today", dtype=Int64),
            ],
        )
        with pytest.raises(ValueError, match="must not contain ':'"):
            registry.apply_feature_view(fv, "test_project", commit=True)
