"""
Unit tests for dual registration functionality in FeatureStore.

Tests that online_enabled=True FeatureViews get automatically registered
as both batch FeatureViews and OnDemandFeatureViews for serving.
"""

from unittest.mock import Mock, patch

from feast.entity import Entity
from feast.feature_store import FeatureStore
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.transformation.base import Transformation, transformation
from feast.types import Float64


class TestDualRegistration:
    """Test dual registration functionality"""

    def test_online_enabled_creates_odfv(self):
        """Test that online_enabled=True creates an OnDemandFeatureView"""
        # Create a FeatureView with online_enabled=True
        driver = Entity(name="driver", join_keys=["driver_id"])
        mock_source = FileSource(path="test.parquet", timestamp_field="ts")

        # Create transformation
        test_transformation = Transformation(
            mode="python", udf=lambda x: x, udf_string="lambda x: x"
        )

        fv = FeatureView(
            name="test_fv",
            source=mock_source,
            entities=[driver],
            schema=[Field(name="feature1", dtype=Float64)],
            feature_transformation=test_transformation,
            when="on_write",
            online_enabled=True,
        )

        # Mock registry and provider
        mock_registry = Mock()
        mock_provider = Mock()

        # Create FeatureStore instance with mocked initialization
        with patch.object(FeatureStore, "__init__", return_value=None):
            fs = FeatureStore()
            fs._registry = mock_registry
            fs._provider = mock_provider
            fs.config = Mock()
            fs.config.project = "test_project"

        # Mock the validation and inference methods to avoid complex setup
        fs._validate_all_feature_views = Mock()
        fs._make_inferences = Mock()

        # Track what gets applied to registry
        applied_views = []

        def capture_feature_view(view, project, commit):
            applied_views.append(view)

        mock_registry.apply_feature_view.side_effect = capture_feature_view
        mock_registry.apply_entity = Mock()
        mock_registry.apply_data_source = Mock()
        mock_registry.apply_feature_service = Mock()
        mock_registry.apply_validation_reference = Mock()
        mock_registry.apply_permission = Mock()
        mock_registry.commit = Mock()

        # Mock provider methods
        mock_provider.update_infra = Mock()
        mock_provider.teardown_infra = Mock()

        # Apply the FeatureView
        fs.apply(fv)

        # Verify that 2 feature views were applied: original FV + generated ODFV
        assert len(applied_views) == 2

        # Find the original FV and generated ODFV
        original_fv = None
        generated_odfv = None

        for view in applied_views:
            if isinstance(view, FeatureView) and not isinstance(
                view, OnDemandFeatureView
            ):
                original_fv = view
            elif isinstance(view, OnDemandFeatureView):
                generated_odfv = view

        # Verify original FV
        assert original_fv is not None
        assert original_fv.name == "test_fv"
        assert original_fv.online_enabled
        assert original_fv.feature_transformation is not None

        # Verify generated ODFV
        assert generated_odfv is not None
        assert generated_odfv.name == "test_fv_online"
        assert generated_odfv.feature_transformation is not None
        assert generated_odfv.feature_transformation.udf == test_transformation.udf
        assert "generated_from" in generated_odfv.tags
        assert generated_odfv.tags["generated_from"] == "test_fv"
        assert generated_odfv.tags["dual_registration"] == "true"

    def test_no_dual_registration_when_online_disabled(self):
        """Test that online_enabled=False does not create ODFV"""
        driver = Entity(name="driver", join_keys=["driver_id"])
        mock_source = FileSource(path="test.parquet", timestamp_field="ts")

        fv = FeatureView(
            name="test_fv",
            source=mock_source,
            entities=[driver],
            schema=[Field(name="feature1", dtype=Float64)],
            online_enabled=False,  # Disabled
        )

        # Mock FeatureStore
        # Create FeatureStore instance with mocked initialization
        with patch.object(FeatureStore, "__init__", return_value=None):
            fs = FeatureStore()
            fs.config = Mock()
            fs.config.project = "test_project"
        fs._registry = Mock()
        fs._provider = Mock()
        fs._validate_all_feature_views = Mock()
        fs._make_inferences = Mock()

        applied_views = []
        fs._registry.apply_feature_view.side_effect = (
            lambda view, project, commit: applied_views.append(view)
        )
        fs._registry.apply_entity = Mock()
        fs._registry.apply_data_source = Mock()
        fs._registry.apply_feature_service = Mock()
        fs._registry.apply_validation_reference = Mock()
        fs._registry.apply_permission = Mock()
        fs._registry.commit = Mock()
        fs._provider.update_infra = Mock()
        fs._provider.teardown_infra = Mock()

        # Apply the FeatureView
        fs.apply(fv)

        # Verify only 1 feature view was applied (no ODFV generated)
        assert len(applied_views) == 1
        assert isinstance(applied_views[0], FeatureView)
        assert not isinstance(applied_views[0], OnDemandFeatureView)

    def test_no_dual_registration_without_transformation(self):
        """Test that FeatureViews without transformations don't create ODFVs"""
        driver = Entity(name="driver", join_keys=["driver_id"])
        mock_source = FileSource(path="test.parquet", timestamp_field="ts")

        fv = FeatureView(
            name="test_fv",
            source=mock_source,
            entities=[driver],
            schema=[Field(name="feature1", dtype=Float64)],
            online_enabled=True,  # Enabled
            # No feature_transformation
        )

        # Mock FeatureStore
        # Create FeatureStore instance with mocked initialization
        with patch.object(FeatureStore, "__init__", return_value=None):
            fs = FeatureStore()
            fs.config = Mock()
            fs.config.project = "test_project"
        fs._registry = Mock()
        fs._provider = Mock()
        fs._validate_all_feature_views = Mock()
        fs._make_inferences = Mock()

        applied_views = []
        fs._registry.apply_feature_view.side_effect = (
            lambda view, project, commit: applied_views.append(view)
        )
        fs._registry.apply_entity = Mock()
        fs._registry.apply_data_source = Mock()
        fs._registry.apply_feature_service = Mock()
        fs._registry.apply_validation_reference = Mock()
        fs._registry.apply_permission = Mock()
        fs._registry.commit = Mock()
        fs._provider.update_infra = Mock()
        fs._provider.teardown_infra = Mock()

        # Apply the FeatureView
        fs.apply(fv)

        # Verify only 1 feature view was applied (no ODFV generated due to missing transformation)
        assert len(applied_views) == 1
        assert isinstance(applied_views[0], FeatureView)
        assert not isinstance(applied_views[0], OnDemandFeatureView)

    def test_enhanced_decorator_with_dual_registration(self):
        """Test end-to-end: enhanced @transformation decorator -> dual registration"""
        driver = Entity(name="driver", join_keys=["driver_id"])

        # Create FeatureView using enhanced decorator with dummy source
        dummy_source = FileSource(
            path="test.parquet", timestamp_field="event_timestamp"
        )

        @transformation(
            mode="python",
            when="on_write",
            online=True,
            sources=[dummy_source],
            schema=[Field(name="doubled", dtype=Float64)],
            entities=[driver],
            name="doubling_transform",
        )
        def doubling_transform(inputs):
            return [{"doubled": inp.get("value", 0) * 2} for inp in inputs]

        # Verify it's a FeatureView with the right properties
        assert isinstance(doubling_transform, FeatureView)
        assert doubling_transform.online_enabled
        assert doubling_transform.feature_transformation is not None

        # Mock FeatureStore and apply
        # Create FeatureStore instance with mocked initialization
        with patch.object(FeatureStore, "__init__", return_value=None):
            fs = FeatureStore()
            fs.config = Mock()
            fs.config.project = "test_project"
        fs._registry = Mock()
        fs._provider = Mock()
        fs._validate_all_feature_views = Mock()
        fs._make_inferences = Mock()

        applied_views = []
        fs._registry.apply_feature_view.side_effect = (
            lambda view, project, commit: applied_views.append(view)
        )
        fs._registry.apply_entity = Mock()
        fs._registry.apply_data_source = Mock()
        fs._registry.apply_feature_service = Mock()
        fs._registry.apply_validation_reference = Mock()
        fs._registry.apply_permission = Mock()
        fs._registry.commit = Mock()
        fs._provider.update_infra = Mock()
        fs._provider.teardown_infra = Mock()

        # Apply the FeatureView
        fs.apply(doubling_transform)

        # Should create both original FV and ODFV
        assert len(applied_views) == 2

        # Verify the ODFV has the same transformation
        odfv = next(
            (v for v in applied_views if isinstance(v, OnDemandFeatureView)), None
        )
        assert odfv is not None
        assert odfv.name == "doubling_transform_online"

        # Test that both use the same UDF
        test_input = [{"value": 5}]
        expected_output = [{"doubled": 10}]

        original_udf = doubling_transform.feature_transformation.udf
        odfv_udf = odfv.feature_transformation.udf

        assert original_udf(test_input) == expected_output
        assert odfv_udf(test_input) == expected_output
