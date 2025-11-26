"""
Unit tests for the unified transformation system.

Tests the enhanced @transformation decorator with when/online parameters,
dual registration, training-serving consistency, and backward compatibility.
"""

import pytest
from datetime import timedelta
from feast.transformation.base import transformation, is_online_compatible, ONLINE_COMPATIBLE_MODES, BATCH_ONLY_MODES
from feast.transformation.mode import TransformationMode, TransformationTiming
from feast.field import Field
from feast.types import Float64, String, Int64
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.transformation.base import Transformation
import pandas as pd


def create_dummy_source():
    """Helper to create a dummy source for tests"""
    from feast.infra.offline_stores.file_source import FileSource
    return FileSource(path="test.parquet", timestamp_field="event_timestamp")


class TestUnifiedTransformation:
    """Test the unified transformation system"""

    def test_backward_compatibility_string_mode(self):
        """Test that old @transformation(mode=string) still works"""
        @transformation(mode="python")
        def old_transform(df):
            return df

        assert isinstance(old_transform, Transformation)
        assert old_transform.mode == TransformationMode.PYTHON

    def test_backward_compatibility_enum_mode(self):
        """Test that old @transformation(mode=enum) still works"""
        @transformation(mode=TransformationMode.PANDAS)
        def old_transform(df):
            return df

        assert isinstance(old_transform, Transformation)
        assert old_transform.mode == TransformationMode.PANDAS

    def test_enhanced_decorator_creates_feature_view(self):
        """Test that enhanced decorator creates FeatureView when all params provided"""
        driver = Entity(name="driver", join_keys=["driver_id"])

        @transformation(
            mode="python",
            when="on_write",
            online=True,
            sources=[create_dummy_source()],
            schema=[Field(name="total", dtype=Float64)],
            entities=[driver]
        )
        def enhanced_transform(inputs):
            return [{"total": inp.get("a", 0) + inp.get("b", 0)} for inp in inputs]

        assert isinstance(enhanced_transform, FeatureView)
        assert enhanced_transform.feature_transformation is not None
        assert enhanced_transform.when == "on_write"
        assert enhanced_transform.online_enabled == True
        assert enhanced_transform.mode == "python"

    def test_enhanced_decorator_with_enum_mode(self):
        """Test enhanced decorator works with TransformationMode enum"""
        @transformation(
            mode=TransformationMode.PANDAS,
            when="batch",
            online=False,
            sources=[create_dummy_source()],
            schema=[Field(name="result", dtype=Int64)]
        )
        def enum_mode_transform(df):
            return df

        assert isinstance(enum_mode_transform, FeatureView)
        assert enum_mode_transform.mode == "pandas"

    def test_required_parameters_validation(self):
        """Test that missing required parameters raise ValueError"""
        # Missing when
        with pytest.raises(ValueError, match="'when' parameter is required"):
            @transformation(
                mode="python",
                online=True,
                sources=[create_dummy_source()],
                schema=[]
            )
            def missing_when(inputs):
                return inputs

        # Missing online
        with pytest.raises(ValueError, match="'online' parameter is required"):
            @transformation(
                mode="python",
                when="on_write",
                sources=[create_dummy_source()],
                schema=[]
            )
            def missing_online(inputs):
                return inputs

        # Missing sources
        with pytest.raises(ValueError, match="'sources' parameter is required"):
            @transformation(
                mode="python",
                when="on_write",
                online=True,
                schema=[]
            )
            def missing_sources(inputs):
                return inputs

        # Missing schema
        with pytest.raises(ValueError, match="'schema' parameter is required"):
            @transformation(
                mode="python",
                when="on_write",
                online=True,
                sources=[create_dummy_source()]
            )
            def missing_schema(inputs):
                return inputs

    def test_invalid_mode_validation(self):
        """Test that invalid mode raises ValueError"""
        with pytest.raises(ValueError, match="Invalid mode 'invalid_mode'"):
            @transformation(mode="invalid_mode")
            def invalid_mode_transform(inputs):
                return inputs

    def test_invalid_timing_validation(self):
        """Test that invalid timing raises ValueError"""
        with pytest.raises(ValueError, match="Invalid timing 'invalid_timing'"):
            @transformation(
                mode="python",
                when="invalid_timing",
                online=False,
                sources=[create_dummy_source()],
                schema=[]
            )
            def invalid_timing_transform(inputs):
                return inputs

    def test_online_compatibility_validation(self):
        """Test online compatibility validation"""
        # SQL can't run online
        with pytest.raises(ValueError, match="cannot run online in Feature Server"):
            @transformation(
                mode="sql",
                when="on_write",
                online=True,
                sources=[create_dummy_source()],
                schema=[]
            )
            def sql_online_transform(inputs):
                return "SELECT * FROM table"

        # Ray can't run online
        with pytest.raises(ValueError, match="cannot run online in Feature Server"):
            @transformation(
                mode="ray",
                when="on_write",
                online=True,
                sources=[create_dummy_source()],
                schema=[]
            )
            def ray_online_transform(inputs):
                return inputs

        # Spark can't run online
        with pytest.raises(ValueError, match="cannot run online in Feature Server"):
            @transformation(
                mode="spark",
                when="on_write",
                online=True,
                sources=[create_dummy_source()],
                schema=[]
            )
            def spark_online_transform(inputs):
                return inputs

    def test_valid_online_modes(self):
        """Test that python and pandas can run online"""
        @transformation(
            mode="python",
            when="on_write",
            online=True,
            sources=[create_dummy_source()],
            schema=[]
        )
        def python_online_transform(inputs):
            return inputs

        @transformation(
            mode="pandas",
            when="on_write",
            online=True,
            sources=[create_dummy_source()],
            schema=[]
        )
        def pandas_online_transform(inputs):
            return inputs

        assert isinstance(python_online_transform, FeatureView)
        assert isinstance(pandas_online_transform, FeatureView)

    def test_training_serving_consistency(self):
        """Test that same UDF produces consistent results"""
        @transformation(
            mode="python",
            when="on_write",
            online=True,
            sources=[create_dummy_source()],
            schema=[Field(name="doubled", dtype=Float64)]
        )
        def consistent_transform(inputs):
            return [{"doubled": inp.get("value", 0) * 2} for inp in inputs]

        # Test the UDF directly
        test_input = [{"value": 5}]
        expected_output = [{"doubled": 10}]

        udf = consistent_transform.feature_transformation.udf
        actual_output = udf(test_input)

        assert actual_output == expected_output

    def test_online_compatibility_functions(self):
        """Test online compatibility helper functions"""
        # Test online compatible modes
        for mode in ONLINE_COMPATIBLE_MODES:
            assert is_online_compatible(mode) == True
            assert is_online_compatible(mode.upper()) == True

        # Test batch only modes
        for mode in BATCH_ONLY_MODES:
            assert is_online_compatible(mode) == False
            assert is_online_compatible(mode.upper()) == False

    def test_transformation_timing_enum(self):
        """Test TransformationTiming enum values"""
        assert TransformationTiming.ON_READ.value == "on_read"
        assert TransformationTiming.ON_WRITE.value == "on_write"
        assert TransformationTiming.BATCH.value == "batch"
        assert TransformationTiming.STREAMING.value == "streaming"

    def test_feature_view_attributes(self):
        """Test that FeatureView gets all the new attributes"""
        driver = Entity(name="driver", join_keys=["driver_id"])

        @transformation(
            mode="python",
            when="on_write",
            online=True,
            sources=[create_dummy_source()],
            schema=[Field(name="result", dtype=String)],
            entities=[driver],
            name="test_transform",
            description="Test description",
            tags={"env": "test"},
            owner="test@example.com"
        )
        def full_featured_transform(inputs):
            return inputs

        fv = full_featured_transform
        assert hasattr(fv, 'feature_transformation')
        assert hasattr(fv, 'when')
        assert hasattr(fv, 'online_enabled')
        assert fv.feature_transformation is not None
        assert fv.when == "on_write"
        assert fv.online_enabled == True
        assert fv.name == "test_transform"
        assert fv.description == "Test description"
        assert fv.tags["env"] == "test"
        assert fv.owner == "test@example.com"

    def test_mode_normalization(self):
        """Test that both enum and string modes are properly normalized"""
        # String mode
        @transformation(
            mode="PYTHON",  # Uppercase
            when="on_write",
            online=False,
            sources=[create_dummy_source()],
            schema=[]
        )
        def string_mode_transform(inputs):
            return inputs

        assert string_mode_transform.mode == "python"  # Normalized to lowercase

        # Enum mode
        @transformation(
            mode=TransformationMode.PANDAS,
            when="on_write",
            online=False,
            sources=[create_dummy_source()],
            schema=[]
        )
        def enum_mode_transform(inputs):
            return inputs

        assert enum_mode_transform.mode == "pandas"  # Enum value extracted

    def test_function_metadata_preservation(self):
        """Test that function metadata is preserved via functools.update_wrapper"""
        @transformation(
            mode="python",
            when="on_write",
            online=False,
            sources=[create_dummy_source()],
            schema=[]
        )
        def documented_transform(inputs):
            """This is a test transformation function"""
            return inputs

        # Check that docstring and name are preserved
        assert documented_transform.__doc__ == "This is a test transformation function"
        assert documented_transform.__name__ == "documented_transform"