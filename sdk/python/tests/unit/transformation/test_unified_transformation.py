"""
Unit tests for the unified transformation system.

Tests the enhanced @transformation decorator with when/online parameters,
dual registration, training-serving consistency, and backward compatibility.
"""

import pytest

from feast.transformation.base import (
    Transformation,
    transformation,
)
from feast.transformation.mode import TransformationMode


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

    def test_simplified_decorator_creates_transformation(self):
        """Test that simplified decorator creates Transformation object only"""

        @transformation(mode="python")
        def simple_transform(inputs):
            return [{"total": inp.get("a", 0) + inp.get("b", 0)} for inp in inputs]

        assert isinstance(simple_transform, Transformation)
        assert simple_transform.mode == TransformationMode.PYTHON
        assert simple_transform.udf is not None
        assert callable(simple_transform.udf)

    def test_simplified_decorator_with_enum_mode(self):
        """Test simplified decorator works with TransformationMode enum"""

        @transformation(mode=TransformationMode.PANDAS)
        def enum_mode_transform(df):
            return df

        assert isinstance(enum_mode_transform, Transformation)
        assert enum_mode_transform.mode == TransformationMode.PANDAS

    def test_mode_parameter_validation(self):
        """Test that mode parameter validation works correctly"""

        # Test valid mode string
        @transformation(mode="python")
        def valid_mode_transform(inputs):
            return inputs

        assert isinstance(valid_mode_transform, Transformation)

        # Test valid mode enum
        @transformation(mode=TransformationMode.PANDAS)
        def valid_enum_transform(df):
            return df

        assert isinstance(valid_enum_transform, Transformation)

    def test_invalid_mode_validation(self):
        """Test that invalid mode raises ValueError"""
        with pytest.raises(ValueError, match="Invalid mode 'invalid_mode'"):

            @transformation(mode="invalid_mode")
            def invalid_mode_transform(inputs):
                return inputs

    def test_training_serving_consistency(self):
        """Test that same UDF produces consistent results"""

        @transformation(mode="python")
        def consistent_transform(inputs):
            return [{"doubled": inp.get("value", 0) * 2} for inp in inputs]

        # Test the UDF directly
        test_input = [{"value": 5}]
        expected_output = [{"doubled": 10}]

        udf = consistent_transform.udf
        actual_output = udf(test_input)

        assert actual_output == expected_output

    def test_transformation_attributes(self):
        """Test that Transformation gets all the attributes"""

        @transformation(
            mode="python",
            name="test_transform",
            description="Test description",
            tags={"env": "test"},
            owner="test@example.com",
        )
        def full_featured_transform(inputs):
            return inputs

        transform = full_featured_transform
        assert transform.name == "test_transform"
        assert transform.description == "Test description"
        assert transform.tags["env"] == "test"
        assert transform.owner == "test@example.com"
        assert transform.mode == TransformationMode.PYTHON

    def test_mode_normalization(self):
        """Test that both enum and string modes are properly normalized"""

        # String mode
        @transformation(mode="PYTHON")  # Uppercase
        def string_mode_transform(inputs):
            return inputs

        assert string_mode_transform.mode == TransformationMode.PYTHON

        # Enum mode
        @transformation(mode=TransformationMode.PANDAS)
        def enum_mode_transform(inputs):
            return inputs

        assert enum_mode_transform.mode == TransformationMode.PANDAS

    def test_function_metadata_preservation(self):
        """Test that function metadata is preserved via functools.update_wrapper"""

        @transformation(mode="python")
        def documented_transform(inputs):
            """This is a test transformation function"""
            return inputs

        # Check that docstring and name are preserved
        assert documented_transform.__doc__ == "This is a test transformation function"
        assert documented_transform.__name__ == "documented_transform"
