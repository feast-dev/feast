"""
Unit tests for schema-based transformation detection.

Tests the automatic detection of whether to apply transformations based on
whether incoming data matches input schemas (raw data) or output schemas
(pre-transformed data).
"""

import unittest

import pandas as pd

from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.schema_utils import (
    extract_column_names,
    get_input_schema_columns,
    get_output_schema_columns,
    should_apply_transformation,
    validate_transformation_compatibility,
)
from feast.transformation.base import transformation
from feast.types import Int64


class TestSchemaDetection(unittest.TestCase):
    """Test schema-based transformation detection."""

    def setUp(self):
        """Set up test fixtures."""
        # Create test entity
        self.driver = Entity(name="driver", join_keys=["driver_id"])

        # Create test source
        self.source = FileSource(path="test.parquet", timestamp_field="event_timestamp")

        # Create transformation
        @transformation(mode="python")
        def doubling_transform(inputs):
            return [{"doubled_value": inp.get("value", 0) * 2} for inp in inputs]

        self.transformation = doubling_transform

        # Create FeatureView with transformation
        self.feature_view = FeatureView(
            name="test_fv",
            source=self.source,
            entities=[self.driver],
            schema=[Field(name="doubled_value", dtype=Int64)],
            feature_transformation=self.transformation,
        )

        # Create FeatureView without transformation
        self.feature_view_no_transform = FeatureView(
            name="test_fv_no_transform",
            source=self.source,
            entities=[self.driver],
            schema=[
                Field(name="value", dtype=Int64),
            ],
        )

    def test_extract_column_names_dataframe(self):
        """Test column name extraction from pandas DataFrame."""
        df = pd.DataFrame({"driver_id": [1], "value": [5]})
        columns = extract_column_names(df)
        self.assertEqual(columns, {"driver_id", "value"})

    def test_extract_column_names_dict(self):
        """Test column name extraction from dictionary."""
        data = {"driver_id": 1, "value": 5}
        columns = extract_column_names(data)
        self.assertEqual(columns, {"driver_id", "value"})

    def test_extract_column_names_list_of_dicts(self):
        """Test column name extraction from list of dictionaries."""
        data = [{"driver_id": 1, "value": 5}, {"driver_id": 2, "value": 10}]
        columns = extract_column_names(data)
        self.assertEqual(columns, {"driver_id", "value"})

    def test_get_input_schema_columns(self):
        """Test getting input schema columns from FeatureView."""
        input_columns = get_input_schema_columns(self.feature_view)
        # Note: FileSource without explicit schema falls back to entity names + timestamp
        # Since entities are stored as strings, we use entity name rather than join keys
        expected_columns = {"driver", "event_timestamp"}
        self.assertEqual(input_columns, expected_columns)

    def test_get_output_schema_columns(self):
        """Test getting output schema columns from FeatureView."""
        output_columns = get_output_schema_columns(self.feature_view)
        expected_columns = {"driver", "doubled_value"}  # Entity + feature columns
        self.assertEqual(output_columns, expected_columns)

    def test_should_apply_transformation_input_schema_match(self):
        """Test detection when data matches input schema (should transform)."""
        # Data matches input schema
        input_data = {"driver": 1, "event_timestamp": "2023-01-01"}

        result = should_apply_transformation(self.feature_view, input_data)
        self.assertTrue(
            result, "Should apply transformation when data matches input schema"
        )

    def test_should_apply_transformation_output_schema_match(self):
        """Test detection when data matches output schema (should not transform)."""
        # Data matches output schema
        output_data = {"driver": 1, "doubled_value": 10}

        result = should_apply_transformation(self.feature_view, output_data)
        self.assertFalse(
            result, "Should not apply transformation when data matches output schema"
        )

    def test_should_apply_transformation_no_transformation(self):
        """Test detection when feature view has no transformation."""
        input_data = {"driver_id": 1, "value": 5}

        result = should_apply_transformation(self.feature_view_no_transform, input_data)
        self.assertIsNone(
            result, "Should return None when no transformation is configured"
        )

    def test_should_apply_transformation_ambiguous_case(self):
        """Test detection when data matches both input and output schemas."""
        # Create a case where input and output schemas overlap
        # This could happen if transformation just adds columns without removing them
        ambiguous_data = {
            "driver_id": 1,
            "value": 5,
            "doubled_value": 10,
            "event_timestamp": "2023-01-01",
        }

        result = should_apply_transformation(self.feature_view, ambiguous_data)
        self.assertIsNone(result, "Should return None for ambiguous cases")

    def test_should_apply_transformation_no_schema_match(self):
        """Test detection when data doesn't match any schema clearly."""
        # Data that doesn't clearly match either schema
        unknown_data = {"unknown_field": 123}

        result = should_apply_transformation(self.feature_view, unknown_data)
        self.assertIsNone(
            result, "Should return None when data doesn't match any schema"
        )

    def test_should_apply_transformation_dataframe_input(self):
        """Test detection with pandas DataFrame input."""
        # DataFrame with input schema
        input_df = pd.DataFrame(
            {
                "driver": [1, 2],
                "value": [5, 10],
                "event_timestamp": ["2023-01-01", "2023-01-02"],
            }
        )

        result = should_apply_transformation(self.feature_view, input_df)
        self.assertTrue(
            result, "Should apply transformation for DataFrame matching input schema"
        )

        # DataFrame with output schema
        output_df = pd.DataFrame({"driver": [1, 2], "doubled_value": [10, 20]})

        result = should_apply_transformation(self.feature_view, output_df)
        self.assertFalse(
            result,
            "Should not apply transformation for DataFrame matching output schema",
        )

    def test_should_apply_transformation_subset_matching(self):
        """Test detection with subset schema matching (superset data)."""
        # Data is superset of input schema (extra columns are ok)
        superset_input_data = {
            "driver": 1,
            "value": 5,
            "event_timestamp": "2023-01-01",
            "extra_field": "extra_value",
        }

        result = should_apply_transformation(self.feature_view, superset_input_data)
        self.assertTrue(
            result, "Should apply transformation when data is superset of input schema"
        )

    def test_validate_transformation_compatibility(self):
        """Test transformation compatibility validation."""
        # Valid input data
        input_data = {"driver": 1, "value": 5, "event_timestamp": "2023-01-01"}
        transformed_data = {"driver": 1, "doubled_value": 10}

        errors = validate_transformation_compatibility(
            self.feature_view, input_data, transformed_data
        )
        self.assertEqual(len(errors), 0, "Should have no errors for valid data")

        # Invalid input data (missing required column)
        invalid_input_data = {"driver": 1}  # Missing value and timestamp

        errors = validate_transformation_compatibility(
            self.feature_view, invalid_input_data
        )
        self.assertGreater(
            len(errors), 0, "Should have errors for missing input columns"
        )
        # Only event_timestamp is required from the input schema (entity + timestamp)
        # 'value' is not part of the detected input schema for sources without explicit schema
        self.assertIn("event_timestamp", str(errors))

        # Invalid transformed data (missing required output column)
        invalid_transformed_data = {"driver": 1}  # Missing doubled_value

        errors = validate_transformation_compatibility(
            self.feature_view, input_data, invalid_transformed_data
        )
        self.assertGreater(
            len(errors), 0, "Should have errors for missing output columns"
        )
        self.assertIn("doubled_value", str(errors))


class TestOnDemandFeatureViewSchemaDetection(unittest.TestCase):
    """Test schema detection for OnDemandFeatureViews."""

    def setUp(self):
        """Set up ODFV test fixtures."""
        from feast.data_source import RequestSource
        from feast.on_demand_feature_view import on_demand_feature_view

        # Create request source
        self.request_source = RequestSource(
            name="request_source",
            schema=[
                Field(name="input_value", dtype=Int64),
            ],
        )

        # Create ODFV
        @on_demand_feature_view(
            sources=[self.request_source],
            schema=[Field(name="output_value", dtype=Int64)],
            mode="python",
        )
        def test_odfv(inputs):
            return {"output_value": inputs["input_value"][0] * 3}

        self.odfv = test_odfv

    def test_get_input_schema_columns_odfv(self):
        """Test getting input schema columns from ODFV."""
        input_columns = get_input_schema_columns(self.odfv)
        expected_columns = {"input_value"}
        self.assertEqual(input_columns, expected_columns)

    def test_get_output_schema_columns_odfv(self):
        """Test getting output schema columns from ODFV."""
        output_columns = get_output_schema_columns(self.odfv)
        expected_columns = {"output_value"}  # No entity columns for ODFVs
        self.assertEqual(output_columns, expected_columns)

    def test_should_apply_transformation_odfv_input_match(self):
        """Test ODFV transformation detection with input match."""
        input_data = {"input_value": 5}

        result = should_apply_transformation(self.odfv, input_data)
        self.assertTrue(
            result, "Should apply transformation for ODFV input schema match"
        )

    def test_should_apply_transformation_odfv_output_match(self):
        """Test ODFV transformation detection with output match."""
        output_data = {"output_value": 15}

        result = should_apply_transformation(self.odfv, output_data)
        self.assertFalse(
            result, "Should not apply transformation for ODFV output schema match"
        )


if __name__ == "__main__":
    unittest.main()
