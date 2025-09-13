"""Tests for RetrievalJob FeastDataFrame integration."""

from unittest.mock import Mock

import pandas as pd
import pyarrow as pa

from feast.dataframe import DataFrameEngine, FeastDataFrame
from feast.infra.offline_stores.offline_store import RetrievalJob


class MockRetrievalJob(RetrievalJob):
    """Mock RetrievalJob for testing."""

    def __init__(
        self, arrow_table: pa.Table, features: list = None, odfvs: list = None
    ):
        self.arrow_table = arrow_table
        self.features = features or []
        self.odfvs = odfvs or []

    def _to_arrow_internal(self, timeout=None):
        return self.arrow_table

    @property
    def full_feature_names(self):
        return False

    @property
    def on_demand_feature_views(self):
        return self.odfvs


class TestRetrievalJobFeastDataFrame:
    """Test RetrievalJob FeastDataFrame integration."""

    def test_to_feast_df_basic(self):
        """Test basic to_feast_df functionality."""
        # Create test data
        test_data = pa.table(
            {
                "feature1": [1, 2, 3],
                "feature2": ["a", "b", "c"],
                "timestamp": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
            }
        )

        # Create mock retrieval job
        job = MockRetrievalJob(test_data, features=["feature1", "feature2"])

        # Test to_feast_df
        feast_df = job.to_feast_df()

        # Assertions
        assert isinstance(feast_df, FeastDataFrame)
        assert feast_df.engine == DataFrameEngine.ARROW
        assert isinstance(feast_df.data, pa.Table)
        assert feast_df.data.num_rows == 3
        assert feast_df.data.num_columns == 3

    def test_to_feast_df_metadata(self):
        """Test to_feast_df metadata population."""
        # Create test data
        test_data = pa.table({"feature1": [1, 2, 3], "feature2": [4.0, 5.0, 6.0]})

        # Create mock on-demand feature views
        mock_odfv1 = Mock()
        mock_odfv1.name = "odfv1"
        mock_odfv2 = Mock()
        mock_odfv2.name = "odfv2"

        # Create mock retrieval job with features and ODFVs
        job = MockRetrievalJob(
            test_data, features=["feature1", "feature2"], odfvs=[mock_odfv1, mock_odfv2]
        )

        # Test to_feast_df
        feast_df = job.to_feast_df()

        # Check metadata
        assert "features" in feast_df.metadata
        assert "on_demand_feature_views" in feast_df.metadata
        assert feast_df.metadata["features"] == ["feature1", "feature2"]
        assert feast_df.metadata["on_demand_feature_views"] == ["odfv1", "odfv2"]

    def test_to_feast_df_with_timeout(self):
        """Test to_feast_df with timeout parameter."""
        test_data = pa.table({"feature1": [1, 2, 3]})
        job = MockRetrievalJob(test_data)

        # Test with timeout - should not raise any errors
        feast_df = job.to_feast_df(timeout=30)

        assert isinstance(feast_df, FeastDataFrame)
        assert feast_df.engine == DataFrameEngine.ARROW

    def test_to_feast_df_empty_metadata(self):
        """Test to_feast_df with empty features and ODFVs."""
        test_data = pa.table({"feature1": [1, 2, 3]})
        job = MockRetrievalJob(test_data)  # No features or ODFVs provided

        feast_df = job.to_feast_df()

        # Should handle missing features gracefully
        assert feast_df.metadata["features"] == []
        assert feast_df.metadata["on_demand_feature_views"] == []

    def test_to_feast_df_preserves_arrow_data(self):
        """Test that to_feast_df preserves the original Arrow data."""
        # Create test data with specific types
        test_data = pa.table(
            {
                "int_feature": pa.array([1, 2, 3], type=pa.int64()),
                "float_feature": pa.array([1.1, 2.2, 3.3], type=pa.float64()),
                "string_feature": pa.array(["a", "b", "c"], type=pa.string()),
                "bool_feature": pa.array([True, False, True], type=pa.bool_()),
            }
        )

        job = MockRetrievalJob(test_data)
        feast_df = job.to_feast_df()

        # Check that the Arrow data is exactly the same
        assert feast_df.data.equals(test_data)
        assert feast_df.data.schema == test_data.schema

        # Check column names and types are preserved
        assert feast_df.data.column_names == test_data.column_names
        for i, column in enumerate(test_data.schema):
            assert feast_df.data.schema.field(i).type == column.type

    def test_to_df_still_works(self):
        """Test that the original to_df method still works unchanged."""
        test_data = pa.table({"feature1": [1, 2, 3], "feature2": ["a", "b", "c"]})

        job = MockRetrievalJob(test_data)

        # Test to_df returns pandas DataFrame
        df = job.to_df()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert list(df.columns) == ["feature1", "feature2"]
        assert df["feature1"].tolist() == [1, 2, 3]
        assert df["feature2"].tolist() == ["a", "b", "c"]

    def test_both_methods_return_same_data(self):
        """Test that to_df and to_feast_df return equivalent data."""
        test_data = pa.table(
            {"feature1": [1, 2, 3, 4], "feature2": [10.5, 20.5, 30.5, 40.5]}
        )

        job = MockRetrievalJob(test_data)

        # Get data from both methods
        df = job.to_df()
        feast_df = job.to_feast_df()

        # Convert FeastDataFrame to pandas for comparison
        feast_as_pandas = feast_df.data.to_pandas().reset_index(drop=True)

        # Should be equivalent
        pd.testing.assert_frame_equal(df, feast_as_pandas)
