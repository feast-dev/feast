"""Unit tests for FeastDataFrame."""

import pandas as pd
import pyarrow as pa
import pytest

from feast.dataframe import DataFrameEngine, FeastDataFrame


class TestFeastDataFrame:
    """Test suite for FeastDataFrame functionality."""

    def test_pandas_detection(self):
        """Test auto-detection of pandas DataFrame."""
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        feast_df = FeastDataFrame(df)

        assert feast_df.engine == DataFrameEngine.PANDAS
        assert not feast_df.is_lazy
        assert isinstance(feast_df.data, pd.DataFrame)

    def test_arrow_detection(self):
        """Test auto-detection of Arrow Table."""
        table = pa.table({"a": [1, 2, 3], "b": [4, 5, 6]})
        feast_df = FeastDataFrame(table)

        assert feast_df.engine == DataFrameEngine.ARROW
        assert not feast_df.is_lazy
        assert isinstance(feast_df.data, pa.Table)

    def test_explicit_engine(self):
        """Test explicit engine specification with unknown data."""
        data = {"mock": "data"}
        feast_df = FeastDataFrame(data, engine=DataFrameEngine.UNKNOWN)

        assert feast_df.engine == DataFrameEngine.UNKNOWN
        assert not feast_df.is_lazy

    def test_unknown_engine(self):
        """Test handling of unknown DataFrame types."""
        data = {"some": "dict"}
        feast_df = FeastDataFrame(data)

        assert feast_df.engine == DataFrameEngine.UNKNOWN

    def test_metadata(self):
        """Test metadata handling."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        metadata = {"features": ["a"], "source": "test"}
        feast_df = FeastDataFrame(df, metadata=metadata)

        assert feast_df.metadata == metadata
        assert feast_df.metadata["features"] == ["a"]

    def test_repr(self):
        """Test string representation."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        feast_df = FeastDataFrame(df)

        repr_str = repr(feast_df)
        assert "FeastDataFrame" in repr_str
        assert "engine=pandas" in repr_str
        assert "DataFrame" in repr_str

    def test_is_lazy_property(self):
        """Test is_lazy property for different engines."""
        # Test with pandas DataFrame (not lazy)
        df = pd.DataFrame({"a": [1, 2, 3]})
        feast_df = FeastDataFrame(df)
        assert not feast_df.is_lazy
        
        # Test with Arrow table (not lazy)
        table = pa.table({"a": [1, 2, 3]})
        feast_df = FeastDataFrame(table)
        assert not feast_df.is_lazy
        
        # Test with unknown data type (not lazy)
        unknown_data = {"mock": "data"}
        feast_df = FeastDataFrame(unknown_data)
        assert not feast_df.is_lazy
        
        # Test explicit lazy engines (using unknown data to avoid type validation)
        for lazy_engine in [DataFrameEngine.SPARK, DataFrameEngine.DASK, DataFrameEngine.RAY]:
            feast_df = FeastDataFrame(unknown_data, engine=DataFrameEngine.UNKNOWN)
            feast_df._engine = lazy_engine  # Override for testing
            assert feast_df.is_lazy

    def test_polars_detection(self):
        """Test detection of polars DataFrame (using mock)."""

        # Mock polars DataFrame
        class MockPolarsDF:
            __module__ = "polars.dataframe.frame"
            
            def __init__(self):
                pass

        polars_df = MockPolarsDF()
        feast_df = FeastDataFrame(polars_df)

        assert feast_df.engine == DataFrameEngine.POLARS
        assert not feast_df.is_lazy

    def test_engine_validation_valid(self):
        """Test that providing a correct engine passes validation."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        feast_df = FeastDataFrame(df, engine=DataFrameEngine.PANDAS)
        
        assert feast_df.engine == DataFrameEngine.PANDAS
        assert isinstance(feast_df.data, pd.DataFrame)

    def test_engine_validation_invalid(self):
        """Test that providing an incorrect engine raises ValueError."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        
        with pytest.raises(ValueError, match="Provided engine 'spark' does not match detected engine 'pandas'"):
            FeastDataFrame(df, engine=DataFrameEngine.SPARK)

    def test_engine_validation_arrow(self):
        """Test engine validation with Arrow table."""
        table = pa.table({"a": [1, 2, 3]})
        
        # Valid case
        feast_df = FeastDataFrame(table, engine=DataFrameEngine.ARROW)
        assert feast_df.engine == DataFrameEngine.ARROW
        
        # Invalid case
        with pytest.raises(ValueError, match="Provided engine 'pandas' does not match detected engine 'arrow'"):
            FeastDataFrame(table, engine=DataFrameEngine.PANDAS)
