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
        """Test explicit engine specification."""
        data = {"mock": "data"}
        feast_df = FeastDataFrame(data, engine=DataFrameEngine.SPARK)

        assert feast_df.engine == DataFrameEngine.SPARK
        assert feast_df.is_lazy

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

    @pytest.mark.parametrize(
        "engine,expected_lazy",
        [
            (DataFrameEngine.PANDAS, False),
            (DataFrameEngine.ARROW, False),
            (DataFrameEngine.POLARS, False),
            (DataFrameEngine.SPARK, True),
            (DataFrameEngine.DASK, True),
            (DataFrameEngine.RAY, True),
            (DataFrameEngine.UNKNOWN, False),
        ],
    )
    def test_is_lazy_property(self, engine, expected_lazy):
        """Test is_lazy property for different engines."""
        feast_df = FeastDataFrame({"mock": "data"}, engine=engine)
        assert feast_df.is_lazy == expected_lazy

    def test_polars_detection(self):
        """Test detection of polars DataFrame (using mock)."""

        # Mock polars DataFrame
        class MockPolarsDF:
            def __init__(self):
                self.__module__ = "polars.dataframe.frame"
                self.__class__.__name__ = "DataFrame"

        polars_df = MockPolarsDF()
        feast_df = FeastDataFrame(polars_df)

        assert feast_df.engine == DataFrameEngine.POLARS
        assert not feast_df.is_lazy
