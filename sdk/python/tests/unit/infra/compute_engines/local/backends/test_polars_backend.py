import pytest

pytest.importorskip("polars")

import polars as pl
from feast.infra.compute_engines.local.backends.polars_backend import PolarsBackend


class TestPolarsBackend:
    def test_columns(self):
        """Test that PolarsBackend.columns() returns the correct column names"""
        backend = PolarsBackend()
        
        # Test with multiple columns
        df = pl.DataFrame({
            'column_a': [1, 2, 3], 
            'column_b': ['x', 'y', 'z'],
            'column_c': [1.1, 2.2, 3.3]
        })
        
        columns = backend.columns(df)
        expected = ['column_a', 'column_b', 'column_c']
        
        assert columns == expected
        assert isinstance(columns, list)
        
    def test_columns_empty_dataframe(self):
        """Test columns method with empty DataFrame"""
        backend = PolarsBackend()
        
        empty_df = pl.DataFrame()
        columns = backend.columns(empty_df)
        
        assert columns == []
        assert isinstance(columns, list)
        
    def test_columns_single_column(self):
        """Test columns method with single column DataFrame"""
        backend = PolarsBackend()
        
        single_df = pl.DataFrame({'single': [1, 2, 3]})
        columns = backend.columns(single_df)
        
        assert columns == ['single']
        assert isinstance(columns, list)
        
    def test_column_existence_check(self):
        """Test that columns method works for column existence checks (the main use case)"""
        backend = PolarsBackend()
        
        # Create DataFrame with entity timestamp column
        df_with_entity_ts = pl.DataFrame({
            'feature_col': [1, 2, 3], 
            'timestamp_col': [1, 2, 3],
            '__entity_event_timestamp': [10, 20, 30]
        })
        
        columns = backend.columns(df_with_entity_ts)
        assert '__entity_event_timestamp' in columns
        
        # Create DataFrame without entity timestamp column
        df_without_entity_ts = pl.DataFrame({
            'feature_col': [1, 2, 3], 
            'timestamp_col': [1, 2, 3]
        })
        
        columns = backend.columns(df_without_entity_ts)
        assert '__entity_event_timestamp' not in columns