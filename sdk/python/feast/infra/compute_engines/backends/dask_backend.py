from datetime import timedelta
from typing import TYPE_CHECKING, Any, Dict, List, Tuple

import dask.dataframe as dd
import pyarrow as pa

from feast.infra.compute_engines.backends.base import DataFrameBackend

if TYPE_CHECKING:
    import numpy as np


class DaskBackend(DataFrameBackend):
    """Dask DataFrame backend implementation."""

    def __init__(self):
        if dd is None:
            raise ImportError(
                "Dask is not installed. Please install it to use Dask backend."
            )

    def columns(self, df: dd.DataFrame) -> List[str]:
        """Get column names from Dask DataFrame."""
        return df.columns.tolist()

    def from_arrow(self, table: pa.Table) -> dd.DataFrame:
        """Convert Arrow table to Dask DataFrame."""
        pandas_df = table.to_pandas()
        return dd.from_pandas(pandas_df, npartitions=1)

    def to_arrow(self, df: dd.DataFrame) -> pa.Table:
        """Convert Dask DataFrame to Arrow table."""
        pandas_df = df.compute()
        return pa.Table.from_pandas(pandas_df)

    def join(
        self, left: dd.DataFrame, right: dd.DataFrame, on: List[str], how: str
    ) -> dd.DataFrame:
        """Join two Dask DataFrames."""
        return left.merge(right, on=on, how=how)

    def groupby_agg(
        self,
        df: dd.DataFrame,
        group_keys: List[str],
        agg_ops: Dict[str, Tuple[str, str]],
    ) -> dd.DataFrame:
        """Group and aggregate Dask DataFrame."""

        # Convert agg_ops to pandas-style aggregation
        agg_dict = {col_name: func for alias, (func, col_name) in agg_ops.items()}

        result = df.groupby(group_keys).agg(agg_dict).reset_index()

        # Rename columns to match expected aliases
        rename_mapping = {}
        for alias, (func, col_name) in agg_ops.items():
            if func in ["sum", "count", "mean", "min", "max"]:
                old_name = f"{col_name}"
                if old_name in result.columns:
                    rename_mapping[old_name] = alias

        if rename_mapping:
            result = result.rename(columns=rename_mapping)

        return result

    def filter(self, df: dd.DataFrame, expr: str) -> dd.DataFrame:
        """Apply filter expression to Dask DataFrame."""
        return df.query(expr)

    def to_timedelta_value(self, delta: timedelta) -> Any:
        """Convert timedelta to Dask-compatible timedelta."""
        import pandas as pd

        return pd.to_timedelta(delta)

    def drop_duplicates(
        self,
        df: dd.DataFrame,
        keys: List[str],
        sort_by: List[str],
        ascending: bool = False,
    ) -> dd.DataFrame:
        """Deduplicate Dask DataFrame by keys, sorted by sort_by columns."""
        return df.drop_duplicates(subset=keys).reset_index(drop=True)

    def rename_columns(self, df: dd.DataFrame, columns: Dict[str, str]) -> dd.DataFrame:
        """Rename columns in Dask DataFrame."""
        return df.rename(columns=columns)

    def get_schema(self, df: dd.DataFrame) -> Dict[str, "np.dtype"]:
        """Get Dask DataFrame schema as column name to numpy dtype mapping."""
        # Dask dtypes are pandas-compatible numpy dtypes
        return {col: dtype for col, dtype in df.dtypes.items()}

    def get_timestamp_range(self, df: dd.DataFrame, timestamp_column: str) -> tuple:
        """Get min/max of a timestamp column in Dask DataFrame."""
        import pandas as pd

        col = df[timestamp_column]
        # Ensure it's datetime type
        if not pd.api.types.is_datetime64_any_dtype(col.dtype):
            col = dd.to_datetime(col, utc=True)

        min_val = col.min().compute()
        max_val = col.max().compute()

        # Convert to datetime objects
        if hasattr(min_val, "to_pydatetime"):
            min_val = min_val.to_pydatetime()
        if hasattr(max_val, "to_pydatetime"):
            max_val = max_val.to_pydatetime()

        return (min_val, max_val)
