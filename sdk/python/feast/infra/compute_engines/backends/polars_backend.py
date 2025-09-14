from datetime import timedelta
from typing import TYPE_CHECKING

import polars as pl
import pyarrow as pa

from feast.infra.compute_engines.backends.base import DataFrameBackend

if TYPE_CHECKING:
    import numpy as np


class PolarsBackend(DataFrameBackend):
    def columns(self, df: pl.DataFrame) -> list[str]:
        return df.columns

    def from_arrow(self, table: pa.Table) -> pl.DataFrame:
        return pl.from_arrow(table)

    def to_arrow(self, df: pl.DataFrame) -> pa.Table:
        return df.to_arrow()

    def join(self, left: pl.DataFrame, right: pl.DataFrame, on, how) -> pl.DataFrame:
        return left.join(right, on=on, how=how)

    def groupby_agg(self, df: pl.DataFrame, group_keys, agg_ops) -> pl.DataFrame:
        agg_exprs = [
            getattr(pl.col(col), func)().alias(alias)
            for alias, (func, col) in agg_ops.items()
        ]
        return df.groupby(group_keys).agg(agg_exprs)

    def filter(self, df: pl.DataFrame, expr: str) -> pl.DataFrame:
        return df.filter(pl.sql_expr(expr))

    def to_timedelta_value(self, delta: timedelta):
        return pl.duration(milliseconds=delta.total_seconds() * 1000)

    def drop_duplicates(
        self,
        df: pl.DataFrame,
        keys: list[str],
        sort_by: list[str],
        ascending: bool = False,
    ) -> pl.DataFrame:
        return df.sort(by=sort_by, descending=not ascending).unique(
            subset=keys, keep="first"
        )

    def rename_columns(self, df: pl.DataFrame, columns: dict[str, str]) -> pl.DataFrame:
        return df.rename(columns)

    def get_schema(self, df: pl.DataFrame) -> dict[str, "np.dtype"]:
        """Get Polars DataFrame schema as column name to numpy dtype mapping."""
        import numpy as np

        # Convert Polars dtypes to numpy dtypes
        def polars_to_numpy_dtype(polars_dtype):
            dtype_map = {
                pl.Boolean: np.dtype("bool"),
                pl.Int8: np.dtype("int8"),
                pl.Int16: np.dtype("int16"),
                pl.Int32: np.dtype("int32"),
                pl.Int64: np.dtype("int64"),
                pl.UInt8: np.dtype("uint8"),
                pl.UInt16: np.dtype("uint16"),
                pl.UInt32: np.dtype("uint32"),
                pl.UInt64: np.dtype("uint64"),
                pl.Float32: np.dtype("float32"),
                pl.Float64: np.dtype("float64"),
                pl.Utf8: np.dtype("O"),  # Object dtype for strings
                pl.Date: np.dtype("datetime64[D]"),
                pl.Datetime: np.dtype("datetime64[ns]"),
            }
            return dtype_map.get(polars_dtype, np.dtype("O"))  # Default to object

        return {col: polars_to_numpy_dtype(dtype) for col, dtype in df.schema.items()}

    def get_timestamp_range(self, df: pl.DataFrame, timestamp_column: str) -> tuple:
        """Get min/max of a timestamp column in Polars DataFrame."""
        min_val = df[timestamp_column].min()
        max_val = df[timestamp_column].max()

        # Convert to datetime objects if needed
        if hasattr(min_val, "to_py"):
            min_val = min_val.to_py()
        if hasattr(max_val, "to_py"):
            max_val = max_val.to_py()

        return (min_val, max_val)
