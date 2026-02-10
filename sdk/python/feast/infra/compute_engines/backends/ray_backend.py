from datetime import timedelta
from typing import TYPE_CHECKING, Any, Dict, List, Tuple

import pyarrow as pa
import ray.data

from feast.infra.compute_engines.backends.base import DataFrameBackend

if TYPE_CHECKING:
    import numpy as np


class RayBackend(DataFrameBackend):
    """Ray Dataset backend implementation."""

    def __init__(self):
        if not hasattr(ray.data, "Dataset"):
            raise ImportError(
                "Ray is not installed. Please install it to use Ray backend."
            )

    def columns(self, df: ray.data.Dataset) -> List[str]:
        """Get column names from Ray Dataset."""
        return df.schema().names

    def from_arrow(self, table: pa.Table) -> ray.data.Dataset:
        """Convert Arrow table to Ray Dataset."""
        return ray.data.from_arrow(table)

    def to_arrow(self, df: ray.data.Dataset) -> pa.Table:
        """Convert Ray Dataset to Arrow table."""
        return df.to_arrow()

    def join(
        self, left: ray.data.Dataset, right: ray.data.Dataset, on: List[str], how: str
    ) -> ray.data.Dataset:
        """Join two Ray Datasets."""
        # Ray doesn't support arbitrary join keys directly, convert via Arrow
        left_arrow = self.to_arrow(left)
        right_arrow = self.to_arrow(right)

        # Use pandas for the join operation
        left_pd = left_arrow.to_pandas()
        right_pd = right_arrow.to_pandas()

        result_pd = left_pd.merge(right_pd, on=on, how=how)
        result_arrow = pa.Table.from_pandas(result_pd)
        return self.from_arrow(result_arrow)

    def groupby_agg(
        self,
        df: ray.data.Dataset,
        group_keys: List[str],
        agg_ops: Dict[str, Tuple[str, str]],
    ) -> ray.data.Dataset:
        """Group and aggregate Ray Dataset."""
        # Ray's groupby is limited, so we'll use pandas conversion
        arrow_table = self.to_arrow(df)
        pandas_df = arrow_table.to_pandas()

        # Use pandas groupby
        agg_dict = {col_name: func for alias, (func, col_name) in agg_ops.items()}

        result = pandas_df.groupby(group_keys).agg(agg_dict).reset_index()

        # Rename columns to match expected aliases
        rename_mapping = {}
        for alias, (func, col_name) in agg_ops.items():
            if func in ["sum", "count", "mean", "min", "max"]:
                old_name = f"{col_name}"
                if old_name in result.columns:
                    rename_mapping[old_name] = alias

        if rename_mapping:
            result = result.rename(columns=rename_mapping)

        # Convert back to Ray Dataset
        result_arrow = pa.Table.from_pandas(result)
        return self.from_arrow(result_arrow)

    def filter(self, df: ray.data.Dataset, expr: str) -> ray.data.Dataset:
        """Apply filter expression to Ray Dataset."""
        # Ray has limited SQL support, convert to pandas for complex expressions
        arrow_table = self.to_arrow(df)
        pandas_df = arrow_table.to_pandas()
        filtered_df = pandas_df.query(expr)
        result_arrow = pa.Table.from_pandas(filtered_df)
        return self.from_arrow(result_arrow)

    def to_timedelta_value(self, delta: timedelta) -> Any:
        """Convert timedelta to Ray-compatible timedelta."""
        import pandas as pd

        return pd.to_timedelta(delta)

    def drop_duplicates(
        self,
        df: ray.data.Dataset,
        keys: List[str],
        sort_by: List[str],
        ascending: bool = False,
    ) -> ray.data.Dataset:
        """Deduplicate Ray Dataset by keys, sorted by sort_by columns."""
        # Convert to pandas for deduplication
        arrow_table = self.to_arrow(df)
        pandas_df = arrow_table.to_pandas()

        result_df = pandas_df.sort_values(
            by=sort_by, ascending=ascending
        ).drop_duplicates(subset=keys)

        result_arrow = pa.Table.from_pandas(result_df)
        return self.from_arrow(result_arrow)

    def rename_columns(
        self, df: ray.data.Dataset, columns: Dict[str, str]
    ) -> ray.data.Dataset:
        """Rename columns in Ray Dataset."""
        # Ray doesn't have direct rename, so convert via Arrow
        arrow_table = self.to_arrow(df)
        pandas_df = arrow_table.to_pandas()
        renamed_df = pandas_df.rename(columns=columns)
        result_arrow = pa.Table.from_pandas(renamed_df)
        return self.from_arrow(result_arrow)

    def get_schema(self, df: ray.data.Dataset) -> Dict[str, "np.dtype"]:
        """Get Ray Dataset schema as column name to numpy dtype mapping."""
        import numpy as np

        # Convert Ray schema to numpy dtypes
        schema = df.schema()
        result = {}

        for field in schema:
            # Map Arrow types to numpy dtypes
            arrow_type = field.type
            numpy_dtype: "np.dtype[Any]"
            if pa.types.is_boolean(arrow_type):
                numpy_dtype = np.dtype("bool")
            elif pa.types.is_integer(arrow_type):
                numpy_dtype = np.dtype("int64")
            elif pa.types.is_floating(arrow_type):
                numpy_dtype = np.dtype("float64")
            elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
                numpy_dtype = np.dtype("O")  # Object dtype for strings
            elif pa.types.is_timestamp(arrow_type):
                numpy_dtype = np.dtype("datetime64[ns]")
            elif pa.types.is_date(arrow_type):
                numpy_dtype = np.dtype("datetime64[D]")
            else:
                numpy_dtype = np.dtype("O")  # Default to object

            result[field.name] = numpy_dtype

        return result

    def get_timestamp_range(self, df: ray.data.Dataset, timestamp_column: str) -> tuple:
        """Get min/max of a timestamp column in Ray Dataset."""
        # Use Ray's built-in aggregation for min/max
        stats = df.aggregate(
            ray.data.aggregate.Min(timestamp_column),
            ray.data.aggregate.Max(timestamp_column),
        )

        min_val = stats[f"min({timestamp_column})"]
        max_val = stats[f"max({timestamp_column})"]

        # Convert to datetime objects if needed
        if hasattr(min_val, "to_pydatetime"):
            min_val = min_val.to_pydatetime()
        if hasattr(max_val, "to_pydatetime"):
            max_val = max_val.to_pydatetime()

        return (min_val, max_val)
