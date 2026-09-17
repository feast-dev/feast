from datetime import timedelta
from typing import TYPE_CHECKING

import pandas as pd
import pyarrow as pa

from feast.infra.compute_engines.backends.base import DataFrameBackend

if TYPE_CHECKING:
    import numpy as np


class PandasBackend(DataFrameBackend):
    def columns(self, df):
        return df.columns.tolist()

    def from_arrow(self, table):
        return table.to_pandas()

    def join(self, left, right, on, how):
        return left.merge(right, on=on, how=how)

    def groupby_agg(self, df, group_keys, agg_ops):
        return (
            df.groupby(group_keys)
            .agg(
                **{
                    alias: pd.NamedAgg(column=col, aggfunc=func)
                    for alias, (func, col) in agg_ops.items()
                }
            )
            .reset_index()
        )

    def filter(self, df, expr):
        return df.query(expr)

    def to_arrow(self, df):
        return pa.Table.from_pandas(df)

    def to_timedelta_value(self, delta: timedelta):
        return pd.to_timedelta(delta)

    def drop_duplicates(self, df, keys, sort_by, ascending: bool = False):
        return df.sort_values(by=sort_by, ascending=ascending).drop_duplicates(
            subset=keys
        )

    def rename_columns(self, df, columns: dict[str, str]):
        return df.rename(columns=columns)

    def get_schema(self, df) -> dict[str, "np.dtype"]:
        """Get pandas DataFrame schema as column name to numpy dtype mapping."""
        return {col: dtype for col, dtype in df.dtypes.items()}

    def get_timestamp_range(self, df, timestamp_column: str) -> tuple:
        """Get min/max of a timestamp column in pandas DataFrame."""
        col = df[timestamp_column]
        # Ensure it's datetime type
        if not pd.api.types.is_datetime64_any_dtype(col):
            col = pd.to_datetime(col, utc=True)
        return (col.min().to_pydatetime(), col.max().to_pydatetime())
