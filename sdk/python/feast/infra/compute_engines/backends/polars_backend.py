from datetime import timedelta

import polars as pl
import pyarrow as pa

from feast.infra.compute_engines.backends.base import DataFrameBackend


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
