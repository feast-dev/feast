from typing import Optional

import pandas as pd
import pyarrow

from feast.infra.compute_engines.backends.base import DataFrameBackend
from feast.infra.compute_engines.backends.pandas_backend import PandasBackend


class BackendFactory:
    """
    Factory class for constructing DataFrameBackend implementations based on backend name
    or runtime entity_df type.
    """

    @staticmethod
    def from_name(name: str) -> DataFrameBackend:
        if name == "pandas":
            return PandasBackend()
        if name == "polars":
            return BackendFactory._get_polars_backend()
        raise ValueError(f"Unsupported backend name: {name}")

    @staticmethod
    def infer_from_entity_df(entity_df) -> Optional[DataFrameBackend]:
        if (
            not entity_df
            or isinstance(entity_df, pyarrow.Table)
            or isinstance(entity_df, pd.DataFrame)
        ):
            return PandasBackend()

        if BackendFactory._is_polars(entity_df):
            return BackendFactory._get_polars_backend()
        return None

    @staticmethod
    def _is_polars(entity_df) -> bool:
        try:
            import polars as pl
        except ImportError:
            raise ImportError(
                "Polars is not installed. Please install it to use Polars backend."
            )
        return isinstance(entity_df, pl.DataFrame)

    @staticmethod
    def _get_polars_backend():
        from feast.infra.compute_engines.backends.polars_backend import (
            PolarsBackend,
        )

        return PolarsBackend()
