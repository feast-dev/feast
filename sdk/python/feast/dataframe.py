"""FeastDataFrame: A lightweight container for DataFrame-like objects in Feast."""

from enum import Enum
from typing import Any, Dict, Optional

import pandas as pd
import pyarrow as pa


class DataFrameEngine(str, Enum):
    """Supported DataFrame engines."""

    PANDAS = "pandas"
    SPARK = "spark"
    DASK = "dask"
    RAY = "ray"
    ARROW = "arrow"
    POLARS = "polars"
    UNKNOWN = "unknown"


class FeastDataFrame:
    """
    A lightweight container for DataFrame-like objects in Feast.

    This class wraps any DataFrame implementation and provides metadata
    about the engine type for proper routing in Feast's processing pipeline.
    """

    def __init__(
        self,
        data: Any,
        engine: Optional[DataFrameEngine] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize a FeastDataFrame.

        Args:
            data: The wrapped DataFrame object (pandas, Spark, Dask, etc.)
            engine: Explicitly specify the engine type (auto-detected if None)
            metadata: Additional metadata (schema hints, etc.)
        """
        self.data = data
        self.metadata = metadata or {}

        # Detect the actual engine from the data
        detected_engine = self._detect_engine()

        if engine is not None:
            # Validate that the provided engine matches the detected engine
            if engine != detected_engine:
                raise ValueError(
                    f"Provided engine '{engine.value}' does not match detected engine '{detected_engine.value}' "
                    f"for data type {type(data).__name__}"
                )
            self._engine = engine
        else:
            self._engine = detected_engine

    def _detect_engine(self) -> DataFrameEngine:
        """Auto-detect the DataFrame engine based on type."""
        if isinstance(self.data, pd.DataFrame):
            return DataFrameEngine.PANDAS
        elif isinstance(self.data, pa.Table):
            return DataFrameEngine.ARROW

        # For optional dependencies, check module name to avoid import errors
        module = type(self.data).__module__
        if "pyspark" in module:
            return DataFrameEngine.SPARK
        elif "dask" in module:
            return DataFrameEngine.DASK
        elif "ray" in module:
            return DataFrameEngine.RAY
        elif "polars" in module:
            return DataFrameEngine.POLARS
        else:
            return DataFrameEngine.UNKNOWN

    @property
    def engine(self) -> DataFrameEngine:
        """Get the detected or specified engine type."""
        return self._engine

    def __repr__(self):
        return f"FeastDataFrame(engine={self.engine.value}, type={type(self.data).__name__})"

    @property
    def is_lazy(self) -> bool:
        """Check if the underlying DataFrame is lazy (Spark, Dask, Ray)."""
        return self.engine in [
            DataFrameEngine.SPARK,
            DataFrameEngine.DASK,
            DataFrameEngine.RAY,
        ]
