"""
Utility functions for Ray compute engine.
"""

import logging
from typing import Callable, Dict, Union

import numpy as np
import pandas as pd
import pyarrow as pa

from feast.batch_feature_view import BatchFeatureView
from feast.feature_view import FeatureView
from feast.infra.online_stores.online_store import OnlineStore
from feast.repo_config import RepoConfig
from feast.stream_feature_view import StreamFeatureView
from feast.utils import _convert_arrow_to_proto
from feast.value_type import ValueType

logger = logging.getLogger(__name__)


def write_to_online_store(
    arrow_table: pa.Table,
    feature_view: Union[BatchFeatureView, StreamFeatureView, FeatureView],
    online_store: OnlineStore,
    repo_config: RepoConfig,
) -> None:
    """
    Writes Arrow table data to the online store.

    Args:
        arrow_table: Arrow table containing the data to write
        feature_view: Feature view being materialized
        online_store: Online store instance
        repo_config: Repository configuration
    """
    if not getattr(feature_view, "online", False):
        return

    try:
        join_key_to_value_type: Dict[str, ValueType] = {}
        if hasattr(feature_view, "entity_columns") and feature_view.entity_columns:
            join_key_to_value_type = {
                entity.name: entity.dtype.to_value_type()
                for entity in feature_view.entity_columns
            }

        batch_size = repo_config.materialization_config.online_write_batch_size
        # Single batch if None (backward compatible), otherwise use configured batch_size
        batches = (
            [arrow_table]
            if batch_size is None
            else arrow_table.to_batches(max_chunksize=batch_size)
        )

        total_rows = 0
        for batch in batches:
            rows_to_write = _convert_arrow_to_proto(
                batch, feature_view, join_key_to_value_type
            )

            if rows_to_write:
                online_store.online_write_batch(
                    config=repo_config,
                    table=feature_view,
                    data=rows_to_write,
                    progress=lambda x: None,
                )
                total_rows += len(rows_to_write)

        if total_rows > 0:
            logger.debug(
                f"Successfully wrote {total_rows} rows to online store for {feature_view.name}"
            )
        else:
            logger.warning(f"No rows to write for {feature_view.name}")

    except Exception as e:
        logger.error(f"Failed to write to online store for {feature_view.name}: {e}")


# Ray Data batch type: pandas DataFrame, numpy dict, or pyarrow Table
BatchType = Union[pd.DataFrame, Dict[str, np.ndarray], pa.Table]


def _is_empty_batch(batch: BatchType) -> bool:
    """Return True if the batch contains no rows, regardless of Ray Data batch format.

    Ray Data delivers batches in three formats depending on the batch_format
    argument passed to map_batches:
      - "pandas"  → pd.DataFrame   (.empty attribute)
      - "numpy"   → Dict[str, np.ndarray]  (check length of first array)
      - "pyarrow" → pa.Table       (.num_rows attribute)
    """
    if isinstance(batch, pd.DataFrame):
        return batch.empty
    if isinstance(batch, dict):
        if not batch:
            return True
        first = next(iter(batch.values()))
        return len(first) == 0
    if isinstance(batch, pa.Table):
        return batch.num_rows == 0
    return False


def safe_batch_processor(
    func: Callable[[BatchType], BatchType],
) -> Callable[[BatchType], BatchType]:
    """
    Decorator for batch processing functions that handles empty batches and
    exceptions gracefully across all Ray Data batch formats.

    Ray Data can deliver batches as a pandas DataFrame (batch_format="pandas"),
    a Dict[str, np.ndarray] (batch_format="numpy"), or a pa.Table
    (batch_format="pyarrow"). The decorator handles all three so that callers
    using gpu_batch_format="numpy" or "pyarrow" do not crash on the empty-batch
    check.

    Args:
        func: Batch processing function. Receives and returns the same batch
            type that Ray Data passes (pandas, numpy dict, or pyarrow Table).

    Returns:
        Wrapped function that skips empty batches and swallows exceptions.
    """

    def wrapper(batch: BatchType) -> BatchType:
        if _is_empty_batch(batch):
            return batch

        try:
            return func(batch)
        except Exception as e:
            logger.error(f"Batch processing failed in {func.__name__}: {e}")
            return batch

    return wrapper
