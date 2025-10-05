"""
Utility functions for Ray compute engine.
"""

import logging
from typing import Callable, Dict, Union

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

        rows_to_write = _convert_arrow_to_proto(
            arrow_table, feature_view, join_key_to_value_type
        )

        if rows_to_write:
            online_store.online_write_batch(
                config=repo_config,
                table=feature_view,
                data=rows_to_write,
                progress=lambda x: None,
            )
            logger.debug(
                f"Successfully wrote {len(rows_to_write)} rows to online store for {feature_view.name}"
            )
        else:
            logger.warning(f"No rows to write for {feature_view.name}")

    except Exception as e:
        logger.error(f"Failed to write to online store for {feature_view.name}: {e}")


def safe_batch_processor(
    func: Callable[[pd.DataFrame], pd.DataFrame],
) -> Callable[[pd.DataFrame], pd.DataFrame]:
    """
    Decorator for batch processing functions that handles empty batches and errors gracefully.

    Args:
        func: Function that processes a pandas DataFrame batch

    Returns:
        Wrapped function that handles empty batches and exceptions
    """

    def wrapper(batch: pd.DataFrame) -> pd.DataFrame:
        # Handle empty batches
        if batch.empty:
            return batch

        try:
            return func(batch)
        except Exception as e:
            logger.error(f"Batch processing failed in {func.__name__}: {e}")
            return batch

    return wrapper
