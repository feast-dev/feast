import logging
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Literal, Optional, Tuple, Union

import fsspec
import numpy as np
import pandas as pd
import pyarrow as pa
import ray
import ray.data
from ray.data import Dataset
from ray.data.context import DatasetContext

from feast.data_source import DataSource
from feast.errors import (
    RequestDataNotFoundInEntityDfException,
    SavedDatasetLocationAlreadyExists,
)
from feast.feature_logging import LoggingConfig, LoggingSource
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, FeatureView
from feast.infra.offline_stores.file_source import FileSource, SavedDatasetFileStorage
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.offline_stores.offline_utils import (
    assert_expected_columns_in_entity_df,
    get_entity_df_timestamp_bounds,
    get_expected_join_keys,
    get_pyarrow_schema_from_batch_source,
    infer_event_timestamp_from_entity_df,
)
from feast.infra.registry.base_registry import BaseRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage, ValidationReference
from feast.type_map import feast_value_type_to_pandas_type
from feast.utils import _get_column_names, make_df_tzaware
from feast.value_type import ValueType

logger = logging.getLogger(__name__)


def _normalize_timestamp_column(
    df: pd.DataFrame, column: str, inplace: bool = False
) -> pd.DataFrame:
    """
    Normalize a timestamp column to UTC with second precision.
    Args:
        df: DataFrame containing the timestamp column
        column: Name of the timestamp column to normalize
        inplace: Whether to modify the DataFrame in place
    Returns:
        DataFrame with normalized timestamp column
    """
    if not inplace:
        df = df.copy()

    if column in df.columns:
        df[column] = (
            pd.to_datetime(df[column], utc=True, errors="coerce")
            .dt.floor("s")
            .astype("datetime64[ns, UTC]")
        )

    return df


def _normalize_timestamp_columns(
    df: pd.DataFrame, columns: List[str], inplace: bool = False
) -> pd.DataFrame:
    """
    Normalize multiple timestamp columns to UTC with second precision.
    Args:
        df: DataFrame containing the timestamp columns
        columns: List of timestamp column names to normalize
        inplace: Whether to modify the DataFrame in place
    Returns:
        DataFrame with normalized timestamp columns
    """
    if not inplace:
        df = df.copy()

    for column in columns:
        if column in df.columns:
            df = _normalize_timestamp_column(df, column, inplace=True)

    return df


def _create_time_window_column(
    df: pd.DataFrame,
    timestamp_column: str,
    window_size: str,
    window_column: str = "time_window",
    inplace: bool = False,
) -> pd.DataFrame:
    """
    Create a time window column by flooring timestamps to specified window size.
    Args:
        df: DataFrame containing the timestamp column
        timestamp_column: Name of the timestamp column
        window_size: Window size string (e.g., "1H", "30min")
        window_column: Name for the new window column
        inplace: Whether to modify the DataFrame in place
    Returns:
        DataFrame with added time window column
    """
    if not inplace:
        df = df.copy()

    if timestamp_column in df.columns:
        df[window_column] = (
            pd.to_datetime(df[timestamp_column])
            .dt.floor(window_size)
            .astype("datetime64[ns, UTC]")
        )

    return df


def _create_empty_timestamp_column(
    length: int, dtype: str = "datetime64[ns, UTC]"
) -> pd.Series:
    """
    Create an empty timestamp column with proper dtype.
    Args:
        length: Length of the series
        dtype: Pandas dtype for the timestamp column
    Returns:
        Series with NaT values and proper datetime dtype
    """
    return pd.Series([pd.NaT] * length, dtype=dtype)


def _ensure_timestamp_compatibility(
    df: pd.DataFrame, timestamp_fields: List[str], inplace: bool = False
) -> pd.DataFrame:
    """
    Ensure timestamp columns have compatible dtypes and precision for joins.
    Args:
        df: DataFrame to process
        timestamp_fields: List of timestamp field names
        inplace: Whether to modify the DataFrame in place
    Returns:
        DataFrame with compatible timestamp columns
    """
    if not inplace:
        df = df.copy()

    # Use existing utility for timezone awareness
    df = make_df_tzaware(df)

    # Then normalize timestamp precision for specified fields only
    for field in timestamp_fields:
        if field in df.columns:
            df = _normalize_timestamp_column(df, field, inplace=True)

    return df


def _create_empty_dataframe_with_timestamp_columns(
    columns: List[str], timestamp_columns: List[str]
) -> pd.DataFrame:
    """
    Create an empty DataFrame with proper column types including datetime columns.
    Args:
        columns: List of all column names
        timestamp_columns: List of timestamp column names that need proper dtype
    Returns:
        Empty DataFrame with proper column types
    """
    df = pd.DataFrame(columns=columns)

    # Set proper dtype for timestamp columns
    for col in timestamp_columns:
        if col in df.columns:
            df[col] = df[col].astype("datetime64[ns, UTC]")

    return df


def _safe_infer_event_timestamp_column(
    entity_df: pd.DataFrame, fallback_column: str = "event_timestamp"
) -> str:
    """
    Safely infer the event timestamp column using offline_utils with fallback.
    Args:
        entity_df: Entity DataFrame to analyze
        fallback_column: Default column name to use if inference fails
    Returns:
        Inferred or fallback timestamp column name
    """
    try:
        return infer_event_timestamp_from_entity_df(entity_df.dtypes.to_dict())
    except Exception as e:
        logger.debug(
            f"Timestamp column inference failed: {e}, using fallback: {fallback_column}"
        )
        return fallback_column


def _safe_get_entity_timestamp_bounds(
    entity_df: pd.DataFrame, timestamp_column: str
) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Safely get entity timestamp bounds using offline_utils with fallback.
    Args:
        entity_df: Entity DataFrame
        timestamp_column: Name of timestamp column
    Returns:
        Tuple of (min_timestamp, max_timestamp) or (None, None) if failed
    """
    try:
        if timestamp_column in entity_df.columns:
            min_ts, max_ts = get_entity_df_timestamp_bounds(entity_df, timestamp_column)
            # Convert Pandas Timestamp to datetime if needed
            if hasattr(min_ts, "to_pydatetime"):
                min_ts = min_ts.to_pydatetime()
            if hasattr(max_ts, "to_pydatetime"):
                max_ts = max_ts.to_pydatetime()
            return min_ts, max_ts
    except Exception as e:
        logger.debug(
            f"Timestamp bounds extraction failed: {e}, falling back to manual calculation"
        )

    # Fallback to original logic
    try:
        if timestamp_column in entity_df.columns:
            timestamps = pd.to_datetime(entity_df[timestamp_column], utc=True)
            return timestamps.min().to_pydatetime(), timestamps.max().to_pydatetime()
    except Exception:
        pass

    return None, None


def _safe_validate_entity_dataframe(
    entity_df: pd.DataFrame,
    feature_views: List[FeatureView],
    project: str,
    registry: BaseRegistry,
) -> None:
    """
    Safely validate entity DataFrame using offline_utils with graceful fallback.
    Args:
        entity_df: Entity DataFrame to validate
        feature_views: List of feature views to validate against
        project: Feast project name
        registry: Feature registry
    """
    try:
        # Get expected join keys for validation
        expected_join_keys = get_expected_join_keys(project, feature_views, registry)

        # Infer event timestamp column
        timestamp_col = infer_event_timestamp_from_entity_df(entity_df.dtypes.to_dict())

        # Validate entity DataFrame has required columns
        assert_expected_columns_in_entity_df(
            entity_df.dtypes.to_dict(), expected_join_keys, timestamp_col
        )

        logger.info(
            f"Entity DataFrame validation passed:\n"
            f"  Expected join keys: {expected_join_keys}\n"
            f"  Detected timestamp column: {timestamp_col}"
        )

    except Exception as e:
        # Log validation issues but don't fail
        logger.warning(f"Entity DataFrame validation skipped due to error: {e}")
        logger.debug("Validation error details:", exc_info=True)


def _safe_validate_schema(
    config: RepoConfig,
    data_source: DataSource,
    table_columns: List[str],
    operation_name: str = "operation",
) -> Optional[Tuple[pa.Schema, List[str]]]:
    """
    Safely validate schema using offline_utils with graceful fallback.
    Args:
        config: Repo configuration
        data_source: Data source to validate against
        table_columns: Actual table column names
        operation_name: Name of operation for logging
    Returns:
        Tuple of (expected_schema, expected_columns) or None if validation fails
    """
    try:
        expected_schema, expected_columns = get_pyarrow_schema_from_batch_source(
            config, data_source
        )

        if set(expected_columns) != set(table_columns):
            logger.warning(
                f"Schema mismatch in {operation_name}:\n"
                f"  Expected columns: {expected_columns}\n"
                f"  Actual columns: {table_columns}"
            )

            # Check if it's just a column order issue
            if set(expected_columns) == set(table_columns):
                logger.info(f"Columns match but order differs for {operation_name}")
                return expected_schema, expected_columns
        else:
            logger.debug(f"Schema validation passed for {operation_name}")
            return expected_schema, expected_columns

    except Exception as e:
        logger.warning(
            f"Schema validation skipped for {operation_name} due to error: {e}"
        )
        logger.debug("Schema validation error details:", exc_info=True)

    return None


def _convert_feature_column_types(
    batch: pd.DataFrame, feature_views: List[FeatureView]
) -> pd.DataFrame:
    """
    Convert feature columns to appropriate pandas types using Feast's type mapping utilities.
    Args:
        batch: DataFrame containing feature data
        feature_views: List of feature views with type information
    Returns:
        DataFrame with properly converted feature column types
    """
    batch = batch.copy()

    for fv in feature_views:
        for feature in fv.features:
            feat_name = feature.name

            # Check if this feature exists in the batch
            if feat_name not in batch.columns:
                continue

            try:
                # Get the Feast ValueType for this feature
                value_type = feature.dtype.to_value_type()

                # Handle array/list types
                if value_type.name.endswith("_LIST"):
                    batch[feat_name] = _convert_array_column(
                        batch[feat_name], value_type
                    )
                else:
                    # Handle scalar types using feast type mapping
                    target_pandas_type = feast_value_type_to_pandas_type(value_type)
                    batch[feat_name] = _convert_scalar_column(
                        batch[feat_name], value_type, target_pandas_type
                    )

            except Exception as e:
                logger.warning(
                    f"Failed to convert feature {feat_name} to proper type: {e}"
                )
                # Keep original dtype if conversion fails
                continue

    return batch


def _convert_scalar_column(
    series: pd.Series, value_type: ValueType, target_pandas_type: str
) -> pd.Series:
    """Convert a scalar feature column to the appropriate pandas type."""
    if value_type == ValueType.INT32:
        return pd.to_numeric(series, errors="coerce").astype("Int32")
    elif value_type == ValueType.INT64:
        return pd.to_numeric(series, errors="coerce").astype("Int64")
    elif value_type in [ValueType.FLOAT, ValueType.DOUBLE]:
        return pd.to_numeric(series, errors="coerce").astype("float64")
    elif value_type == ValueType.BOOL:
        return series.astype("boolean")
    elif value_type == ValueType.STRING:
        return series.astype("string")
    elif value_type == ValueType.UNIX_TIMESTAMP:
        return pd.to_datetime(series, unit="s", errors="coerce")
    else:
        # For other types, use pandas default conversion
        return series.astype(target_pandas_type)


def _convert_array_column(series: pd.Series, value_type: ValueType) -> pd.Series:
    """Convert an array feature column to the appropriate type with proper empty array handling."""
    # Determine the base type for array elements
    base_type_map = {
        ValueType.INT32_LIST: np.int32,
        ValueType.INT64_LIST: np.int64,
        ValueType.FLOAT_LIST: np.float32,
        ValueType.DOUBLE_LIST: np.float64,
        ValueType.BOOL_LIST: np.bool_,
        ValueType.STRING_LIST: object,
        ValueType.BYTES_LIST: object,
        ValueType.UNIX_TIMESTAMP_LIST: "datetime64[s]",
    }

    target_dtype = base_type_map.get(value_type, object)

    def convert_array_item(item):
        if item is None or (isinstance(item, list) and len(item) == 0):
            # Return properly typed empty array
            if target_dtype == object:
                return np.array([], dtype=object)
            else:
                return np.array([], dtype=target_dtype)
        else:
            # Return the item as-is for non-empty arrays
            return item

    return series.apply(convert_array_item)


class RayOfflineStoreConfig(FeastConfigBaseModel):
    """
    Configuration for the Ray Offline Store.
    """

    type: Literal[
        "feast.offline_stores.contrib.ray_offline_store.ray.RayOfflineStore", "ray"
    ] = "ray"
    storage_path: Optional[str] = None
    ray_address: Optional[str] = None
    use_ray_cluster: Optional[bool] = False

    # Optimization settings
    broadcast_join_threshold_mb: Optional[int] = 100
    enable_distributed_joins: Optional[bool] = True
    max_parallelism_multiplier: Optional[int] = 2
    target_partition_size_mb: Optional[int] = 64
    window_size_for_joins: Optional[str] = "1H"


class RayResourceManager:
    """
    Manages Ray cluster resources for optimal performance.
    """

    def __init__(self, config: Optional[RayOfflineStoreConfig] = None):
        """
        Initialize the resource manager with cluster resource information.
        """
        self.config = config or RayOfflineStoreConfig()
        self.cluster_resources = ray.cluster_resources()
        self.available_memory = self.cluster_resources.get("memory", 8 * 1024**3)
        self.available_cpus = int(self.cluster_resources.get("CPU", 4))
        self.num_nodes = len(ray.nodes()) if ray.is_initialized() else 1

    def configure_ray_context(self) -> None:
        """
        Configure Ray DatasetContext for optimal performance based on available resources.
        """
        ctx = DatasetContext.get_current()

        if self.available_memory > 32 * 1024**3:
            ctx.target_shuffle_buffer_size = 2 * 1024**3
            ctx.target_max_block_size = 512 * 1024**2
        else:
            ctx.target_shuffle_buffer_size = 512 * 1024**2
            ctx.target_max_block_size = 128 * 1024**2
        ctx.min_parallelism = self.available_cpus
        multiplier = (
            self.config.max_parallelism_multiplier
            if self.config.max_parallelism_multiplier is not None
            else 2
        )
        ctx.max_parallelism = self.available_cpus * multiplier
        ctx.shuffle_strategy = "sort"  # type: ignore
        ctx.enable_tensor_extension_casting = False
        logger.info(
            f"Configured Ray context: {self.available_cpus} CPUs, "
            f"{self.available_memory // 1024**3}GB memory, {self.num_nodes} nodes"
        )

    def estimate_optimal_partitions(self, dataset_size_bytes: int) -> int:
        """
        Estimate optimal number of partitions for a dataset based on size and resources.
        """
        target_partition_size = (self.config.target_partition_size_mb or 64) * 1024**2
        size_based_partitions = max(1, dataset_size_bytes // target_partition_size)
        max_partitions = self.available_cpus * (
            self.config.max_parallelism_multiplier or 2
        )
        return min(size_based_partitions, max_partitions)

    def should_use_broadcast_join(
        self, dataset_size_bytes: int, threshold_mb: Optional[int] = None
    ) -> bool:
        """
        Determine if dataset is small enough for broadcast join.
        """
        threshold = (
            threshold_mb
            if threshold_mb is not None
            else (self.config.broadcast_join_threshold_mb or 100)
        )
        return dataset_size_bytes <= threshold * 1024**2

    def estimate_processing_requirements(
        self, dataset_size_bytes: int, operation_type: str
    ) -> Dict[str, Any]:
        """
        Estimate resource requirements for different operations.
        """
        memory_multiplier = {
            "read": 1.2,  # 20% overhead for reading
            "join": 3.0,  # 3x for join operations
            "aggregate": 2.0,  # 2x for aggregations
            "shuffle": 2.5,  # 2.5x for shuffling
        }
        required_memory = dataset_size_bytes * memory_multiplier.get(
            operation_type, 2.0
        )
        return {
            "required_memory": required_memory,
            "optimal_partitions": self.estimate_optimal_partitions(dataset_size_bytes),
            "can_fit_in_memory": required_memory <= self.available_memory * 0.8,
            "should_broadcast": self.should_use_broadcast_join(dataset_size_bytes),
        }


class RayDataProcessor:
    """
    Optimized data processing with Ray for feature store operations.
    """

    def __init__(self, resource_manager: RayResourceManager):
        """
        Initialize the data processor with a resource manager.
        """
        self.resource_manager = resource_manager

    def optimize_dataset_for_join(self, ds: Dataset, join_keys: List[str]) -> Dataset:
        """
        Optimize dataset partitioning for join operations.
        """
        dataset_size = ds.size_bytes()
        optimal_partitions = self.resource_manager.estimate_optimal_partitions(
            dataset_size
        )
        if not join_keys:
            # For datasets without join keys, use simple repartitioning
            return ds.repartition(num_blocks=optimal_partitions)
        # For datasets with join keys, use shuffle for better distribution
        return ds.random_shuffle(num_blocks=optimal_partitions)

    def _manual_point_in_time_join(
        self,
        batch_df: pd.DataFrame,
        features_df: pd.DataFrame,
        join_keys: List[str],
        feature_join_keys: List[str],
        timestamp_field: str,
        requested_feats: List[str],
    ) -> pd.DataFrame:
        """
        Perform manual point-in-time join when merge_asof fails.

        This method handles cases where merge_asof cannot be used due to:
        - Entity mapping (different column names)
        - Complex multi-entity joins
        - Sorting issues with the data
        """
        result = batch_df.copy()
        for feat in requested_feats:
            is_list_feature = False
            if feat in features_df.columns:
                sample_values = features_df[feat].dropna()
                if not sample_values.empty:
                    sample_value = sample_values.iloc[0]
                    if isinstance(sample_value, (list, np.ndarray)):
                        is_list_feature = True
                    elif (
                        features_df[feat].dtype == object
                        and sample_values.apply(
                            lambda x: isinstance(x, (list, np.ndarray))
                        ).any()
                    ):
                        is_list_feature = True

            if is_list_feature:
                result[feat] = [[] for _ in range(len(result))]
            else:
                # Check if the feature column is datetime
                if feat in features_df.columns and pd.api.types.is_datetime64_any_dtype(
                    features_df[feat]
                ):
                    result[feat] = _create_empty_timestamp_column(len(result))
                else:
                    result[feat] = np.nan

        for _, entity_row in batch_df.iterrows():
            entity_matches = pd.Series(
                [True] * len(features_df), index=features_df.index
            )
            for entity_key, feature_key in zip(join_keys, feature_join_keys):
                if entity_key in entity_row and feature_key in features_df.columns:
                    entity_value = entity_row[entity_key]
                    feature_column = features_df[feature_key]
                    if pd.api.types.is_scalar(entity_value):
                        entity_matches &= feature_column == entity_value
                    else:
                        if hasattr(entity_value, "__len__") and len(entity_value) > 0:
                            entity_matches &= feature_column.isin(entity_value)
                        else:
                            entity_matches &= pd.Series(
                                [False] * len(features_df), index=features_df.index
                            )

            if not entity_matches.any():
                continue

            matching_features = features_df[entity_matches]

            if matching_features.empty:
                continue

            entity_timestamp = entity_row[timestamp_field]
            if timestamp_field in matching_features.columns:
                time_matches = matching_features[timestamp_field] <= entity_timestamp
                matching_features = matching_features[time_matches]

            if matching_features.empty:
                continue

            if timestamp_field in matching_features.columns:
                matching_features = matching_features.sort_values(timestamp_field)
                latest_feature = matching_features.iloc[-1]
            else:
                latest_feature = matching_features.iloc[-1]

            entity_index = entity_row.name
            for feat in requested_feats:
                if feat in latest_feature:
                    feature_value = latest_feature[feat]
                    if pd.api.types.is_scalar(feature_value):
                        if pd.notna(feature_value):
                            result.loc[entity_index, feat] = feature_value
                    elif isinstance(feature_value, (list, tuple, np.ndarray)):
                        result.at[entity_index, feat] = feature_value
                    else:
                        try:
                            if pd.notna(feature_value):
                                result.at[entity_index, feat] = feature_value
                        except (ValueError, TypeError):
                            if feature_value is not None:
                                result.at[entity_index, feat] = feature_value

        return result

    def broadcast_join_features(
        self,
        entity_ds: Dataset,
        feature_df: pd.DataFrame,
        join_keys: List[str],
        timestamp_field: str,
        requested_feats: List[str],
        full_feature_names: bool = False,
        feature_view_name: Optional[str] = None,
        original_join_keys: Optional[List[str]] = None,
    ) -> Dataset:
        """Perform broadcast join for small feature datasets."""

        # Put feature data in Ray object store for efficient broadcasting
        feature_ref = ray.put(feature_df)

        def join_batch_with_features(batch: pd.DataFrame) -> pd.DataFrame:
            """Join a batch with broadcast feature data."""
            features = ray.get(feature_ref)

            logger.info(
                f"Processing feature view {feature_view_name} with join keys {join_keys}"
            )

            # Determine feature join keys
            # For entity mapping (join key mapping), original_join_keys contains the original feature view join keys
            # join_keys contains the mapped entity join keys
            if original_join_keys:
                # Entity mapping case: entity has join_keys, features have original_join_keys
                feature_join_keys = original_join_keys
                entity_join_keys = join_keys
            else:
                # Normal case: both use the same join keys
                feature_join_keys = join_keys
                entity_join_keys = join_keys

            # Select only required feature columns plus join keys and timestamp
            feature_cols = [timestamp_field] + feature_join_keys + requested_feats

            # Only include columns that actually exist in the features DataFrame
            available_feature_cols = [
                col for col in feature_cols if col in features.columns
            ]

            # Ensure we have the minimum required columns
            if timestamp_field not in available_feature_cols:
                raise ValueError(
                    f"Timestamp field '{timestamp_field}' not found in features columns: {list(features.columns)}"
                )

            # Check if required feature columns exist
            missing_feats = [
                feat for feat in requested_feats if feat not in features.columns
            ]
            if missing_feats:
                raise ValueError(
                    f"Requested features {missing_feats} not found in features columns: {list(features.columns)}"
                )

            features_filtered = features[available_feature_cols].copy()

            # Ensure timestamp columns have compatible dtypes and precision
            batch = _normalize_timestamp_column(batch, timestamp_field, inplace=True)
            features_filtered = _normalize_timestamp_column(
                features_filtered, timestamp_field, inplace=True
            )

            if not entity_join_keys:
                # Temporal join without entity keys
                batch_sorted = batch.sort_values(timestamp_field).reset_index(drop=True)
                features_sorted = features_filtered.sort_values(
                    timestamp_field
                ).reset_index(drop=True)
                result = pd.merge_asof(
                    batch_sorted,
                    features_sorted,
                    on=timestamp_field,
                    direction="backward",
                )
            else:
                # Ensure entity join keys exist in batch
                for key in entity_join_keys:
                    if key not in batch.columns:
                        batch[key] = np.nan

                # Ensure feature join keys exist in features
                for key in feature_join_keys:
                    if key not in features_filtered.columns:
                        features_filtered[key] = np.nan

                # Drop rows with NaN values in join keys or timestamp
                batch_clean = batch.dropna(
                    subset=entity_join_keys + [timestamp_field]
                ).copy()
                features_clean = features_filtered.dropna(
                    subset=feature_join_keys + [timestamp_field]
                ).copy()

                # If no valid data remains, return empty result
                if batch_clean.empty or features_clean.empty:
                    return batch.head(0)  # Return empty dataframe with same columns

                # Sort both DataFrames for merge_asof requirements
                # merge_asof requires: left sorted by 'on' column, right sorted by ['by'] + ['on'] columns

                # For the left DataFrame (batch), sort by timestamp (on column)
                if timestamp_field in batch_clean.columns:
                    batch_sorted = batch_clean.sort_values(
                        timestamp_field, ascending=True
                    ).reset_index(drop=True)
                else:
                    batch_sorted = batch_clean.reset_index(drop=True)

                # For the right DataFrame (features), sort by join keys (by columns) + timestamp (on column)
                right_sort_columns = []

                # Add join keys to sort columns (these are the 'by' columns for merge_asof)
                for key in feature_join_keys:
                    if key in features_clean.columns:
                        right_sort_columns.append(key)

                # Add timestamp field to sort columns (this is the 'on' column for merge_asof)
                if timestamp_field in features_clean.columns:
                    right_sort_columns.append(timestamp_field)

                # Sort the right DataFrame
                if right_sort_columns:
                    # Remove duplicates first, then sort
                    features_clean = features_clean.drop_duplicates(
                        subset=right_sort_columns, keep="last"
                    )
                    features_sorted = features_clean.sort_values(
                        right_sort_columns, ascending=True
                    ).reset_index(drop=True)
                else:
                    features_sorted = features_clean.reset_index(drop=True)

                # Verify sorting for merge_asof
                if (
                    timestamp_field in features_sorted.columns
                    and len(features_sorted) > 1
                ):
                    # Check if timestamp is monotonic within each group
                    if feature_join_keys:
                        # Group by join keys and check if timestamp is monotonic within each group
                        grouped = features_sorted.groupby(feature_join_keys, sort=False)
                        for name, group in grouped:
                            if not group[timestamp_field].is_monotonic_increasing:
                                # If not monotonic, sort again more carefully
                                features_sorted = features_sorted.sort_values(
                                    feature_join_keys + [timestamp_field],
                                    ascending=True,
                                ).reset_index(drop=True)
                                break
                    else:
                        # No join keys, just check timestamp monotonicity
                        if not features_sorted[timestamp_field].is_monotonic_increasing:
                            features_sorted = features_sorted.sort_values(
                                timestamp_field, ascending=True
                            ).reset_index(drop=True)

                # Attempt merge_asof with proper error handling
                try:
                    # Remove duplicates from both DataFrames before merge_asof
                    if feature_join_keys:
                        # For batch DataFrame, remove duplicates based on join keys + timestamp
                        batch_dedup_cols = [
                            k for k in entity_join_keys if k in batch_sorted.columns
                        ]
                        if timestamp_field in batch_sorted.columns:
                            batch_dedup_cols.append(timestamp_field)
                        if batch_dedup_cols:
                            batch_sorted = batch_sorted.drop_duplicates(
                                subset=batch_dedup_cols, keep="last"
                            )

                        # For features DataFrame, remove duplicates based on join keys + timestamp
                        feature_dedup_cols = [
                            k for k in feature_join_keys if k in features_sorted.columns
                        ]
                        if timestamp_field in features_sorted.columns:
                            feature_dedup_cols.append(timestamp_field)
                        if feature_dedup_cols:
                            features_sorted = features_sorted.drop_duplicates(
                                subset=feature_dedup_cols, keep="last"
                            )

                    # Perform merge_asof
                    if feature_join_keys:
                        # Handle join keys properly - if they are the same, just use one set
                        if entity_join_keys == feature_join_keys:
                            result = pd.merge_asof(
                                batch_sorted,
                                features_sorted,
                                on=timestamp_field,
                                by=entity_join_keys,
                                direction="backward",
                                suffixes=("", "_right"),
                            )
                        else:
                            # Different join keys, use left_by and right_by parameters
                            result = pd.merge_asof(
                                batch_sorted,
                                features_sorted,
                                on=timestamp_field,
                                left_by=entity_join_keys,
                                right_by=feature_join_keys,
                                direction="backward",
                                suffixes=("", "_right"),
                            )
                    else:
                        result = pd.merge_asof(
                            batch_sorted,
                            features_sorted,
                            on=timestamp_field,
                            direction="backward",
                            suffixes=("", "_right"),
                        )

                except Exception as e:
                    logger.warning(
                        f"merge_asof failed: {e}, implementing manual point-in-time join"
                    )
                    # Fall back to manual join
                    result = self._manual_point_in_time_join(
                        batch_clean,
                        features_clean,
                        entity_join_keys,
                        feature_join_keys,
                        timestamp_field,
                        requested_feats,
                    )
            # Handle feature renaming if full_feature_names is True
            if full_feature_names and feature_view_name:
                for feat in requested_feats:
                    if feat in result.columns:
                        new_name = f"{feature_view_name}__{feat}"
                        result[new_name] = result[feat]
                        result = result.drop(columns=[feat])

            return result

        return entity_ds.map_batches(join_batch_with_features, batch_format="pandas")

    def windowed_temporal_join(
        self,
        entity_ds: Dataset,
        feature_ds: Dataset,
        join_keys: List[str],
        timestamp_field: str,
        requested_feats: List[str],
        window_size: Optional[str] = None,
        full_feature_names: bool = False,
        feature_view_name: Optional[str] = None,
        original_join_keys: Optional[List[str]] = None,
    ) -> Dataset:
        """Perform windowed temporal join for large datasets."""

        # Use configured window size if not provided
        window_size = window_size or (
            self.resource_manager.config.window_size_for_joins or "1H"
        )

        # Step 1: Optimize both datasets for joining
        entity_optimized = self.optimize_dataset_for_join(entity_ds, join_keys)
        feature_optimized = self.optimize_dataset_for_join(feature_ds, join_keys)

        # Step 2: Add time windows and data source markers
        entity_windowed = self._add_time_windows_and_source_marker(
            entity_optimized, timestamp_field, "entity", window_size
        )
        feature_windowed = self._add_time_windows_and_source_marker(
            feature_optimized, timestamp_field, "feature", window_size
        )

        # Step 3: Union datasets for co-processing
        combined_ds = entity_windowed.union(feature_windowed)

        # Step 4: Group by time window and join keys, then apply point-in-time logic
        result_ds = combined_ds.map_batches(
            self._apply_windowed_point_in_time_logic,
            batch_format="pandas",
            fn_kwargs={
                "timestamp_field": timestamp_field,
                "join_keys": join_keys,
                "requested_feats": requested_feats,
                "full_feature_names": full_feature_names,
                "feature_view_name": feature_view_name,
                "original_join_keys": original_join_keys,
            },
        )

        return result_ds

    def _add_time_windows_and_source_marker(
        self, ds: Dataset, timestamp_field: str, source_marker: str, window_size: str
    ) -> Dataset:
        """Add time windows and source markers to dataset."""

        def add_window_and_source(batch: pd.DataFrame) -> pd.DataFrame:
            batch = _create_time_window_column(
                batch, timestamp_field, window_size, "time_window"
            )
            batch["_data_source"] = source_marker
            return batch

        return ds.map_batches(add_window_and_source, batch_format="pandas")

    def _apply_windowed_point_in_time_logic(
        self,
        batch: pd.DataFrame,
        timestamp_field: str,
        join_keys: List[str],
        requested_feats: List[str],
        full_feature_names: bool = False,
        feature_view_name: Optional[str] = None,
        original_join_keys: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Apply point-in-time correctness within time windows."""

        if len(batch) == 0:
            return pd.DataFrame()

        # Group by window and join keys to apply merge_asof
        result_chunks = []
        group_keys = ["time_window"] + join_keys

        for group_values, group_data in batch.groupby(group_keys):
            # Separate entity and feature data
            entity_data = group_data[group_data["_data_source"] == "entity"].copy()
            feature_data = group_data[group_data["_data_source"] == "feature"].copy()

            if len(entity_data) > 0 and len(feature_data) > 0:
                # Drop helper columns for merge_asof
                entity_clean = entity_data.drop(columns=["time_window", "_data_source"])
                feature_clean = feature_data.drop(
                    columns=["time_window", "_data_source"]
                )

                # Apply merge_asof within the group
                if join_keys:
                    merged = pd.merge_asof(
                        entity_clean.sort_values(join_keys + [timestamp_field]),
                        feature_clean.sort_values(join_keys + [timestamp_field]),
                        on=timestamp_field,
                        by=join_keys,
                        direction="backward",
                    )
                else:
                    merged = pd.merge_asof(
                        entity_clean.sort_values(timestamp_field),
                        feature_clean.sort_values(timestamp_field),
                        on=timestamp_field,
                        direction="backward",
                    )

                result_chunks.append(merged)
            elif len(entity_data) > 0:
                # No features found, return entity data with NaN features
                entity_clean = entity_data.drop(columns=["time_window", "_data_source"])
                for feat in requested_feats:
                    if feat not in entity_clean.columns:
                        entity_clean[feat] = np.nan
                result_chunks.append(entity_clean)

        if result_chunks:
            result = pd.concat(result_chunks, ignore_index=True)

            # Handle feature renaming if full_feature_names is True
            if full_feature_names and feature_view_name:
                for feat in requested_feats:
                    if feat in result.columns:
                        new_name = f"{feature_view_name}__{feat}"
                        result[new_name] = result[feat]
                        result = result.drop(columns=[feat])

            return result
        else:
            return pd.DataFrame()


class RayRetrievalJob(RetrievalJob):
    def __init__(
        self,
        dataset_or_callable: Union[
            Dataset, pd.DataFrame, Callable[[], Union[Dataset, pd.DataFrame]]
        ],
        staging_location: Optional[str] = None,
    ):
        self._dataset_or_callable = dataset_or_callable
        self._staging_location = staging_location
        self._cached_df: Optional[pd.DataFrame] = None
        self._cached_dataset: Optional[Dataset] = None
        self._metadata: Optional[RetrievalMetadata] = None
        self._full_feature_names: bool = False
        self._on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None
        self._feature_refs: List[str] = []
        self._entity_df: Optional[pd.DataFrame] = None

    def _create_metadata(self) -> RetrievalMetadata:
        """Create metadata from the entity DataFrame and feature references."""
        if self._entity_df is not None:
            # Auto-detect timestamp column and get timestamp bounds using utilities
            timestamp_col = _safe_infer_event_timestamp_column(
                self._entity_df, "event_timestamp"
            )
            min_timestamp, max_timestamp = _safe_get_entity_timestamp_bounds(
                self._entity_df, timestamp_col
            )

            # Get keys (all columns except the detected timestamp column)
            keys = [col for col in self._entity_df.columns if col != timestamp_col]
        else:
            min_timestamp = None
            max_timestamp = None
            keys = []

        return RetrievalMetadata(
            features=self._feature_refs,
            keys=keys,
            min_event_timestamp=min_timestamp,
            max_event_timestamp=max_timestamp,
        )

    def _set_metadata_info(self, feature_refs: List[str], entity_df: pd.DataFrame):
        """Set the feature references and entity DataFrame for metadata creation."""
        self._feature_refs = feature_refs
        self._entity_df = entity_df

    def _resolve(self) -> Union[Dataset, pd.DataFrame]:
        if callable(self._dataset_or_callable):
            result = self._dataset_or_callable()
        else:
            result = self._dataset_or_callable
        return result

    def to_df(
        self,
        validation_reference: Optional[ValidationReference] = None,
        timeout: Optional[int] = None,
    ) -> pd.DataFrame:
        # Use cached DataFrame if available for repeated access
        if self._cached_df is not None and not self.on_demand_feature_views:
            df = self._cached_df
        else:
            # If we have on-demand feature views, use the parent's implementation
            # which calls to_arrow and applies the transformations
            if self.on_demand_feature_views:
                logger.info(
                    f"Using parent implementation for {len(self.on_demand_feature_views)} ODFVs"
                )
                df = super().to_df(
                    validation_reference=validation_reference, timeout=timeout
                )
            else:
                result = self._resolve()
                if isinstance(result, pd.DataFrame):
                    df = result
                else:
                    df = result.to_pandas()
                self._cached_df = df

        # Handle validation reference if provided
        if validation_reference:
            try:
                # Import here to avoid circular imports
                from feast.dqm.errors import ValidationFailed

                # Run validation using the validation reference
                validation_result = validation_reference.profile.validate(df)
                if not validation_result.is_success:
                    raise ValidationFailed(validation_result)
            except ImportError:
                logger.warning("DQM profiler not available, skipping validation")
            except Exception as e:
                logger.error(f"Validation failed: {e}")
                raise ValueError(f"Data validation failed: {e}")
        return df

    def to_arrow(
        self,
        validation_reference: Optional[ValidationReference] = None,
        timeout: Optional[int] = None,
    ) -> pa.Table:
        # If we have ODFVs, use the parent's implementation
        if self.on_demand_feature_views:
            return super().to_arrow(
                validation_reference=validation_reference, timeout=timeout
            )

        # For non-ODFV cases, use direct conversion
        result = self._resolve()
        if isinstance(result, pd.DataFrame):
            return pa.Table.from_pandas(result)

        # For Ray Dataset, use direct Arrow conversion if available
        try:
            if hasattr(result, "to_arrow"):
                return result.to_arrow()
            else:
                # Fallback to pandas conversion
                return pa.Table.from_pandas(result.to_pandas())
        except Exception:
            # Fallback to pandas conversion
            return pa.Table.from_pandas(result.to_pandas())

    def to_remote_storage(self) -> list[str]:
        if not self._staging_location:
            raise ValueError("Staging location must be set for remote materialization.")
        try:
            ds = self._resolve()
            RayOfflineStore._ensure_ray_initialized()
            output_uri = os.path.join(self._staging_location, str(uuid.uuid4()))
            ds.write_parquet(output_uri)
            return [output_uri]
        except Exception as e:
            raise RuntimeError(f"Failed to write to remote storage: {e}")

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        """Return metadata information about retrieval."""
        if self._metadata is None:
            self._metadata = self._create_metadata()
        return self._metadata

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        return self._on_demand_feature_views or []

    def to_sql(self) -> str:
        raise NotImplementedError("SQL export not supported for Ray offline store")

    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        return self._resolve().to_pandas()

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pa.Table:
        result = self._resolve()
        if isinstance(result, pd.DataFrame):
            return pa.Table.from_pandas(result)

        # For Ray Dataset, convert to pandas first then to arrow
        df = result.to_pandas()
        return pa.Table.from_pandas(df)

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: Optional[bool] = False,
        timeout: Optional[int] = None,
    ) -> str:
        """Persist the dataset to storage."""

        if not isinstance(storage, SavedDatasetFileStorage):
            raise ValueError(
                f"Ray offline store only supports SavedDatasetFileStorage, got {type(storage)}"
            )
        destination_path = storage.file_options.uri
        if not destination_path.startswith(("s3://", "gs://", "hdfs://")):
            if not allow_overwrite and os.path.exists(destination_path):
                raise SavedDatasetLocationAlreadyExists(location=destination_path)
        try:
            result = self._resolve()
            if not destination_path.startswith(("s3://", "gs://", "hdfs://")):
                os.makedirs(os.path.dirname(destination_path), exist_ok=True)

            # Handle both DataFrame and Ray Dataset
            if isinstance(result, pd.DataFrame):
                # For DataFrame, convert to Ray Dataset first
                RayOfflineStore._ensure_ray_initialized()
                ds = ray.data.from_pandas(result)
                ds.write_parquet(destination_path)
            else:
                # For Ray Dataset, use direct write
                result.write_parquet(destination_path)

            return destination_path
        except Exception as e:
            raise RuntimeError(f"Failed to persist dataset to {destination_path}: {e}")


class RayOfflineStore(OfflineStore):
    def __init__(self):
        self._staging_location: Optional[str] = None
        self._ray_initialized: bool = False
        self._resource_manager: Optional[RayResourceManager] = None
        self._data_processor: Optional[RayDataProcessor] = None

    @staticmethod
    def _ensure_ray_initialized(config: Optional[RepoConfig] = None):
        """Ensure Ray is initialized with proper configuration."""
        if not ray.is_initialized():
            if config and hasattr(config, "offline_store"):
                ray_config = config.offline_store
                if isinstance(ray_config, RayOfflineStoreConfig):
                    if ray_config.use_ray_cluster and ray_config.ray_address:
                        ray.init(
                            address=ray_config.ray_address,
                            ignore_reinit_error=True,
                            include_dashboard=False,
                        )
                    else:
                        ray.init(
                            _node_ip_address=os.getenv("RAY_NODE_IP", "127.0.0.1"),
                            num_cpus=os.cpu_count() or 4,
                            ignore_reinit_error=True,
                            include_dashboard=False,
                        )
                else:
                    ray.init(ignore_reinit_error=True)
            else:
                ray.init(ignore_reinit_error=True)

        ctx = DatasetContext.get_current()
        ctx.shuffle_strategy = "sort"  # type: ignore
        ctx.enable_tensor_extension_casting = False

    def _init_ray(self, config: RepoConfig):
        ray_config = config.offline_store
        assert isinstance(ray_config, RayOfflineStoreConfig)
        self._ensure_ray_initialized(config)
        if self._resource_manager is None:
            self._resource_manager = RayResourceManager(ray_config)
            self._resource_manager.configure_ray_context()
        if self._data_processor is None:
            self._data_processor = RayDataProcessor(self._resource_manager)

    def _get_source_path(self, source: DataSource, config: RepoConfig) -> str:
        if not isinstance(source, FileSource):
            raise ValueError("RayOfflineStore currently only supports FileSource")
        repo_path = getattr(config, "repo_path", None)
        uri = FileSource.get_uri_for_file_path(repo_path, source.path)
        return uri

    @staticmethod
    def _create_filtered_dataset(
        source_path: str,
        timestamp_field: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> Dataset:
        """Helper method to create a filtered dataset based on timestamp range."""
        ds = ray.data.read_parquet(source_path)

        try:
            col_names = ds.schema().names
            if timestamp_field not in col_names:
                raise ValueError(
                    f"Timestamp field '{timestamp_field}' not found in columns: {col_names}"
                )
        except Exception as e:
            raise ValueError(f"Failed to get dataset schema: {e}")

        if start_date or end_date:
            try:
                if start_date and end_date:

                    def filter_func(row):
                        try:
                            ts = row[timestamp_field]
                            return start_date <= ts <= end_date
                        except KeyError:
                            raise KeyError(
                                f"Timestamp field '{timestamp_field}' not found in row. Available keys: {list(row.keys())}"
                            )

                    filtered_ds = ds.filter(filter_func)
                elif start_date:

                    def filter_func(row):
                        try:
                            ts = row[timestamp_field]
                            return ts >= start_date
                        except KeyError:
                            raise KeyError(
                                f"Timestamp field '{timestamp_field}' not found in row. Available keys: {list(row.keys())}"
                            )

                    filtered_ds = ds.filter(filter_func)
                elif end_date:

                    def filter_func(row):
                        try:
                            ts = row[timestamp_field]
                            return ts <= end_date
                        except KeyError:
                            raise KeyError(
                                f"Timestamp field '{timestamp_field}' not found in row. Available keys: {list(row.keys())}"
                            )

                    filtered_ds = ds.filter(filter_func)
                else:
                    return ds

                return filtered_ds
            except Exception as e:
                raise RuntimeError(f"Failed to filter by timestamp: {e}")

        return ds

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: BaseRegistry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        store = RayOfflineStore()
        store._init_ray(config)

        # Load entity_df as Ray dataset for distributed processing
        if isinstance(entity_df, str):
            entity_ds = ray.data.read_csv(entity_df)
            original_entity_df = pd.read_csv(entity_df)
        else:
            entity_ds = ray.data.from_pandas(entity_df)
            original_entity_df = entity_df.copy()

        # Make entity dataframe timezone aware and normalize timestamp
        original_entity_df = _ensure_timestamp_compatibility(
            original_entity_df, ["event_timestamp"]
        )

        # Parse feature_refs and get ODFVs
        on_demand_feature_views = OnDemandFeatureView.get_requested_odfvs(
            feature_refs, project, registry
        )

        # Validate request data for ODFVs
        for odfv in on_demand_feature_views:
            odfv_request_data_schema = odfv.get_request_data_schema()
            for feature_name in odfv_request_data_schema.keys():
                if feature_name not in original_entity_df.columns:
                    raise RequestDataNotFoundInEntityDfException(
                        feature_name=feature_name,
                        feature_view_name=odfv.name,
                    )

        # Filter out on-demand feature views from regular feature views
        # ODFVs don't have data sources and are computed from base features
        odfv_names = {odfv.name for odfv in on_demand_feature_views}
        regular_feature_views = [
            fv for fv in feature_views if fv.name not in odfv_names
        ]

        # Enhanced validation using offline_utils with safe fallback
        _safe_validate_entity_dataframe(
            original_entity_df, regular_feature_views, project, registry
        )
        # Apply field mappings to entity dataset if needed
        global_field_mappings = {}
        for fv in regular_feature_views:
            mapping = getattr(fv.batch_source, "field_mapping", None)
            if mapping:
                for k, v in mapping.items():
                    global_field_mappings[v] = k

        if global_field_mappings:
            cols_to_rename = {
                v: k
                for k, v in global_field_mappings.items()
                if v in original_entity_df.columns
            }
            if cols_to_rename:
                entity_ds = entity_ds.map_batches(
                    lambda batch: batch.rename(columns=cols_to_rename),
                    batch_format="pandas",
                )

        # Start with entity dataset
        result_ds = entity_ds

        # Process each regular feature view with intelligent join strategy
        for fv in regular_feature_views:
            fv_feature_refs = [
                ref
                for ref in feature_refs
                if ref.startswith(fv.projection.name_to_use() + ":")
            ]
            if not fv_feature_refs:
                continue
            # Get join configuration
            entities = fv.entities or []
            entity_objs = [registry.get_entity(e, project) for e in entities]
            original_join_keys, _, timestamp_field, created_col = _get_column_names(
                fv, entity_objs
            )

            # Apply join key mapping from projection if present
            if fv.projection.join_key_map:
                join_keys = [
                    fv.projection.join_key_map.get(key, key)
                    for key in original_join_keys
                ]
            else:
                join_keys = original_join_keys

            # Extract requested features
            requested_feats = [ref.split(":", 1)[1] for ref in fv_feature_refs]

            # Validate requested features exist
            available_feature_names = [f.name for f in fv.features]
            missing_feats = [
                f for f in requested_feats if f not in available_feature_names
            ]
            if missing_feats:
                raise KeyError(
                    f"Requested features {missing_feats} not found in feature view '{fv.name}' "
                    f"(available: {available_feature_names})"
                )

            # Load feature data as Ray dataset
            source_path = store._get_source_path(fv.batch_source, config)
            feature_ds = ray.data.read_parquet(source_path)
            feature_size = feature_ds.size_bytes()

            # Apply field mapping to feature dataset if needed
            field_mapping = getattr(fv.batch_source, "field_mapping", None)
            if field_mapping:
                feature_ds = feature_ds.map_batches(
                    lambda batch: batch.rename(columns=field_mapping),
                    batch_format="pandas",
                )
                # Update join keys and timestamp field to mapped names
                join_keys = [field_mapping.get(k, k) for k in join_keys]
                timestamp_field = field_mapping.get(timestamp_field, timestamp_field)
                if created_col:
                    created_col = field_mapping.get(created_col, created_col)

            # Ensure timestamp compatibility in entity dataset
            if (
                timestamp_field != "event_timestamp"
                and timestamp_field not in original_entity_df.columns
                and "event_timestamp" in original_entity_df.columns
            ):

                def add_timestamp_field(batch: pd.DataFrame) -> pd.DataFrame:
                    batch = batch.copy()
                    batch[timestamp_field] = batch["event_timestamp"]
                    return _normalize_timestamp_column(
                        batch, timestamp_field, inplace=True
                    )

                result_ds = result_ds.map_batches(
                    add_timestamp_field, batch_format="pandas"
                )

            # Determine join strategy based on dataset sizes and cluster resources
            if store._resource_manager is None:
                raise ValueError("Resource manager not initialized")
            requirements = store._resource_manager.estimate_processing_requirements(
                feature_size, "join"
            )

            if requirements["should_broadcast"]:
                # Use broadcast join for small feature datasets
                logger.info(
                    f"Using broadcast join for {fv.name} (size: {feature_size // 1024**2}MB)"
                )
                feature_df = feature_ds.to_pandas()
                feature_df = _ensure_timestamp_compatibility(
                    feature_df, [timestamp_field]
                )

                if store._data_processor is None:
                    raise ValueError("Data processor not initialized")
                result_ds = store._data_processor.broadcast_join_features(
                    result_ds,
                    feature_df,
                    join_keys,
                    timestamp_field,
                    requested_feats,
                    full_feature_names,
                    fv.projection.name_to_use(),
                    original_join_keys if fv.projection.join_key_map else None,
                )
            else:
                # Use distributed windowed join for large feature datasets
                logger.info(
                    f"Using distributed join for {fv.name} (size: {feature_size // 1024**2}MB)"
                )

                # Ensure timestamp format in feature dataset
                def normalize_timestamps(batch: pd.DataFrame) -> pd.DataFrame:
                    return _ensure_timestamp_compatibility(batch, [timestamp_field])

                feature_ds = feature_ds.map_batches(
                    normalize_timestamps, batch_format="pandas"
                )

                if store._data_processor is None:
                    raise ValueError("Data processor not initialized")
                result_ds = store._data_processor.windowed_temporal_join(
                    result_ds,
                    feature_ds,
                    join_keys,
                    timestamp_field,
                    requested_feats,
                    window_size=config.offline_store.window_size_for_joins,
                    full_feature_names=full_feature_names,
                    feature_view_name=fv.projection.name_to_use(),
                    original_join_keys=original_join_keys
                    if fv.projection.join_key_map
                    else None,
                )

        # Final processing: clean up and ensure proper column structure
        def finalize_result(batch: pd.DataFrame) -> pd.DataFrame:
            batch = batch.copy()

            # Preserve existing feature columns (including renamed ones)
            existing_columns = set(batch.columns)

            # Re-attach any missing original entity columns that aren't already present
            for col in original_entity_df.columns:
                if col not in existing_columns:
                    # For missing columns, use values from original entity df
                    if len(batch) <= len(original_entity_df):
                        batch[col] = original_entity_df[col].iloc[: len(batch)].values
                    else:
                        # Repeat values if batch is larger
                        repeated_values = np.tile(
                            original_entity_df[col].values,
                            (len(batch) // len(original_entity_df) + 1),
                        )
                        batch[col] = repeated_values[: len(batch)]

            # Ensure event_timestamp is present
            if "event_timestamp" not in batch.columns:
                if "event_timestamp" in original_entity_df.columns:
                    batch["event_timestamp"] = (
                        original_entity_df["event_timestamp"].iloc[: len(batch)].values
                    )
                    batch = _normalize_timestamp_column(
                        batch, "event_timestamp", inplace=True
                    )
                elif timestamp_field in batch.columns:
                    batch["event_timestamp"] = batch[timestamp_field]

            # Fix data types for feature columns using centralized type mapping utilities
            batch = _convert_feature_column_types(batch, regular_feature_views)

            return batch

        result_ds = result_ds.map_batches(finalize_result, batch_format="pandas")

        # Storage path validation
        storage_path = config.offline_store.storage_path
        if not storage_path:
            raise ValueError("Storage path must be set in config")

        # Create retrieval job following standard pattern
        job = RayRetrievalJob(result_ds, staging_location=storage_path)
        job._full_feature_names = full_feature_names
        job._on_demand_feature_views = on_demand_feature_views
        job._feature_refs = feature_refs
        job._entity_df = original_entity_df
        job._metadata = job._create_metadata()
        return job

    def validate_data_source(
        self,
        config: RepoConfig,
        data_source: DataSource,
    ):
        """Validates the underlying data source."""
        self._init_ray(config)
        data_source.validate(config=config)

    def get_table_column_names_and_types_from_data_source(
        self,
        config: RepoConfig,
        data_source: DataSource,
    ) -> Iterable[Tuple[str, str]]:
        """Returns the list of column names and raw column types for a DataSource."""
        return data_source.get_table_column_names_and_types(config=config)

    def supports_remote_storage_export(self) -> bool:
        """Check if remote storage export is supported."""
        return self._staging_location is not None

    @staticmethod
    def _load_and_filter_dataset(
        source_path: str,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str],
        start_date: Optional[datetime],
        end_date: Optional[datetime],
    ) -> pd.DataFrame:
        """
        Common method to load and filter dataset for both pull_latest and pull_all methods.
        Args:
            source_path: Path to the data source
            data_source: DataSource object containing field mapping
            join_key_columns: List of join key columns
            feature_name_columns: List of feature columns
            timestamp_field: Name of the timestamp field
            created_timestamp_column: Optional created timestamp column
            start_date: Optional start date for filtering
            end_date: Optional end date for filtering
        Returns:
            Processed pandas DataFrame
        """
        try:
            # Get field mapping for column renaming after loading
            field_mapping = getattr(data_source, "field_mapping", None)

            # Load and filter the dataset using the original timestamp field name
            ds = RayOfflineStore._create_filtered_dataset(
                source_path, timestamp_field, start_date, end_date
            )

            # Convert to pandas for processing
            df = ds.to_pandas()
            df = make_df_tzaware(df)

            # Apply field mapping if needed
            if field_mapping:
                df = df.rename(columns=field_mapping)

            # Get mapped field names
            timestamp_field_mapped = (
                field_mapping.get(timestamp_field, timestamp_field)
                if field_mapping
                else timestamp_field
            )
            created_timestamp_column_mapped = (
                field_mapping.get(created_timestamp_column, created_timestamp_column)
                if field_mapping and created_timestamp_column
                else created_timestamp_column
            )

            # Build timestamp columns list
            timestamp_columns = [timestamp_field_mapped]
            if created_timestamp_column_mapped:
                timestamp_columns.append(created_timestamp_column_mapped)

            # Normalize timestamp columns
            df = _normalize_timestamp_columns(df, timestamp_columns, inplace=True)

            # Handle empty DataFrame case
            if df.empty:
                empty_columns = (
                    join_key_columns + feature_name_columns + timestamp_columns
                )
                if not join_key_columns:
                    empty_columns.append(DUMMY_ENTITY_ID)
                if "event_timestamp" not in empty_columns:
                    empty_columns.append("event_timestamp")
                return _create_empty_dataframe_with_timestamp_columns(
                    empty_columns, timestamp_columns
                )

            # Build required columns list
            all_required_columns = (
                join_key_columns + feature_name_columns + timestamp_columns
            )
            if not join_key_columns:
                df[DUMMY_ENTITY_ID] = DUMMY_ENTITY_VAL
                all_required_columns.append(DUMMY_ENTITY_ID)

            # Select only the required columns that exist
            available_columns = [
                col for col in all_required_columns if col in df.columns
            ]
            df = df[available_columns]

            # Basic sorting by timestamp (most recent first)
            existing_timestamp_columns = [
                col for col in timestamp_columns if col in df.columns
            ]
            if existing_timestamp_columns:
                df = df.sort_values(existing_timestamp_columns, ascending=False)

            # Reset index
            df = df.reset_index(drop=True)

            # Ensure 'event_timestamp' column exists for pandas backend compatibility
            if (
                "event_timestamp" not in df.columns
                and timestamp_field_mapped != "event_timestamp"
            ):
                if timestamp_field_mapped in df.columns:
                    df["event_timestamp"] = df[timestamp_field_mapped]

            return df

        except Exception as e:
            raise RuntimeError(f"Failed to load data from {source_path}: {e}")

    @staticmethod
    def pull_latest_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        store = RayOfflineStore()
        store._init_ray(config)

        source_path = store._get_source_path(data_source, config)

        def _load():
            # Load and filter the dataset using the shared method
            df = store._load_and_filter_dataset(
                source_path,
                data_source,
                join_key_columns,
                feature_name_columns,
                timestamp_field,
                created_timestamp_column,
                start_date,
                end_date,
            )

            # Handle deduplication (keep latest records) - specific to pull_latest
            if join_key_columns and not df.empty:
                # Get field mapping for proper column names
                field_mapping = getattr(data_source, "field_mapping", None)
                timestamp_field_mapped = (
                    field_mapping.get(timestamp_field, timestamp_field)
                    if field_mapping
                    else timestamp_field
                )
                created_timestamp_column_mapped = (
                    field_mapping.get(
                        created_timestamp_column, created_timestamp_column
                    )
                    if field_mapping and created_timestamp_column
                    else created_timestamp_column
                )

                # Build timestamp columns for sorting
                timestamp_columns = [timestamp_field_mapped]
                if created_timestamp_column_mapped:
                    timestamp_columns.append(created_timestamp_column_mapped)

                # Filter out timestamp columns that don't exist in the dataframe
                existing_timestamp_columns = [
                    col for col in timestamp_columns if col in df.columns
                ]

                # Sort by join keys (ascending) and timestamps (descending for latest first)
                sort_columns = join_key_columns + existing_timestamp_columns
                if sort_columns:
                    df = df.sort_values(
                        sort_columns,
                        ascending=[True] * len(join_key_columns)
                        + [False] * len(existing_timestamp_columns),
                    )
                    df = df.drop_duplicates(subset=join_key_columns, keep="first")

            return df

        return RayRetrievalJob(
            _load, staging_location=config.offline_store.storage_path
        )

    @staticmethod
    def pull_all_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> RetrievalJob:
        store = RayOfflineStore()
        store._init_ray(config)

        source_path = store._get_source_path(data_source, config)

        fs, path_in_fs = fsspec.core.url_to_fs(source_path)
        if not fs.exists(path_in_fs):
            raise FileNotFoundError(f"Parquet path does not exist: {source_path}")

        def _load():
            return store._load_and_filter_dataset(
                source_path,
                data_source,
                join_key_columns,
                feature_name_columns,
                timestamp_field,
                created_timestamp_column,
                start_date,
                end_date,
            )

        return RayRetrievalJob(
            _load, staging_location=config.offline_store.storage_path
        )

    @staticmethod
    def write_logged_features(
        config: RepoConfig,
        data: Union[pa.Table, Path],
        source: LoggingSource,
        logging_config: LoggingConfig,
        registry: BaseRegistry,
    ) -> None:
        RayOfflineStore._ensure_ray_initialized(config)

        repo_path = getattr(config, "repo_path", None) or os.getcwd()

        # Get source path and resolve URI
        source_path = getattr(source, "file_path", None)
        if not source_path:
            raise ValueError("LoggingSource must have a file_path attribute")

        path = FileSource.get_uri_for_file_path(repo_path, source_path)

        try:
            if isinstance(data, Path):
                ds = ray.data.read_parquet(str(data))
            else:
                ds = ray.data.from_pandas(pa.Table.to_pandas(data))

            ds.materialize()

            if not path.startswith(("s3://", "gs://")):
                os.makedirs(os.path.dirname(path), exist_ok=True)

            ds.write_parquet(path)
        except Exception as e:
            raise RuntimeError(f"Failed to write logged features: {e}")

    @staticmethod
    def offline_write_batch(
        config: RepoConfig,
        feature_view: FeatureView,
        table: pa.Table,
        progress: Optional[Callable[[int], Any]] = None,
    ) -> None:
        RayOfflineStore._ensure_ray_initialized(config)

        repo_path = getattr(config, "repo_path", None) or os.getcwd()
        ray_config = config.offline_store
        assert isinstance(ray_config, RayOfflineStoreConfig)
        assert isinstance(feature_view.batch_source, FileSource)

        # Enhanced schema validation using safe utility
        validation_result = _safe_validate_schema(
            config, feature_view.batch_source, table.column_names, "offline_write_batch"
        )

        if validation_result:
            expected_schema, expected_columns = validation_result
            # Try to reorder columns to match expected order if needed
            if expected_columns != table.column_names and set(expected_columns) == set(
                table.column_names
            ):
                logger.info("Reordering table columns to match expected schema")
                table = table.select(expected_columns)

        batch_source_path = feature_view.batch_source.file_options.uri
        feature_path = FileSource.get_uri_for_file_path(repo_path, batch_source_path)

        # If the path points to a file, write directly to that file location
        # If it points to a directory, write to that directory
        if feature_path.endswith(".parquet"):
            # Convert PyArrow table to pandas DataFrame
            df = table.to_pandas()

            # Check if file exists and append if it does
            if os.path.exists(feature_path):
                # Read existing data
                existing_df = pd.read_parquet(feature_path)
                # Append new data
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                # Write combined data
                combined_df.to_parquet(feature_path, index=False)
            else:
                # Write new data
                df.to_parquet(feature_path, index=False)
        else:
            # Write to directory (multiple parquet files)
            os.makedirs(feature_path, exist_ok=True)

            # Convert PyArrow table to Ray dataset
            ds = ray.data.from_arrow(table)

            # Write to parquet
            ds.write_parquet(feature_path)

    @staticmethod
    def create_saved_dataset_destination(
        config: RepoConfig,
        name: str,
        path: Optional[str] = None,
    ) -> SavedDatasetStorage:
        """Create a saved dataset destination for Ray offline store."""

        if path is None:
            ray_config = config.offline_store
            assert isinstance(ray_config, RayOfflineStoreConfig)
            base_storage_path = ray_config.storage_path or "/tmp/ray-storage"
            path = f"{base_storage_path}/saved_datasets/{name}.parquet"

        return SavedDatasetFileStorage(path=path)
