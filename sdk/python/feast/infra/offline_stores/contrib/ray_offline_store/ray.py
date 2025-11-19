import logging
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Union

import dill
import fsspec
import numpy as np
import pandas as pd
import pyarrow as pa
import ray
from ray.data import Dataset
from ray.data.context import DatasetContext

from feast.data_source import DataSource
from feast.dataframe import DataFrameEngine, FeastDataFrame
from feast.errors import (
    RequestDataNotFoundInEntityDfException,
    SavedDatasetLocationAlreadyExists,
)
from feast.feature_logging import LoggingConfig, LoggingSource
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, FeatureView
from feast.feature_view_utils import resolve_feature_view_source_with_fallback
from feast.infra.offline_stores.file_source import (
    FileLoggingDestination,
    FileSource,
    SavedDatasetFileStorage,
)
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.offline_stores.offline_utils import (
    get_entity_df_timestamp_bounds,
    get_pyarrow_schema_from_batch_source,
    infer_event_timestamp_from_entity_df,
)
from feast.infra.ray_initializer import (
    ensure_ray_initialized,
    get_ray_wrapper,
)
from feast.infra.ray_shared_utils import (
    _build_required_columns,
    apply_field_mapping,
    ensure_timestamp_compatibility,
    is_ray_data,
    normalize_timestamp_columns,
)
from feast.infra.registry.base_registry import BaseRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage, ValidationReference
from feast.type_map import (
    convert_array_column,
    convert_scalar_column,
    feast_value_type_to_pandas_type,
    pa_to_feast_value_type,
)
from feast.utils import _get_column_names, make_df_tzaware, make_tzaware

logger = logging.getLogger(__name__)


def _get_data_schema_info(
    data: Union[pd.DataFrame, Dataset, Any],
) -> Tuple[Dict[str, Any], List[str]]:
    """
    Extract schema information from DataFrame or Dataset.
    Args:
        data: DataFrame or Ray Dataset
    Returns:
        Tuple of (dtypes_dict, column_names)
    """
    if is_ray_data(data):
        schema = data.schema()
        dtypes = {}
        for i, col in enumerate(schema.names):
            field_type = schema.field(i).type
            try:
                pa_type_str = str(field_type).lower()
                feast_value_type = pa_to_feast_value_type(pa_type_str)
                pandas_type_str = feast_value_type_to_pandas_type(feast_value_type)
                dtypes[col] = pd.api.types.pandas_dtype(pandas_type_str)
            except Exception:
                dtypes[col] = pd.api.types.pandas_dtype("object")
        columns = schema.names
    else:
        assert isinstance(data, pd.DataFrame)
        dtypes = data.dtypes.to_dict()
        columns = list(data.columns)
    return dtypes, columns


def _apply_to_data(
    data: Union[pd.DataFrame, Dataset, Any],
    process_func: Callable[[pd.DataFrame], pd.DataFrame],
    inplace: bool = False,
) -> Union[pd.DataFrame, Dataset, Any]:
    """
    Apply a processing function to DataFrame or Dataset.
    Args:
        data: DataFrame or Ray Dataset to process
        process_func: Function that takes a DataFrame and returns a processed DataFrame
        inplace: Whether to modify DataFrame in place (only applies to pandas)
    Returns:
        Processed DataFrame or Dataset
    """
    if is_ray_data(data):
        return data.map_batches(process_func, batch_format="pandas")
    else:
        assert isinstance(data, pd.DataFrame)
        if not inplace:
            data = data.copy()
        return process_func(data)


def _handle_empty_dataframe_case(
    join_key_columns: List[str],
    feature_name_columns: List[str],
    timestamp_columns: List[str],
) -> pd.DataFrame:
    """
    Handle empty DataFrame case by creating properly structured empty DataFrame.
    Args:
        join_key_columns: List of join key columns
        feature_name_columns: List of feature columns
        timestamp_columns: List of timestamp columns
    Returns:
        Empty DataFrame with proper structure and column types
    """
    empty_columns = _build_required_columns(
        join_key_columns, feature_name_columns, timestamp_columns
    )
    df = pd.DataFrame(columns=empty_columns)
    for col in timestamp_columns:
        if col in df.columns:
            df[col] = df[col].astype("datetime64[ns, UTC]")
    return df


def _safe_infer_event_timestamp_column(
    data: Union[pd.DataFrame, Dataset], fallback_column: str = "event_timestamp"
) -> str:
    """
    Safely infer the event timestamp column.
    Works with both pandas DataFrames and Ray Datasets.
    Args:
        data: DataFrame or Ray Dataset to analyze
        fallback_column: Default column name to use if inference fails
    Returns:
        Inferred or fallback timestamp column name
    """
    try:
        dtypes, _ = _get_data_schema_info(data)
        return infer_event_timestamp_from_entity_df(dtypes)
    except Exception as e:
        logger.debug(
            f"Timestamp column inference failed: {e}, using fallback: {fallback_column}"
        )
        return fallback_column


def _safe_get_entity_timestamp_bounds(
    data: Union[pd.DataFrame, Dataset, Any], timestamp_column: str
) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Safely get entity timestamp bounds.
    Works with both pandas DataFrames and Ray Datasets.
    Args:
        data: DataFrame or Ray Dataset
        timestamp_column: Name of timestamp column
    Returns:
        Tuple of (min_timestamp, max_timestamp) or (None, None) if failed
    """
    try:
        if is_ray_data(data):
            min_ts = data.min(timestamp_column)
            max_ts = data.max(timestamp_column)
        else:
            if timestamp_column in data.columns:
                min_ts, max_ts = get_entity_df_timestamp_bounds(data, timestamp_column)
            else:
                return None, None
        if hasattr(min_ts, "to_pydatetime"):
            min_ts = min_ts.to_pydatetime()
        elif isinstance(min_ts, pd.Timestamp):
            min_ts = min_ts.to_pydatetime()
        if hasattr(max_ts, "to_pydatetime"):
            max_ts = max_ts.to_pydatetime()
        elif isinstance(max_ts, pd.Timestamp):
            max_ts = max_ts.to_pydatetime()
        return min_ts, max_ts
    except Exception as e:
        logger.debug(
            f"Timestamp bounds extraction failed: {e}, falling back to manual calculation"
        )
        try:
            if is_ray_data(data):

                def extract_bounds(batch: pd.DataFrame) -> pd.DataFrame:
                    if timestamp_column in batch.columns and not batch.empty:
                        timestamps = pd.to_datetime(batch[timestamp_column], utc=True)
                        return pd.DataFrame(
                            {"min_ts": [timestamps.min()], "max_ts": [timestamps.max()]}
                        )
                    return pd.DataFrame({"min_ts": [None], "max_ts": [None]})

                bounds_ds = data.map_batches(extract_bounds, batch_format="pandas")
                bounds_df = bounds_ds.to_pandas()

                if not bounds_df.empty:
                    min_ts = bounds_df["min_ts"].min()
                    max_ts = bounds_df["max_ts"].max()

                    if pd.notna(min_ts) and pd.notna(max_ts):
                        return min_ts.to_pydatetime(), max_ts.to_pydatetime()
            else:
                assert isinstance(data, pd.DataFrame)
                if timestamp_column in data.columns:
                    timestamps = pd.to_datetime(data[timestamp_column], utc=True)
                    return (
                        timestamps.min().to_pydatetime(),
                        timestamps.max().to_pydatetime(),
                    )
        except Exception:
            pass

        return None, None


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
    data: Union[pd.DataFrame, Dataset], feature_views: List[FeatureView]
) -> Union[pd.DataFrame, Dataset]:
    """
    Convert feature columns to appropriate pandas types using Feast's type mapping utilities.
    Works with both pandas DataFrames and Ray Datasets.
    Args:
        data: DataFrame or Ray Dataset containing feature data
        feature_views: List of feature views with type information
    Returns:
        DataFrame or Dataset with properly converted feature column types
    """

    def convert_batch(batch: pd.DataFrame) -> pd.DataFrame:
        batch = batch.copy()

        for fv in feature_views:
            for feature in fv.features:
                feat_name = feature.name
                if feat_name not in batch.columns:
                    continue
                try:
                    value_type = feature.dtype.to_value_type()
                    if value_type.name.endswith("_LIST"):
                        batch[feat_name] = convert_array_column(
                            batch[feat_name], value_type
                        )
                    else:
                        target_pandas_type = feast_value_type_to_pandas_type(value_type)
                        batch[feat_name] = convert_scalar_column(
                            batch[feat_name], value_type, target_pandas_type
                        )
                except Exception as e:
                    logger.warning(
                        f"Failed to convert feature {feat_name} to proper type: {e}"
                    )
                    continue
        return batch

    return _apply_to_data(data, convert_batch)


class RayOfflineStoreConfig(FeastConfigBaseModel):
    """
    Configuration for the Ray Offline Store.

    For detailed configuration options and examples, see the documentation:
    https://docs.feast.dev/reference/offline-stores/ray
    """

    type: Literal[
        "feast.offline_stores.contrib.ray_offline_store.ray.RayOfflineStore", "ray"
    ] = "ray"
    storage_path: Optional[str] = None
    ray_address: Optional[str] = None

    # Optimization settings
    broadcast_join_threshold_mb: Optional[int] = 100
    enable_distributed_joins: Optional[bool] = True
    max_parallelism_multiplier: Optional[int] = 2
    target_partition_size_mb: Optional[int] = 64
    window_size_for_joins: Optional[str] = "1H"

    # Logging settings
    enable_ray_logging: Optional[bool] = False

    # Ray configuration for resource management (memory, CPU limits)
    ray_conf: Optional[Dict[str, Any]] = None

    # KubeRay/CodeFlare SDK configurations
    use_kuberay: Optional[bool] = None
    """Whether to use KubeRay/CodeFlare SDK for Ray cluster management"""

    cluster_name: Optional[str] = None
    """Name of the KubeRay cluster to connect to (required for KubeRay mode)"""

    auth_token: Optional[str] = None
    """Authentication token for Ray cluster connection (for secure clusters)"""

    kuberay_conf: Optional[Dict[str, Any]] = None
    """KubeRay/CodeFlare configuration parameters (passed to CodeFlare SDK)"""


class RayResourceManager:
    """
    Manages Ray cluster resources for optimal performance.
    # See: https://docs.feast.dev/reference/offline-stores/ray#resource-management-and-testing
    """

    def __init__(self, config: Optional[RayOfflineStoreConfig] = None) -> None:
        """
        Initialize the resource manager with cluster resource information.
        """
        self.config = config or RayOfflineStoreConfig()

        if not ray.is_initialized():
            self.cluster_resources = {"CPU": 4, "memory": 8 * 1024**3}
            self.available_memory = 8 * 1024**3
            self.available_cpus = 4
            self.num_nodes = 1
            return

        self.cluster_resources = ray.cluster_resources()
        self.available_memory = self.cluster_resources.get("memory", 8 * 1024**3)
        self.available_cpus = int(self.cluster_resources.get("CPU", 4))
        self.num_nodes = len(ray.nodes())

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

        ctx.read_op_min_num_blocks = self.available_cpus
        multiplier = (
            self.config.max_parallelism_multiplier
            if self.config.max_parallelism_multiplier is not None
            else 2
        )
        ctx.max_parallelism = self.available_cpus * multiplier
        ctx.shuffle_strategy = "sort"  # type: ignore
        ctx.enable_tensor_extension_casting = False

        if not getattr(self.config, "enable_ray_logging", False):
            ctx.enable_progress_bars = False
            if hasattr(ctx, "verbose_progress"):
                ctx.verbose_progress = False

        if getattr(self.config, "enable_ray_logging", False):
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

    def __init__(self, resource_manager: RayResourceManager) -> None:
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
        # For datasets with join keys, repartition then shuffle for better distribution
        return ds.repartition(num_blocks=optimal_partitions).random_shuffle()

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
                if feat in features_df.columns and pd.api.types.is_datetime64_any_dtype(
                    features_df[feat]
                ):
                    result[feat] = pd.Series(
                        [pd.NaT] * len(result), dtype="datetime64[ns, UTC]"
                    )
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

            enable_logging = getattr(
                self.resource_manager.config, "enable_ray_logging", False
            )
            if enable_logging:
                logger.info(
                    f"Processing feature view {feature_view_name} with join keys {join_keys}"
                )

            if original_join_keys:
                feature_join_keys = original_join_keys
                entity_join_keys = join_keys
            else:
                feature_join_keys = join_keys
                entity_join_keys = join_keys

            feature_cols = [timestamp_field] + feature_join_keys + requested_feats

            available_feature_cols = [
                col for col in feature_cols if col in features.columns
            ]

            if timestamp_field not in available_feature_cols:
                raise ValueError(
                    f"Timestamp field '{timestamp_field}' not found in features columns: {list(features.columns)}"
                )

            missing_feats = [
                feat for feat in requested_feats if feat not in features.columns
            ]
            if missing_feats:
                raise ValueError(
                    f"Requested features {missing_feats} not found in features columns: {list(features.columns)}"
                )

            features_filtered = features[available_feature_cols].copy()

            batch = normalize_timestamp_columns(batch, timestamp_field, inplace=True)
            features_filtered = normalize_timestamp_columns(
                features_filtered, timestamp_field, inplace=True
            )

            if not entity_join_keys:
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
                for key in entity_join_keys:
                    if key not in batch.columns:
                        batch[key] = np.nan
                for key in feature_join_keys:
                    if key not in features_filtered.columns:
                        features_filtered[key] = np.nan
                batch_clean = batch.dropna(
                    subset=entity_join_keys + [timestamp_field]
                ).copy()
                features_clean = features_filtered.dropna(
                    subset=feature_join_keys + [timestamp_field]
                ).copy()
                if batch_clean.empty or features_clean.empty:
                    return batch.head(0)
                if timestamp_field in batch_clean.columns:
                    batch_sorted = batch_clean.sort_values(
                        timestamp_field, ascending=True
                    ).reset_index(drop=True)
                else:
                    batch_sorted = batch_clean.reset_index(drop=True)

                right_sort_columns = []
                for key in feature_join_keys:
                    if key in features_clean.columns:
                        right_sort_columns.append(key)
                if timestamp_field in features_clean.columns:
                    right_sort_columns.append(timestamp_field)
                if right_sort_columns:
                    features_clean = features_clean.drop_duplicates(
                        subset=right_sort_columns, keep="last"
                    )
                    features_sorted = features_clean.sort_values(
                        right_sort_columns, ascending=True
                    ).reset_index(drop=True)
                else:
                    features_sorted = features_clean.reset_index(drop=True)

                if (
                    timestamp_field in features_sorted.columns
                    and len(features_sorted) > 1
                ):
                    if feature_join_keys:
                        grouped = features_sorted.groupby(feature_join_keys, sort=False)
                        for name, group in grouped:
                            if not group[timestamp_field].is_monotonic_increasing:
                                features_sorted = features_sorted.sort_values(
                                    feature_join_keys + [timestamp_field],
                                    ascending=True,
                                ).reset_index(drop=True)
                                break
                    else:
                        if not features_sorted[timestamp_field].is_monotonic_increasing:
                            features_sorted = features_sorted.sort_values(
                                timestamp_field, ascending=True
                            ).reset_index(drop=True)

                try:
                    if feature_join_keys:
                        batch_dedup_cols = [
                            k for k in entity_join_keys if k in batch_sorted.columns
                        ]
                        if timestamp_field in batch_sorted.columns:
                            batch_dedup_cols.append(timestamp_field)
                        if batch_dedup_cols:
                            batch_sorted = batch_sorted.drop_duplicates(
                                subset=batch_dedup_cols, keep="last"
                            )
                        feature_dedup_cols = [
                            k for k in feature_join_keys if k in features_sorted.columns
                        ]
                        if timestamp_field in features_sorted.columns:
                            feature_dedup_cols.append(timestamp_field)
                        if feature_dedup_cols:
                            features_sorted = features_sorted.drop_duplicates(
                                subset=feature_dedup_cols, keep="last"
                            )

                    if feature_join_keys:
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
                    if enable_logging:
                        logger.warning(
                            f"merge_asof didn't work: {e}, implementing manual point-in-time join"
                        )
                    result = self._manual_point_in_time_join(
                        batch_clean,
                        features_clean,
                        entity_join_keys,
                        feature_join_keys,
                        timestamp_field,
                        requested_feats,
                    )
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

        window_size = window_size or (
            self.resource_manager.config.window_size_for_joins or "1H"
        )
        entity_optimized = self.optimize_dataset_for_join(entity_ds, join_keys)
        feature_optimized = self.optimize_dataset_for_join(feature_ds, join_keys)
        entity_windowed = self._add_time_windows_and_source_marker(
            entity_optimized, timestamp_field, "entity", window_size
        )
        feature_windowed = self._add_time_windows_and_source_marker(
            feature_optimized, timestamp_field, "feature", window_size
        )
        combined_ds = entity_windowed.union(feature_windowed)
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
            batch = batch.copy()
            if timestamp_field in batch.columns:
                batch["time_window"] = (
                    pd.to_datetime(batch[timestamp_field])
                    .dt.floor(window_size)
                    .astype("datetime64[ns, UTC]")
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

        result_chunks = []
        group_keys = ["time_window"] + join_keys

        for group_values, group_data in batch.groupby(group_keys):
            entity_data = group_data[group_data["_data_source"] == "entity"].copy()
            feature_data = group_data[group_data["_data_source"] == "feature"].copy()
            if len(entity_data) > 0 and len(feature_data) > 0:
                entity_clean = entity_data.drop(columns=["time_window", "_data_source"])
                feature_clean = feature_data.drop(
                    columns=["time_window", "_data_source"]
                )
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
                entity_clean = entity_data.drop(columns=["time_window", "_data_source"])
                for feat in requested_feats:
                    if feat not in entity_clean.columns:
                        entity_clean[feat] = np.nan
                result_chunks.append(entity_clean)

        if result_chunks:
            result = pd.concat(result_chunks, ignore_index=True)
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
        config: Optional[RayOfflineStoreConfig] = None,
    ):
        self._dataset_or_callable = dataset_or_callable
        self._staging_location = staging_location
        self._config = config or RayOfflineStoreConfig()
        self._cached_df: Optional[pd.DataFrame] = None
        self._cached_dataset: Optional[Dataset] = None
        self._metadata: Optional[RetrievalMetadata] = None
        self._full_feature_names: bool = False
        self._on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None
        self._feature_refs: List[str] = []
        self._entity_df: Optional[pd.DataFrame] = None
        self._prefer_ray_datasets: bool = True

    def _create_metadata(self) -> RetrievalMetadata:
        """Create metadata from the entity DataFrame and feature references."""
        if self._entity_df is not None:
            timestamp_col = _safe_infer_event_timestamp_column(
                self._entity_df, "event_timestamp"
            )
            min_timestamp, max_timestamp = _safe_get_entity_timestamp_bounds(
                self._entity_df, timestamp_col
            )

            keys = [col for col in self._entity_df.columns if col != timestamp_col]
        else:
            try:
                result = self._resolve()
                if is_ray_data(result):
                    timestamp_col = _safe_infer_event_timestamp_column(
                        result, "event_timestamp"
                    )
                    min_timestamp, max_timestamp = _safe_get_entity_timestamp_bounds(
                        result, timestamp_col
                    )
                    schema = result.schema()
                    keys = [col for col in schema.names if col != timestamp_col]
                else:
                    min_timestamp = None
                    max_timestamp = None
                    keys = []
            except Exception:
                min_timestamp = None
                max_timestamp = None
                keys = []

        return RetrievalMetadata(
            features=self._feature_refs,
            keys=keys,
            min_event_timestamp=min_timestamp,
            max_event_timestamp=max_timestamp,
        )

    def _set_metadata_info(
        self, feature_refs: List[str], entity_df: pd.DataFrame
    ) -> None:
        """Set the feature references and entity DataFrame for metadata creation."""
        self._feature_refs = feature_refs
        self._entity_df = entity_df

    def _resolve(self) -> Union[Dataset, pd.DataFrame]:
        if callable(self._dataset_or_callable):
            result = self._dataset_or_callable()
        else:
            result = self._dataset_or_callable
        return result

    def _get_ray_dataset(self) -> Dataset:
        """Get the result as a Ray Dataset, converting if necessary."""
        if self._cached_dataset is not None:
            return self._cached_dataset

        result = self._resolve()
        if is_ray_data(result):
            self._cached_dataset = result
            return result
        elif isinstance(result, pd.DataFrame):
            ray_wrapper = get_ray_wrapper()
            self._cached_dataset = ray_wrapper.from_pandas(result)
            return self._cached_dataset
        else:
            raise ValueError(f"Unsupported result type: {type(result)}")

    def to_df(
        self,
        validation_reference: Optional[ValidationReference] = None,
        timeout: Optional[int] = None,
    ) -> pd.DataFrame:
        if self._cached_df is not None and not self.on_demand_feature_views:
            df = self._cached_df
        else:
            if self.on_demand_feature_views:
                df = super().to_df(
                    validation_reference=validation_reference, timeout=timeout
                )
            else:
                if self._prefer_ray_datasets:
                    ray_ds = self._get_ray_dataset()
                    df = ray_ds.to_pandas()
                else:
                    result = self._resolve()
                    if isinstance(result, pd.DataFrame):
                        df = result
                    else:
                        df = result.to_pandas()
                self._cached_df = df

        if validation_reference:
            try:
                from feast.dqm.errors import ValidationFailed

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
        if self.on_demand_feature_views:
            return super().to_arrow(
                validation_reference=validation_reference, timeout=timeout
            )

        if self._prefer_ray_datasets:
            try:
                ray_ds = self._get_ray_dataset()
                if hasattr(ray_ds, "to_arrow"):
                    return ray_ds.to_arrow()
                else:
                    df = ray_ds.to_pandas()
                    return pa.Table.from_pandas(df)
            except Exception:
                df = self.to_df(
                    validation_reference=validation_reference, timeout=timeout
                )
                return pa.Table.from_pandas(df)
        else:
            result = self._resolve()
            if isinstance(result, pd.DataFrame):
                return pa.Table.from_pandas(result)
            else:
                df = result.to_pandas()
                return pa.Table.from_pandas(df)

    def to_feast_df(
        self,
        validation_reference: Optional[ValidationReference] = None,
        timeout: Optional[int] = None,
    ) -> FeastDataFrame:
        """
        Return the result as a FeastDataFrame with Ray engine.

        This preserves Ray's lazy execution by wrapping the Ray Dataset directly.
        """
        # If we have on-demand feature views, fall back to base class Arrow implementation
        if self.on_demand_feature_views:
            return super().to_feast_df(validation_reference, timeout)

        # Get the Ray Dataset directly (maintains lazy execution)
        ray_ds = self._get_ray_dataset()

        return FeastDataFrame(
            data=ray_ds,
            engine=DataFrameEngine.RAY,
        )

    def to_remote_storage(self) -> list[str]:
        if not self._staging_location:
            raise ValueError("Staging location must be set for remote materialization.")
        try:
            ray_ds = self._get_ray_dataset()
            RayOfflineStore._ensure_ray_initialized()
            output_uri = os.path.join(self._staging_location, str(uuid.uuid4()))
            ray_ds.write_parquet(output_uri)
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
        if self._prefer_ray_datasets:
            ray_ds = self._get_ray_dataset()
            return ray_ds.to_pandas()
        else:
            return self._resolve().to_pandas()

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pa.Table:
        if self._prefer_ray_datasets:
            ray_ds = self._get_ray_dataset()
            try:
                if hasattr(ray_ds, "to_arrow"):
                    return ray_ds.to_arrow()
                else:
                    df = ray_ds.to_pandas()
                    return pa.Table.from_pandas(df)
            except Exception:
                df = ray_ds.to_pandas()
                return pa.Table.from_pandas(df)
        else:
            result = self._resolve()
            if isinstance(result, pd.DataFrame):
                return pa.Table.from_pandas(result)
            else:
                df = result.to_pandas()
                return pa.Table.from_pandas(df)

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: Optional[bool] = False,
        timeout: Optional[int] = None,
    ) -> str:
        """Persist the dataset to storage using Ray operations."""

        if not isinstance(storage, SavedDatasetFileStorage):
            raise ValueError(
                f"Ray offline store only supports SavedDatasetFileStorage, got {type(storage)}"
            )
        destination_path = storage.file_options.uri
        if not destination_path.startswith(("s3://", "gs://", "hdfs://")):
            if not allow_overwrite and os.path.exists(destination_path):
                raise SavedDatasetLocationAlreadyExists(location=destination_path)
        try:
            ray_ds = self._get_ray_dataset()

            if not destination_path.startswith(("s3://", "gs://", "hdfs://")):
                os.makedirs(os.path.dirname(destination_path), exist_ok=True)

            ray_ds.write_parquet(destination_path)

            return destination_path
        except Exception as e:
            raise RuntimeError(f"Failed to persist dataset to {destination_path}: {e}")

    def materialize(self) -> None:
        """Materialize the Ray dataset to improve subsequent access performance."""
        try:
            ray_ds = self._get_ray_dataset()
            materialized_ds = ray_ds.materialize()
            self._cached_dataset = materialized_ds

            if getattr(self._config, "enable_ray_logging", False):
                logger.info("Ray dataset materialized successfully")
        except Exception as e:
            logger.warning(f"Failed to materialize Ray dataset: {e}")

    def schema(self) -> pa.Schema:
        """Get the schema of the dataset efficiently using Ray operations."""
        try:
            ray_ds = self._get_ray_dataset()
            return ray_ds.schema()
        except Exception:
            df = self.to_df()
            return pa.Table.from_pandas(df).schema


class RayOfflineStore(OfflineStore):
    def __init__(self) -> None:
        self._staging_location: Optional[str] = None
        self._ray_initialized: bool = False
        self._resource_manager: Optional[RayResourceManager] = None
        self._data_processor: Optional[RayDataProcessor] = None

    @staticmethod
    def _suppress_ray_logging() -> None:
        """Suppress Ray and Ray Data logging completely."""
        import warnings

        # Suppress Ray warnings
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="ray")
        warnings.filterwarnings("ignore", category=UserWarning, module="ray")

        # Set environment variables to suppress Ray output
        os.environ["RAY_DISABLE_IMPORT_WARNING"] = "1"
        os.environ["RAY_SUPPRESS_UNVERIFIED_TLS_WARNING"] = "1"
        os.environ["RAY_LOG_LEVEL"] = "ERROR"
        os.environ["RAY_DATA_LOG_LEVEL"] = "ERROR"
        os.environ["RAY_DISABLE_PROGRESS_BARS"] = "1"

        # Suppress all Ray-related loggers
        ray_loggers = [
            "ray",
            "ray.data",
            "ray.data.dataset",
            "ray.data.context",
            "ray.data._internal.streaming_executor",
            "ray.data._internal.execution",
            "ray.data._internal",
            "ray.tune",
            "ray.serve",
            "ray.util",
            "ray._private",
        ]
        for logger_name in ray_loggers:
            logging.getLogger(logger_name).setLevel(logging.ERROR)

        # Configure DatasetContext to disable progress bars
        try:
            from ray.data.context import DatasetContext

            ctx = DatasetContext.get_current()
            ctx.enable_progress_bars = False
            if hasattr(ctx, "verbose_progress"):
                ctx.verbose_progress = False
        except Exception:
            pass  # Ignore if Ray Data is not available

    @staticmethod
    def _ensure_ray_initialized(config: Optional[RepoConfig] = None) -> None:
        """Ensure Ray is initialized with proper configuration."""
        ensure_ray_initialized(config)

    def _init_ray(self, config: RepoConfig) -> None:
        ray_config = config.offline_store
        assert isinstance(ray_config, RayOfflineStoreConfig)

        RayOfflineStore._ensure_ray_initialized(config)

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

    def _optimize_dataset_for_operation(self, ds: Dataset, operation: str) -> Dataset:
        """Optimize dataset for specific operations."""
        if self._resource_manager is None:
            return ds

        dataset_size = ds.size_bytes()
        requirements = self._resource_manager.estimate_processing_requirements(
            dataset_size, operation
        )

        if requirements["can_fit_in_memory"]:
            ds = ds.materialize()

        optimal_partitions = requirements["optimal_partitions"]
        current_partitions = ds.num_blocks()

        if current_partitions != optimal_partitions:
            if getattr(self._resource_manager.config, "enable_ray_logging", False):
                logger.debug(
                    f"Repartitioning dataset from {current_partitions} to {optimal_partitions} blocks"
                )
            ds = ds.repartition(num_blocks=optimal_partitions)

        return ds

    @staticmethod
    def offline_write_batch(
        config: RepoConfig,
        feature_view: FeatureView,
        table: pa.Table,
        progress: Optional[Callable[[int], Any]] = None,
    ) -> None:
        """Write batch data using Ray operations with performance monitoring."""
        import time

        start_time = time.time()

        RayOfflineStore._ensure_ray_initialized(config)

        repo_path = getattr(config, "repo_path", None) or os.getcwd()
        ray_config = config.offline_store
        assert isinstance(ray_config, RayOfflineStoreConfig)

        if not ray_config.enable_ray_logging:
            RayOfflineStore._suppress_ray_logging()
        assert isinstance(feature_view.batch_source, FileSource)

        validation_result = _safe_validate_schema(
            config, feature_view.batch_source, table.column_names, "offline_write_batch"
        )

        if validation_result:
            expected_schema, expected_columns = validation_result
            if expected_columns != table.column_names and set(expected_columns) == set(
                table.column_names
            ):
                if getattr(ray_config, "enable_ray_logging", False):
                    logger.info("Reordering table columns to match expected schema")
                table = table.select(expected_columns)

        batch_source_path = feature_view.batch_source.file_options.uri
        feature_path = FileSource.get_uri_for_file_path(repo_path, batch_source_path)

        ray_wrapper = get_ray_wrapper()
        ds = ray_wrapper.from_arrow(table)

        try:
            if feature_path.endswith(".parquet"):
                if os.path.exists(feature_path):
                    existing_ds = ray_wrapper.read_parquet(feature_path)
                    combined_ds = existing_ds.union(ds)
                    combined_ds.write_parquet(feature_path)
                else:
                    ds.write_parquet(feature_path)
            else:
                os.makedirs(feature_path, exist_ok=True)
                ds.write_parquet(feature_path)

            if progress:
                progress(table.num_rows)

        except Exception:
            if getattr(ray_config, "enable_ray_logging", False):
                logger.info("Falling back to pandas-based writing")
            df = table.to_pandas()
            if feature_path.endswith(".parquet"):
                if os.path.exists(feature_path):
                    existing_df = pd.read_parquet(feature_path)
                    combined_df = pd.concat([existing_df, df], ignore_index=True)
                    combined_df.to_parquet(feature_path, index=False)
                else:
                    df.to_parquet(feature_path, index=False)
            else:
                os.makedirs(feature_path, exist_ok=True)
                ds_fallback = ray_wrapper.from_pandas(df)
                ds_fallback.write_parquet(feature_path)

            if progress:
                progress(table.num_rows)

        duration = time.time() - start_time
        if getattr(ray_config, "enable_ray_logging", False):
            logger.info(
                f"Ray offline_write_batch performance: {table.num_rows} rows in {duration:.2f}s "
                f"({table.num_rows / duration:.0f} rows/s)"
            )

    def online_write_batch(
        self,
        config: RepoConfig,
        table: pa.Table,
        progress: Optional[Callable[[int], Any]] = None,
    ) -> None:
        """Ray offline store doesn't support online writes."""
        raise NotImplementedError("Ray offline store doesn't support online writes")

    @staticmethod
    def _process_filtered_batch(
        batch: pd.DataFrame,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_columns: List[str],
        timestamp_field_mapped: str,
    ) -> pd.DataFrame:
        batch = make_df_tzaware(batch)
        if batch.empty:
            return _handle_empty_dataframe_case(
                join_key_columns, feature_name_columns, timestamp_columns
            )

        if not join_key_columns:
            batch[DUMMY_ENTITY_ID] = DUMMY_ENTITY_VAL

        # If feature_name_columns is empty, it means "keep all columns" (for transformations)
        # Otherwise, filter to only the requested columns
        if feature_name_columns:
            all_required_columns = _build_required_columns(
                join_key_columns, feature_name_columns, timestamp_columns
            )
            available_columns = [
                col for col in all_required_columns if col in batch.columns
            ]
            batch = batch[available_columns]

        if (
            "event_timestamp" not in batch.columns
            and timestamp_field_mapped != "event_timestamp"
        ):
            if timestamp_field_mapped in batch.columns:
                batch["event_timestamp"] = batch[timestamp_field_mapped]
        return batch

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
        try:
            field_mapping = getattr(data_source, "field_mapping", None)

            if not feature_name_columns:
                columns_to_read = None
            else:
                columns_to_read = list(
                    set(join_key_columns + feature_name_columns + [timestamp_field])
                )
                if created_timestamp_column:
                    columns_to_read.append(created_timestamp_column)

            ds = RayOfflineStore._create_filtered_dataset(
                source_path,
                timestamp_field,
                start_date,
                end_date,
                columns=columns_to_read,
            )
            df = ds.to_pandas()
            if field_mapping:
                df = df.rename(columns=field_mapping)
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
            timestamp_columns = [timestamp_field_mapped]
            if created_timestamp_column_mapped:
                timestamp_columns.append(created_timestamp_column_mapped)
            df = normalize_timestamp_columns(df, timestamp_columns, inplace=True)
            df = RayOfflineStore._process_filtered_batch(
                df,
                join_key_columns,
                feature_name_columns,
                timestamp_columns,
                timestamp_field_mapped,
            )
            existing_timestamp_columns = [
                col for col in timestamp_columns if col in df.columns
            ]
            if existing_timestamp_columns:
                df = df.sort_values(existing_timestamp_columns, ascending=False)
            df = df.reset_index(drop=True)
            return df
        except Exception as e:
            raise RuntimeError(f"Failed to load data from {source_path}: {e}")

    @staticmethod
    def _load_and_filter_dataset_ray(
        source_path: str,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str],
        start_date: Optional[datetime],
        end_date: Optional[datetime],
    ) -> Dataset:
        try:
            field_mapping = getattr(data_source, "field_mapping", None)

            if not feature_name_columns:
                columns_to_read = None
            else:
                columns_to_read = list(
                    set(join_key_columns + feature_name_columns + [timestamp_field])
                )
                if created_timestamp_column:
                    columns_to_read.append(created_timestamp_column)

            ds = RayOfflineStore._create_filtered_dataset(
                source_path,
                timestamp_field,
                start_date,
                end_date,
                columns=columns_to_read,
            )
            if field_mapping:
                ds = apply_field_mapping(ds, field_mapping)
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
            timestamp_columns = [timestamp_field_mapped]
            if created_timestamp_column_mapped:
                timestamp_columns.append(created_timestamp_column_mapped)
            # Exclude __log_timestamp from normalization as it's used for time range filtering
            exclude_columns = (
                ["__log_timestamp"] if "__log_timestamp" in timestamp_columns else []
            )
            ds = normalize_timestamp_columns(
                ds, timestamp_columns, exclude_columns=exclude_columns
            )
            ds = ds.map_batches(
                lambda batch: RayOfflineStore._process_filtered_batch(
                    batch,
                    join_key_columns,
                    feature_name_columns,
                    timestamp_columns,
                    timestamp_field_mapped,
                ),
                batch_format="pandas",
            )
            timestamp_columns_existing = [
                col for col in timestamp_columns if col in ds.schema().names
            ]
            if timestamp_columns_existing:
                ds = ds.sort(timestamp_columns_existing, descending=True)

            return ds
        except Exception as e:
            raise RuntimeError(f"Failed to load data from {source_path}: {e}")

    @staticmethod
    def _pull_latest_processing_ray(
        ds: Dataset,
        join_key_columns: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str],
        field_mapping: Optional[Dict[str, str]] = None,
    ) -> Dataset:
        """
        Ray-native processing for pull_latest operations with deduplication.
        Args:
            ds: Ray Dataset to process
            join_key_columns: List of join key columns
            timestamp_field: Name of the timestamp field
            created_timestamp_column: Optional created timestamp column
            field_mapping: Optional field mapping dictionary
        Returns:
            Ray Dataset with latest records only
        """
        if not join_key_columns:
            return ds

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

        timestamp_columns = [timestamp_field_mapped]
        if created_timestamp_column_mapped:
            timestamp_columns.append(created_timestamp_column_mapped)

        def deduplicate_batch(batch: pd.DataFrame) -> pd.DataFrame:
            if batch.empty:
                return batch

            existing_timestamp_columns = [
                col for col in timestamp_columns if col in batch.columns
            ]

            sort_columns = join_key_columns + existing_timestamp_columns
            if sort_columns:
                batch = batch.sort_values(
                    sort_columns,
                    ascending=[True] * len(join_key_columns)
                    + [False] * len(existing_timestamp_columns),
                )
                batch = batch.drop_duplicates(subset=join_key_columns, keep="first")

            return batch

        return ds.map_batches(deduplicate_batch, batch_format="pandas")

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

        def _load_ray_dataset():
            ds = store._load_and_filter_dataset_ray(
                source_path,
                data_source,
                join_key_columns,
                feature_name_columns,
                timestamp_field,
                created_timestamp_column,
                start_date,
                end_date,
            )
            field_mapping = getattr(data_source, "field_mapping", None)
            ds = store._pull_latest_processing_ray(
                ds,
                join_key_columns,
                timestamp_field,
                created_timestamp_column,
                field_mapping,
            )

            return ds

        def _load_pandas_fallback():
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

        try:
            return RayRetrievalJob(
                _load_ray_dataset,
                staging_location=config.offline_store.storage_path,
                config=config.offline_store,
            )
        except Exception as e:
            logger.warning(f"Ray-native processing failed: {e}, falling back to pandas")
            return RayRetrievalJob(
                _load_pandas_fallback,
                staging_location=config.offline_store.storage_path,
                config=config.offline_store,
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

        def _load_ray_dataset():
            return store._load_and_filter_dataset_ray(
                source_path,
                data_source,
                join_key_columns,
                feature_name_columns,
                timestamp_field,
                created_timestamp_column,
                start_date,
                end_date,
            )

        def _load_pandas_fallback():
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

        try:
            return RayRetrievalJob(
                _load_ray_dataset,
                staging_location=config.offline_store.storage_path,
                config=config.offline_store,
            )
        except Exception as e:
            logger.warning(f"Ray-native processing failed: {e}, falling back to pandas")
            return RayRetrievalJob(
                _load_pandas_fallback,
                staging_location=config.offline_store.storage_path,
                config=config.offline_store,
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

        ray_config = getattr(config, "offline_store", None)
        if (
            ray_config
            and isinstance(ray_config, RayOfflineStoreConfig)
            and not ray_config.enable_ray_logging
        ):
            RayOfflineStore._suppress_ray_logging()

        destination = logging_config.destination
        assert isinstance(destination, FileLoggingDestination), (
            f"Ray offline store only supports FileLoggingDestination for logging, "
            f"got {type(destination)}"
        )

        repo_path = getattr(config, "repo_path", None) or os.getcwd()
        absolute_path = FileSource.get_uri_for_file_path(repo_path, destination.path)

        try:
            ray_wrapper = get_ray_wrapper()
            if isinstance(data, Path):
                ds = ray_wrapper.read_parquet(str(data))
            else:
                ds = ray_wrapper.from_arrow(data)

                # Normalize feature timestamp precision to seconds to match test expectations during write
                # Note: Don't normalize __log_timestamp as it's used for time range filtering
                def normalize_timestamps(batch: pd.DataFrame) -> pd.DataFrame:
                    batch = batch.copy()
                    for col in batch.columns:
                        if (
                            pd.api.types.is_datetime64_any_dtype(batch[col])
                            and col != "__log_timestamp"
                        ):
                            batch[col] = batch[col].dt.floor("s")
                    return batch

                ds = ds.map_batches(normalize_timestamps, batch_format="pandas")
            ds = ds.materialize()
            filesystem, resolved_path = FileSource.create_filesystem_and_path(
                absolute_path, destination.s3_endpoint_override
            )
            path_obj = Path(resolved_path)
            if path_obj.suffix == ".parquet":
                path_obj = path_obj.with_suffix("")
            if not absolute_path.startswith(("s3://", "gs://")):
                path_obj.mkdir(parents=True, exist_ok=True)
            ds.write_parquet(str(path_obj))
        except Exception as e:
            raise RuntimeError(f"Failed to write logged features: {e}")

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

    @staticmethod
    def _create_filtered_dataset(
        source_path: str,
        timestamp_field: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        columns: Optional[List[str]] = None,
    ) -> Dataset:
        """Helper method to create a filtered dataset based on timestamp range."""
        ray_wrapper = get_ray_wrapper()
        ds = ray_wrapper.read_parquet(source_path, columns=columns)

        try:
            col_names = ds.schema().names
            if timestamp_field not in col_names:
                raise ValueError(
                    f"Timestamp field '{timestamp_field}' not found in columns: {col_names}"
                )
        except Exception as e:
            raise ValueError(f"Failed to get dataset schema: {e}")

        def normalize(dt):
            return make_tzaware(dt) if dt and dt.tzinfo is None else dt

        start_date = normalize(start_date)
        end_date = normalize(end_date)

        try:
            if start_date and end_date:

                def filter_by_timestamp_range(batch):
                    return (batch[timestamp_field] >= start_date) & (
                        batch[timestamp_field] <= end_date
                    )

                ds = ds.filter(filter_by_timestamp_range)
            elif start_date:

                def filter_by_start_date(batch):
                    return batch[timestamp_field] >= start_date

                ds = ds.filter(filter_by_start_date)
            elif end_date:

                def filter_by_end_date(batch):
                    return batch[timestamp_field] <= end_date

                ds = ds.filter(filter_by_end_date)
        except Exception as e:
            raise RuntimeError(f"Failed to filter dataset by timestamp: {e}")

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
        ray_wrapper = get_ray_wrapper()
        if isinstance(entity_df, str):
            entity_ds = ray_wrapper.read_csv(entity_df)
            entity_df_sample = entity_ds.limit(1000).to_pandas()
        else:
            entity_ds = ray_wrapper.from_pandas(entity_df)
            entity_df_sample = entity_df.copy()

        entity_ds = ensure_timestamp_compatibility(entity_ds, ["event_timestamp"])
        on_demand_feature_views = OnDemandFeatureView.get_requested_odfvs(
            feature_refs, project, registry
        )
        for odfv in on_demand_feature_views:
            odfv_request_data_schema = odfv.get_request_data_schema()
            for feature_name in odfv_request_data_schema.keys():
                if feature_name not in entity_df_sample.columns:
                    raise RequestDataNotFoundInEntityDfException(
                        feature_name=feature_name,
                        feature_view_name=odfv.name,
                    )

        odfv_names = {odfv.name for odfv in on_demand_feature_views}
        regular_feature_views = [
            fv for fv in feature_views if fv.name not in odfv_names
        ]
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
                if v in entity_df_sample.columns
            }
            if cols_to_rename:
                entity_ds = apply_field_mapping(entity_ds, cols_to_rename)

        result_ds = entity_ds
        for fv in regular_feature_views:
            fv_feature_refs = [
                ref
                for ref in feature_refs
                if ref.startswith(fv.projection.name_to_use() + ":")
            ]
            if not fv_feature_refs:
                continue

            entities = fv.entities or []
            entity_objs = [registry.get_entity(e, project) for e in entities]
            (
                original_join_keys,
                reverse_mapped_feature_names,
                timestamp_field,
                created_col,
            ) = _get_column_names(fv, entity_objs)

            if fv.projection.join_key_map:
                join_keys = [
                    fv.projection.join_key_map.get(key, key)
                    for key in original_join_keys
                ]
            else:
                join_keys = original_join_keys

            # Get the logical feature names from refs
            logical_requested_feats = [ref.split(":", 1)[1] for ref in fv_feature_refs]

            available_feature_names = [f.name for f in fv.features]
            missing_feats = [
                f for f in logical_requested_feats if f not in available_feature_names
            ]
            if missing_feats:
                raise KeyError(
                    f"Requested features {missing_feats} not found in feature view '{fv.name}' "
                    f"(available: {available_feature_names})"
                )

            # Build reverse field mapping to get actual source column names
            reverse_field_mapping = {}
            if fv.batch_source.field_mapping:
                reverse_field_mapping = {
                    v: k for k, v in fv.batch_source.field_mapping.items()
                }

            # Map logical feature names to actual source column names
            requested_feats = [
                reverse_field_mapping.get(feat, feat)
                for feat in logical_requested_feats
            ]

            source_info = resolve_feature_view_source_with_fallback(
                fv, config, is_materialization=False
            )

            # Read from the resolved data source
            source_path = store._get_source_path(source_info.data_source, config)

            if not source_info.has_transformation:
                required_feature_columns = set(
                    original_join_keys + requested_feats + [timestamp_field]
                )
                if created_col:
                    required_feature_columns.add(created_col)
                feature_ds = ray_wrapper.read_parquet(
                    source_path, columns=list(required_feature_columns)
                )
            else:
                feature_ds = ray_wrapper.read_parquet(source_path)

            # Apply transformation if available
            if source_info.has_transformation and source_info.transformation_func:
                transformation_serialized = dill.dumps(source_info.transformation_func)

                def apply_transformation_with_serialized_func(
                    batch: pd.DataFrame,
                ) -> pd.DataFrame:
                    if batch.empty:
                        return batch
                    try:
                        logger.debug(
                            f"Applying transformation to batch with columns: {list(batch.columns)}"
                        )
                        transformation_func = dill.loads(transformation_serialized)
                        result = transformation_func(batch)
                        logger.debug(
                            f"Transformation result has columns: {list(result.columns)}"
                        )
                        return result
                    except Exception as e:
                        logger.error(f"Transformation failed for {fv.name}: {e}")
                        return batch

                feature_ds = feature_ds.map_batches(
                    apply_transformation_with_serialized_func, batch_format="pandas"
                )
                logger.info(f"Applied transformation to feature view {fv.name}")
            elif source_info.has_transformation:
                logger.warning(
                    f"Feature view {fv.name} marked as having transformation but no UDF found"
                )

            feature_size = feature_ds.size_bytes() or 0

            field_mapping = getattr(fv.batch_source, "field_mapping", None)
            if field_mapping:
                feature_ds = apply_field_mapping(feature_ds, field_mapping)
                # Update original_join_keys to logical names after forward mapping
                original_join_keys = [
                    field_mapping.get(k, k) for k in original_join_keys
                ]
                # Recompute join_keys from updated original_join_keys
                if fv.projection.join_key_map:
                    join_keys = [
                        fv.projection.join_key_map.get(key, key)
                        for key in original_join_keys
                    ]
                else:
                    join_keys = original_join_keys
                timestamp_field = field_mapping.get(timestamp_field, timestamp_field)
                if created_col:
                    created_col = field_mapping.get(created_col, created_col)
                # Also map requested_feats back to logical names after forward mapping
                requested_feats = [field_mapping.get(f, f) for f in requested_feats]

            if (
                timestamp_field != "event_timestamp"
                and timestamp_field not in entity_df_sample.columns
                and "event_timestamp" in entity_df_sample.columns
            ):

                def add_timestamp_field(batch: pd.DataFrame) -> pd.DataFrame:
                    batch = batch.copy()
                    batch[timestamp_field] = batch["event_timestamp"]
                    return batch

                result_ds = result_ds.map_batches(
                    add_timestamp_field, batch_format="pandas"
                )
                result_ds = normalize_timestamp_columns(result_ds, timestamp_field)

            if store._resource_manager is None:
                raise ValueError("Resource manager not initialized")
            requirements = store._resource_manager.estimate_processing_requirements(
                feature_size, "join"
            )

            if requirements["should_broadcast"]:
                # Use broadcast join for small feature datasets
                if getattr(store._resource_manager.config, "enable_ray_logging", False):
                    logger.info(
                        f"Using broadcast join for {fv.name} (size: {feature_size // 1024**2}MB)"
                    )
                feature_df = feature_ds.to_pandas()
                feature_df = ensure_timestamp_compatibility(
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
                if getattr(store._resource_manager.config, "enable_ray_logging", False):
                    logger.info(
                        f"Using distributed join for {fv.name} (size: {feature_size // 1024**2}MB)"
                    )
                feature_ds = ensure_timestamp_compatibility(
                    feature_ds, [timestamp_field]
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

        def finalize_result(batch: pd.DataFrame) -> pd.DataFrame:
            batch = batch.copy()

            existing_columns = set(batch.columns)
            for col in entity_df_sample.columns:
                if col not in existing_columns:
                    if len(batch) <= len(entity_df_sample):
                        batch[col] = entity_df_sample[col].iloc[: len(batch)].values
                    else:
                        repeated_values = np.tile(
                            entity_df_sample[col].values,
                            (len(batch) // len(entity_df_sample) + 1),
                        )
                        batch[col] = repeated_values[: len(batch)]

            if "event_timestamp" not in batch.columns:
                if "event_timestamp" in entity_df_sample.columns:
                    batch["event_timestamp"] = (
                        entity_df_sample["event_timestamp"].iloc[: len(batch)].values
                    )
                    batch = normalize_timestamp_columns(
                        batch, "event_timestamp", inplace=True
                    )
                elif timestamp_field in batch.columns:
                    batch["event_timestamp"] = batch[timestamp_field]

            return batch

        result_ds = result_ds.map_batches(finalize_result, batch_format="pandas")
        result_ds = _convert_feature_column_types(result_ds, regular_feature_views)

        storage_path = config.offline_store.storage_path
        if not storage_path:
            raise ValueError("Storage path must be set in config")

        job = RayRetrievalJob(
            result_ds, staging_location=storage_path, config=config.offline_store
        )
        job._full_feature_names = full_feature_names
        job._on_demand_feature_views = on_demand_feature_views
        job._feature_refs = feature_refs
        job._entity_df = entity_df_sample
        job._metadata = job._create_metadata()
        return job
