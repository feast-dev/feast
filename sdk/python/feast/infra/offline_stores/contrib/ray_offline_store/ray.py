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
from feast.infra.registry.base_registry import BaseRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage, ValidationReference
from feast.utils import _get_column_names, _utc_now, make_df_tzaware

logger = logging.getLogger(__name__)


class RayOfflineStoreConfig(FeastConfigBaseModel):
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
    """Manages Ray cluster resources for optimal performance."""

    def __init__(self, config: Optional[RayOfflineStoreConfig] = None):
        self.config = config or RayOfflineStoreConfig()
        self.cluster_resources = ray.cluster_resources()
        self.available_memory = self.cluster_resources.get(
            "memory", 8 * 1024**3
        )  # 8GB default
        self.available_cpus = int(self.cluster_resources.get("CPU", 4))
        self.num_nodes = len(ray.nodes()) if ray.is_initialized() else 1

    def configure_ray_context(self):
        """Configure Ray DatasetContext for optimal performance."""
        ctx = DatasetContext.get_current()

        # Set buffer sizes based on available memory
        if self.available_memory > 32 * 1024**3:  # 32GB
            ctx.target_shuffle_buffer_size = 2 * 1024**3  # 2GB
            ctx.target_max_block_size = 512 * 1024**2  # 512MB
        else:
            ctx.target_shuffle_buffer_size = 512 * 1024**2  # 512MB
            ctx.target_max_block_size = 128 * 1024**2  # 128MB

        # Configure parallelism
        ctx.min_parallelism = self.available_cpus
        ctx.max_parallelism = (
            self.available_cpus * self.config.max_parallelism_multiplier
        )

        # Optimize for feature store workloads
        ctx.shuffle_strategy = "sort"
        ctx.enable_tensor_extension_casting = False

        logger.info(
            f"Configured Ray context: {self.available_cpus} CPUs, "
            f"{self.available_memory // 1024**3}GB memory, {self.num_nodes} nodes"
        )

    def estimate_optimal_partitions(self, dataset_size_bytes: int) -> int:
        """Estimate optimal number of partitions for a dataset."""
        # Use configured target partition size
        target_partition_size = (self.config.target_partition_size_mb or 64) * 1024**2
        size_based_partitions = max(1, dataset_size_bytes // target_partition_size)

        # Don't exceed configured max parallelism
        max_partitions = self.available_cpus * (
            self.config.max_parallelism_multiplier or 2
        )

        return min(size_based_partitions, max_partitions)

    def should_use_broadcast_join(
        self, dataset_size_bytes: int, threshold_mb: Optional[int] = None
    ) -> bool:
        """Determine if dataset is small enough for broadcast join."""
        threshold = (
            threshold_mb
            if threshold_mb is not None
            else (self.config.broadcast_join_threshold_mb or 100)
        )
        return dataset_size_bytes <= threshold * 1024**2

    def estimate_processing_requirements(
        self, dataset_size_bytes: int, operation_type: str
    ) -> Dict[str, Any]:
        """Estimate resource requirements for different operations."""

        # Memory requirements (with safety margin)
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
    """Optimized data processing with Ray for feature store operations."""

    def __init__(self, resource_manager: RayResourceManager):
        self.resource_manager = resource_manager

    def optimize_dataset_for_join(self, ds: Dataset, join_keys: List[str]) -> Dataset:
        """Optimize dataset partitioning for join operations."""

        # Estimate optimal partitions
        dataset_size = ds.size_bytes()
        optimal_partitions = self.resource_manager.estimate_optimal_partitions(
            dataset_size
        )

        if not join_keys:
            # For datasets without join keys, use simple repartitioning
            return ds.repartition(num_blocks=optimal_partitions)

        # For datasets with join keys, use shuffle for better distribution
        return ds.random_shuffle(num_blocks=optimal_partitions)

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

            logger.debug(f"Broadcast join - Features DataFrame shape: {features.shape}")
            logger.debug(
                f"Broadcast join - Features DataFrame columns: {list(features.columns)}"
            )
            logger.debug(f"Broadcast join - Requested features: {requested_feats}")
            logger.debug(f"Broadcast join - Join keys: {join_keys}")
            logger.debug(f"Broadcast join - Timestamp field: {timestamp_field}")
            logger.debug(f"Broadcast join - Batch DataFrame shape: {batch.shape}")
            logger.debug(
                f"Broadcast join - Batch DataFrame columns: {list(batch.columns)}"
            )
            if feature_view_name:
                logger.info(
                    f"Processing feature view {feature_view_name} with join keys {join_keys}"
                )

            # Select only required feature columns plus join keys and timestamp
            # Use original join keys for filtering if provided (for entity mapping)
            filter_join_keys = original_join_keys if original_join_keys else join_keys
            feature_cols = [timestamp_field] + filter_join_keys + requested_feats
            features_filtered = features[feature_cols].copy()

            logger.debug(
                f"Broadcast join - Features filtered shape: {features_filtered.shape}"
            )
            logger.debug(
                f"Broadcast join - Features filtered columns: {list(features_filtered.columns)}"
            )

            # Ensure timestamp columns have compatible dtypes and precision
            if timestamp_field in batch.columns:
                batch[timestamp_field] = (
                    pd.to_datetime(batch[timestamp_field], utc=True, errors="coerce")
                    .dt.floor("s")
                    .astype("datetime64[ns, UTC]")
                )

            if timestamp_field in features_filtered.columns:
                features_filtered[timestamp_field] = (
                    pd.to_datetime(
                        features_filtered[timestamp_field], utc=True, errors="coerce"
                    )
                    .dt.floor("s")
                    .astype("datetime64[ns, UTC]")
                )

            if not join_keys:
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
                # Temporal join with entity keys
                # Clean data first by removing NaN values
                # Use original join keys for filtering feature dataset, mapped join keys for entity dataset
                feature_join_keys = (
                    original_join_keys if original_join_keys else join_keys
                )

                for key in join_keys:
                    if key not in batch.columns:
                        batch[key] = None
                for key in feature_join_keys:
                    if key not in features_filtered.columns:
                        features_filtered[key] = None

                # Drop rows with NaN values in join keys or timestamp
                batch_clean = batch.dropna(subset=join_keys + [timestamp_field]).copy()
                features_clean = features_filtered.dropna(
                    subset=feature_join_keys + [timestamp_field]
                ).copy()

                # If no valid data remains, return empty result
                if batch_clean.empty or features_clean.empty:
                    return batch.head(0)  # Return empty dataframe with same columns

                # Important: For merge_asof with 'by' parameter, sort by 'by' columns first, then by 'on' column
                # Both DataFrames must be sorted identically
                batch_sort_columns = join_keys + [timestamp_field]
                features_sort_columns = feature_join_keys + [timestamp_field]

                try:
                    # For entity mapping, we need to manually join since merge_asof doesn't support different column names
                    if original_join_keys and original_join_keys != join_keys:
                        # Manual join for entity mapping
                        logger.info("Using manual join for entity mapping")
                        raise ValueError("Entity mapping requires manual join")

                    # For multi-entity joins, use manual join to ensure correctness
                    if len(join_keys) > 1:
                        logger.info(
                            f"Using manual join for multi-entity join with keys: {join_keys}"
                        )
                        raise ValueError("Multi-entity join requires manual join")

                    # Sort both DataFrames consistently
                    batch_sorted = batch_clean.sort_values(
                        batch_sort_columns, ascending=True
                    ).reset_index(drop=True)

                    features_sorted = features_clean.sort_values(
                        features_sort_columns, ascending=True
                    ).reset_index(drop=True)

                    # Verify sorting (merge_asof requirement)
                    for key in join_keys:
                        if not batch_sorted[key].is_monotonic_increasing:
                            # If not monotonic, we need to handle this differently
                            logger.warning(
                                f"Join key {key} is not monotonic, using manual join"
                            )
                            raise ValueError(f"Join key {key} is not monotonic")
                        if not features_sorted[key].is_monotonic_increasing:
                            logger.warning(
                                f"Feature join key {key} is not monotonic, using manual join"
                            )
                            raise ValueError(f"Feature join key {key} is not monotonic")

                    # Perform merge_asof
                    result = pd.merge_asof(
                        batch_sorted,
                        features_sorted,
                        on=timestamp_field,
                        by=join_keys,
                        direction="backward",
                    )
                    logger.debug(
                        f"merge_asof succeeded for batch of size {len(batch_sorted)}"
                    )

                except (ValueError, KeyError) as e:
                    # If merge_asof fails, implement manual point-in-time join
                    logger.warning(
                        f"merge_asof failed, implementing manual point-in-time join: {e}"
                    )

                    # Group by join keys and apply point-in-time logic manually
                    result_chunks = []

                    logger.debug(
                        f"Manual join - batch_clean shape: {batch_clean.shape}"
                    )
                    logger.debug(
                        f"Manual join - features_clean shape: {features_clean.shape}"
                    )
                    logger.debug(f"Manual join - join_keys: {join_keys}")
                    logger.debug(
                        f"Manual join - feature_join_keys: {feature_join_keys}"
                    )

                    for join_key_vals, entity_group in batch_clean.groupby(join_keys):
                        # Create dictionary for filtering features by join keys
                        if len(join_keys) == 1:
                            entity_key_filter = {join_keys[0]: join_key_vals}
                        else:
                            entity_key_filter = dict(zip(join_keys, join_key_vals))

                        # For entity mapping, map the entity keys to feature keys
                        feature_key_filter = {}
                        for i, entity_key in enumerate(join_keys):
                            feature_key = (
                                feature_join_keys[i]
                                if i < len(feature_join_keys)
                                else entity_key
                            )
                            feature_key_filter[feature_key] = entity_key_filter[
                                entity_key
                            ]

                        # Filter features for this join key group
                        feature_group = features_clean
                        for key, val in feature_key_filter.items():
                            # Only filter if the key exists in the feature dataset
                            if key in feature_group.columns:
                                feature_group = feature_group[feature_group[key] == val]
                                logger.debug(
                                    f"Filtered by {key}={val}: {len(feature_group)} rows remaining"
                                )
                            else:
                                logger.warning(
                                    f"Join key {key} not found in feature dataset columns: {list(feature_group.columns)}"
                                )
                                # If the key is missing, we can't match, so return empty
                                feature_group = feature_group.iloc[0:0]
                                break

                        if len(feature_group) == 0:
                            # No features found, add NaN columns
                            entity_result = entity_group.copy()
                            for feat in requested_feats:
                                if feat not in entity_result.columns:
                                    entity_result[feat] = np.nan
                            result_chunks.append(entity_result)
                        else:
                            # Apply point-in-time logic: for each entity timestamp, find the latest feature
                            entity_result = entity_group.copy()
                            for feat in requested_feats:
                                if feat not in entity_result.columns:
                                    entity_result[feat] = np.nan

                            # For each row in entity group, find the latest feature value
                            for idx, entity_row in entity_group.iterrows():
                                entity_ts = entity_row[timestamp_field]
                                # Find features with timestamp <= entity timestamp
                                valid_features = feature_group[
                                    feature_group[timestamp_field] <= entity_ts
                                ]
                                if len(valid_features) > 0:
                                    # Sort by timestamp to ensure we get the latest feature
                                    valid_features = valid_features.sort_values(
                                        timestamp_field
                                    )
                                    latest_feature = valid_features.iloc[-1]
                                    # Update the result with feature values
                                    for feat in requested_feats:
                                        if feat in latest_feature:
                                            entity_result.loc[idx, feat] = (
                                                latest_feature[feat]
                                            )

                            result_chunks.append(entity_result)

                    if result_chunks:
                        result = pd.concat(result_chunks, ignore_index=True)
                    else:
                        result = batch_clean.copy()
                        for feat in requested_feats:
                            if feat not in result.columns:
                                result[feat] = np.nan

            # Debug logging for join result
            logger.debug(f"Join result shape: {result.shape}")
            logger.debug(f"Join result columns: {list(result.columns)}")

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
            batch = batch.copy()
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
            return self._cached_df

        # If we have on-demand feature views, use the parent's implementation
        # which calls to_arrow and applies the transformations
        if self.on_demand_feature_views:
            logger.info(
                f"Using parent implementation for {len(self.on_demand_feature_views)} ODFVs"
            )
            return super().to_df(
                validation_reference=validation_reference, timeout=timeout
            )

        result = self._resolve()
        if isinstance(result, pd.DataFrame):
            self._cached_df = result
            return result

        # Convert Ray Dataset to DataFrame with progress logging
        logger.info("Converting Ray dataset to DataFrame...")
        self._cached_df = result.to_pandas()
        logger.info(f"Converted dataset to DataFrame: {self._cached_df.shape}")
        return self._cached_df

    def to_arrow(
        self,
        validation_reference: Optional[ValidationReference] = None,
        timeout: Optional[int] = None,
    ) -> pa.Table:
        # If we have ODFVs, use the parent's implementation
        if self.on_demand_feature_views:
            logger.debug(
                f"Using parent implementation for {len(self.on_demand_feature_views)} ODFVs"
            )
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
            logger.debug(f"_to_arrow_internal: DataFrame shape: {result.shape}")
            logger.debug(
                f"_to_arrow_internal: DataFrame columns: {list(result.columns)}"
            )
            return pa.Table.from_pandas(result)

        # For Ray Dataset, convert to pandas first then to arrow
        logger.debug(
            "_to_arrow_internal: Converting Ray Dataset to pandas then to arrow"
        )
        df = result.to_pandas()
        logger.debug(f"_to_arrow_internal: Converted dataset shape: {df.shape}")
        logger.debug(
            f"_to_arrow_internal: Converted dataset columns: {list(df.columns)}"
        )
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
            ds = self._resolve()
            if not destination_path.startswith(("s3://", "gs://", "hdfs://")):
                os.makedirs(os.path.dirname(destination_path), exist_ok=True)
            ds.write_parquet(destination_path)
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

        # Initialize optimization components
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
                    filtered_ds = ds.filter(
                        lambda row: start_date <= row[timestamp_field] <= end_date
                    )
                elif start_date:
                    filtered_ds = ds.filter(
                        lambda row: row[timestamp_field] >= start_date
                    )
                elif end_date:
                    filtered_ds = ds.filter(
                        lambda row: row[timestamp_field] <= end_date
                    )
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

        # Make entity dataframe timezone aware
        original_entity_df = make_df_tzaware(original_entity_df)
        if "event_timestamp" in original_entity_df.columns:
            original_entity_df["event_timestamp"] = (
                pd.to_datetime(
                    original_entity_df["event_timestamp"], utc=True, errors="coerce"
                )
                .dt.floor("s")
                .astype("datetime64[ns, UTC]")
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

        logger.info(
            f"Processing {len(regular_feature_views)} regular feature views and {len(on_demand_feature_views)} on-demand feature views with {len(feature_refs)} feature references"
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

            logger.info(f"Processing feature view: {fv.name}")

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

            logger.info(
                f"Feature view '{fv.name}': requesting {requested_feats}, available: {available_feature_names}"
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

            # Apply projection join key mapping to entity dataset if needed
            if fv.projection.join_key_map:
                # The feature dataset keeps its original columns (e.g., location_id)
                # The entity dataset gets the mapped columns (e.g., origin_id, destination_id)
                # We need to ensure the entity dataset has the properly mapped columns
                pass  # The entity dataset already has the mapped columns in this case

            # Ensure timestamp compatibility in entity dataset
            if (
                timestamp_field != "event_timestamp"
                and timestamp_field not in original_entity_df.columns
                and "event_timestamp" in original_entity_df.columns
            ):

                def add_timestamp_field(batch: pd.DataFrame) -> pd.DataFrame:
                    batch = batch.copy()
                    batch[timestamp_field] = (
                        pd.to_datetime(
                            batch["event_timestamp"], utc=True, errors="coerce"
                        )
                        .dt.floor("s")
                        .astype("datetime64[ns, UTC]")
                    )
                    return batch

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
                feature_df = make_df_tzaware(feature_df)

                if timestamp_field in feature_df.columns:
                    feature_df[timestamp_field] = (
                        pd.to_datetime(
                            feature_df[timestamp_field], utc=True, errors="coerce"
                        )
                        .dt.floor("s")
                        .astype("datetime64[ns, UTC]")
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
                    original_join_keys,
                )
            else:
                # Use distributed windowed join for large feature datasets
                logger.info(
                    f"Using distributed join for {fv.name} (size: {feature_size // 1024**2}MB)"
                )

                # Ensure timestamp format in feature dataset
                def normalize_timestamps(batch: pd.DataFrame) -> pd.DataFrame:
                    batch = make_df_tzaware(batch)
                    if timestamp_field in batch.columns:
                        batch[timestamp_field] = (
                            pd.to_datetime(
                                batch[timestamp_field], utc=True, errors="coerce"
                            )
                            .dt.floor("s")
                            .astype("datetime64[ns, UTC]")
                        )
                    return batch

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
                    original_join_keys=original_join_keys,
                )

        # Final processing: clean up and ensure proper column structure
        def finalize_result(batch: pd.DataFrame) -> pd.DataFrame:
            logger.debug(f"Finalizing result - input columns: {list(batch.columns)}")
            logger.debug(f"Finalizing result - batch shape: {batch.shape}")

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
                        pd.to_datetime(
                            original_entity_df["event_timestamp"].iloc[: len(batch)],
                            utc=True,
                            errors="coerce",
                        )
                        .dt.floor("s")
                        .astype("datetime64[ns, UTC]")
                    )
                elif timestamp_field in batch.columns:
                    batch["event_timestamp"] = batch[timestamp_field]

            logger.debug(f"Final columns: {list(batch.columns)}")
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

        logger.info("Historical features processing completed successfully")
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
            try:
                # Load and filter the dataset
                ds = RayOfflineStore._create_filtered_dataset(
                    source_path, timestamp_field, start_date, end_date
                )

                # Convert to pandas for deduplication and column selection
                df = ds.to_pandas()
                df = make_df_tzaware(df)

                # Apply field mapping if needed
                field_mapping = getattr(data_source, "field_mapping", None)
                if field_mapping:
                    df = df.rename(columns=field_mapping)

                # Use the actual timestamp field name (this is already the correct mapped name)
                timestamp_field_mapped = timestamp_field
                created_timestamp_column_mapped = created_timestamp_column

                # Handle empty DataFrame case
                if df.empty:
                    logger.info(
                        "DataFrame is empty after filtering, creating empty DataFrame with required columns"
                    )
                    # Create an empty DataFrame with the required columns
                    empty_columns = (
                        join_key_columns
                        + feature_name_columns
                        + [timestamp_field_mapped]
                    )
                    if created_timestamp_column_mapped:
                        empty_columns.append(created_timestamp_column_mapped)
                    if not join_key_columns:
                        empty_columns.append(DUMMY_ENTITY_ID)

                    # Add event_timestamp column for pandas backend compatibility
                    if "event_timestamp" not in empty_columns:
                        empty_columns.append("event_timestamp")

                    # Create empty DataFrame with proper column types
                    empty_df = pd.DataFrame(columns=empty_columns)
                    return empty_df

                # Ensure timestamp is properly formatted
                if timestamp_field_mapped in df.columns:
                    df[timestamp_field_mapped] = (
                        pd.to_datetime(
                            df[timestamp_field_mapped], utc=True, errors="coerce"
                        )
                        .dt.floor("s")
                        .astype("datetime64[ns, UTC]")
                    )

                if (
                    created_timestamp_column_mapped
                    and created_timestamp_column_mapped in df.columns
                ):
                    df[created_timestamp_column_mapped] = (
                        pd.to_datetime(
                            df[created_timestamp_column_mapped],
                            utc=True,
                            errors="coerce",
                        )
                        .dt.floor("s")
                        .astype("datetime64[ns, UTC]")
                    )

                # Prepare columns to select
                timestamp_columns = [timestamp_field_mapped]
                if created_timestamp_column_mapped:
                    timestamp_columns.append(created_timestamp_column_mapped)

                all_required_columns = (
                    join_key_columns + feature_name_columns + timestamp_columns
                )

                # Select only the required columns that exist
                available_columns = [
                    col for col in all_required_columns if col in df.columns
                ]
                df = df[available_columns]

                # Handle deduplication (keep latest records)
                if join_key_columns:
                    # Sort by timestamp columns (latest first) and deduplicate by join keys
                    # Filter out timestamp columns that don't exist in the dataframe
                    existing_timestamp_columns = [
                        col for col in timestamp_columns if col in df.columns
                    ]
                    sort_columns = join_key_columns + existing_timestamp_columns
                    if sort_columns:
                        df = df.sort_values(
                            sort_columns,
                            ascending=[True] * len(join_key_columns)
                            + [False] * len(existing_timestamp_columns),
                        )
                        df = df.drop_duplicates(subset=join_key_columns, keep="first")
                else:
                    # No join keys - add dummy entity and sort by timestamp
                    df[DUMMY_ENTITY_ID] = DUMMY_ENTITY_VAL
                    # Filter out timestamp columns that don't exist in the dataframe
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
                        logger.debug(
                            f"Added 'event_timestamp' column from '{timestamp_field_mapped}' for pandas backend compatibility"
                        )

                return df

            except Exception as e:
                raise RuntimeError(f"Failed to load data from {source_path}: {e}")

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
            try:
                # Load and filter the dataset
                ds = RayOfflineStore._create_filtered_dataset(
                    source_path, timestamp_field, start_date, end_date
                )

                # Convert to pandas for column selection
                df = ds.to_pandas()
                df = make_df_tzaware(df)

                # Apply field mapping if needed
                field_mapping = getattr(data_source, "field_mapping", None)
                if field_mapping:
                    df = df.rename(columns=field_mapping)

                # Use the actual timestamp field name (this is already the correct mapped name)
                timestamp_field_mapped = timestamp_field
                created_timestamp_column_mapped = created_timestamp_column

                # Debug logging
                logger.debug(f"DataFrame columns: {df.columns.tolist()}")
                logger.debug(f"Timestamp field: {timestamp_field_mapped}")
                logger.debug(
                    f"Created timestamp column: {created_timestamp_column_mapped}"
                )
                logger.debug(f"DataFrame shape: {df.shape}")

                # Handle empty DataFrame case
                if df.empty:
                    logger.info(
                        "DataFrame is empty after filtering, creating empty DataFrame with required columns"
                    )
                    # Create an empty DataFrame with the required columns
                    empty_columns = (
                        join_key_columns
                        + feature_name_columns
                        + [timestamp_field_mapped]
                    )
                    if created_timestamp_column_mapped:
                        empty_columns.append(created_timestamp_column_mapped)
                    if not join_key_columns:
                        empty_columns.append(DUMMY_ENTITY_ID)

                    # Add event_timestamp column for pandas backend compatibility
                    if "event_timestamp" not in empty_columns:
                        empty_columns.append("event_timestamp")

                    # Create empty DataFrame with proper column types
                    empty_df = pd.DataFrame(columns=empty_columns)
                    return empty_df

                # Ensure timestamp is properly formatted
                if timestamp_field_mapped in df.columns:
                    df[timestamp_field_mapped] = (
                        pd.to_datetime(
                            df[timestamp_field_mapped], utc=True, errors="coerce"
                        )
                        .dt.floor("s")
                        .astype("datetime64[ns, UTC]")
                    )

                if (
                    created_timestamp_column_mapped
                    and created_timestamp_column_mapped in df.columns
                ):
                    df[created_timestamp_column_mapped] = (
                        pd.to_datetime(
                            df[created_timestamp_column_mapped],
                            utc=True,
                            errors="coerce",
                        )
                        .dt.floor("s")
                        .astype("datetime64[ns, UTC]")
                    )

                # Prepare columns to select
                timestamp_columns = [timestamp_field_mapped]
                if created_timestamp_column_mapped:
                    timestamp_columns.append(created_timestamp_column_mapped)

                all_required_columns = (
                    join_key_columns + feature_name_columns + timestamp_columns
                )

                # Add dummy entity if no join keys
                if not join_key_columns:
                    df[DUMMY_ENTITY_ID] = DUMMY_ENTITY_VAL
                    all_required_columns.append(DUMMY_ENTITY_ID)

                # Select only the required columns that exist
                available_columns = [
                    col for col in all_required_columns if col in df.columns
                ]
                df = df[available_columns]

                # Sort by timestamp (most recent first)
                # Filter out timestamp columns that don't exist in the dataframe
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
                        logger.debug(
                            f"Added 'event_timestamp' column from '{timestamp_field_mapped}' for pandas backend compatibility"
                        )

                return df

            except Exception as e:
                raise RuntimeError(f"Failed to load data from {source_path}: {e}")

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
        base_storage_path = ray_config.storage_path or "/tmp/ray-storage"

        batch_source_path = getattr(feature_view.batch_source, "file_path", None)
        if not batch_source_path:
            batch_source_path = f"{feature_view.name}/push_{_utc_now()}.parquet"

        feature_path = FileSource.get_uri_for_file_path(repo_path, batch_source_path)
        storage_path = FileSource.get_uri_for_file_path(repo_path, base_storage_path)

        feature_dir = os.path.dirname(feature_path)
        if not feature_dir.startswith(("s3://", "gs://")):
            os.makedirs(feature_dir, exist_ok=True)
        if not storage_path.startswith(("s3://", "gs://")):
            os.makedirs(os.path.dirname(storage_path), exist_ok=True)

        df = table.to_pandas()
        ds = ray.data.from_pandas(df)
        ds.materialize()
        ds.write_parquet(feature_dir)

    @staticmethod
    def create_saved_dataset_destination(
        config: RepoConfig,
        name: str,
        path: Optional[str] = None,
    ) -> SavedDatasetStorage:
        """Create a saved dataset destination for Ray offline store."""

        if path is None:
            # Use default path based on config
            ray_config = config.offline_store
            assert isinstance(ray_config, RayOfflineStoreConfig)
            base_storage_path = ray_config.storage_path or "/tmp/ray-storage"
            path = f"{base_storage_path}/saved_datasets/{name}.parquet"

        return SavedDatasetFileStorage(path=path)
