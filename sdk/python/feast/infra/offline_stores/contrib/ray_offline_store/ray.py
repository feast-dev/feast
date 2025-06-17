import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable, List, Literal, Optional, Tuple, Union

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
from feast.infra.offline_stores.offline_utils import get_expected_join_keys
from feast.infra.registry.base_registry import BaseRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage, ValidationReference
from feast.utils import _get_column_names, _utc_now, make_df_tzaware


class RayRetrievalJob(RetrievalJob):
    def __init__(
        self,
        dataset_or_callable: Union[Dataset, Callable[[], Dataset]],
        staging_location: Optional[str] = None,
    ):
        self._dataset_or_callable = dataset_or_callable
        self._staging_location = staging_location
        self._cached_dataset: Optional[Dataset] = None
        self._metadata: Optional[RetrievalMetadata] = None
        self._full_feature_names: bool = False
        self._on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None

    def _resolve(self) -> Any:
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
        result = self._resolve()
        if isinstance(result, pd.DataFrame):
            return result
        return result.to_pandas()

    def to_arrow(
        self,
        validation_reference: Optional[ValidationReference] = None,
        timeout: Optional[int] = None,
    ) -> pa.Table:
        result = self._resolve()
        if isinstance(result, pd.DataFrame):
            return pa.Table.from_pandas(result)
        # For Ray Dataset, convert to pandas first then to arrow
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
            return pa.Table.from_pandas(result)
        # For Ray Dataset, convert to pandas first then to arrow
        return pa.Table.from_pandas(result.to_pandas())

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


class RayOfflineStoreConfig(FeastConfigBaseModel):
    type: Literal[
        "feast.offline_stores.contrib.ray_offline_store.ray.RayOfflineStore", "ray"
    ] = "ray"
    storage_path: Optional[str] = None
    ray_address: Optional[str] = None
    use_ray_cluster: Optional[bool] = False


class RayOfflineStore(OfflineStore):
    def __init__(self):
        self._staging_location: Optional[str] = None
        self._ray_initialized: bool = False

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
        ctx.shuffle_strategy = "sort"
        ctx.enable_tensor_extension_casting = False

    def _init_ray(self, config: RepoConfig):
        ray_config = config.offline_store
        assert isinstance(ray_config, RayOfflineStoreConfig)

        self._ensure_ray_initialized(config)

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

        # Load entity_df
        original_entity_df = (
            pd.read_csv(entity_df) if isinstance(entity_df, str) else entity_df.copy()
        )
        result_df = make_df_tzaware(original_entity_df.copy())
        if "event_timestamp" in result_df.columns:
            result_df["event_timestamp"] = pd.to_datetime(
                result_df["event_timestamp"], utc=True, errors="coerce"
            ).dt.floor("s")

        # Parse feature_refs and get ODFVs
        on_demand_feature_views = OnDemandFeatureView.get_requested_odfvs(
            feature_refs, project, registry
        )

        # --- Request Data Validation for ODFVs ---
        for odfv in on_demand_feature_views:
            odfv_request_data_schema = odfv.get_request_data_schema()
            for feature_name in odfv_request_data_schema.keys():
                if feature_name not in original_entity_df.columns:
                    raise RequestDataNotFoundInEntityDfException(
                        feature_name=feature_name,
                        feature_view_name=odfv.name,
                    )

        # Collect all join keys from feature views
        all_join_keys = get_expected_join_keys(project, feature_views, registry)
        if "event_timestamp" in result_df.columns:
            all_join_keys.add("event_timestamp")

        # Keep only relevant entity columns and timestamp
        result_df = result_df[
            [col for col in result_df.columns if col in all_join_keys]
        ]

        requested_feature_columns = []
        added_dummy_columns = set()

        # Join each feature view
        for fv in feature_views:
            # Only process feature views that are referenced
            fv_feature_refs = [
                ref for ref in feature_refs if ref.startswith(fv.name + ":")
            ]
            if not fv_feature_refs:
                continue

            # Get join keys, feature names, timestamp, created timestamp
            entities = fv.entities or []
            entity_objs = [registry.get_entity(e, project) for e in entities]
            join_keys, feature_names, timestamp_field, created_col = _get_column_names(
                fv, entity_objs
            )
            if not join_keys:
                join_keys = [DUMMY_ENTITY_ID]

            # Only add features that are actually requested in feature_refs
            requested_feats = [ref.split(":", 1)[1] for ref in fv_feature_refs]

            # --- Error for Missing Features ---
            available_feature_names = [f.name for f in fv.features]
            missing_feats = [
                f for f in requested_feats if f not in available_feature_names
            ]
            if missing_feats:
                raise KeyError(
                    f"Requested features {missing_feats} not found in feature view '{fv.name}' (available: {available_feature_names})"
                )

            for feat in requested_feats:
                col_name = f"{fv.name}__{feat}" if full_feature_names else feat
                requested_feature_columns.append(col_name)

            # Read feature data
            source_path = store._get_source_path(fv.batch_source, config)
            if not source_path:
                raise ValueError(f"Missing batch source for FV {fv.name}")
            feature_ds = ray.data.read_parquet(str(source_path))
            feature_df = feature_ds.to_pandas()
            feature_df = make_df_tzaware(feature_df)
            if timestamp_field in feature_df.columns:
                feature_df[timestamp_field] = pd.to_datetime(
                    feature_df[timestamp_field], utc=True, errors="coerce"
                ).dt.floor("s")

            # Ensure join keys exist in both entity and feature dataframe
            for k in join_keys:
                if k not in result_df.columns:
                    result_df[k] = DUMMY_ENTITY_VAL
                    added_dummy_columns.add(k)
                if k not in feature_df.columns:
                    feature_df[k] = DUMMY_ENTITY_VAL

            if (
                timestamp_field not in result_df.columns
                and "event_timestamp" in result_df.columns
            ):
                result_df[timestamp_field] = result_df["event_timestamp"]

            # Align join key dtypes before merge
            for k in join_keys:
                if k in result_df.columns and k in feature_df.columns:
                    feature_df[k] = feature_df[k].astype(result_df[k].dtype)

            # Deduplicate feature values (avoid list columns in keys)
            dedup_keys = join_keys + [timestamp_field]
            if created_col and created_col in feature_df.columns:
                feature_df = feature_df.sort_values(by=dedup_keys + [created_col])
                feature_df = feature_df.groupby(dedup_keys, as_index=False).last()
            else:
                feature_df = feature_df.sort_values(by=dedup_keys)
                feature_df = feature_df.drop_duplicates(subset=dedup_keys, keep="last")

            # Select only requested features that exist in feature_df
            existing_feats = [f for f in requested_feats if f in feature_df.columns]
            cols_to_keep = join_keys + [timestamp_field] + existing_feats
            feature_df = feature_df[cols_to_keep]

            # Join into result_df
            result_df = result_df.merge(
                feature_df,
                how="inner",
                on=join_keys
                + ([timestamp_field] if timestamp_field in result_df.columns else []),
            )

            # Handle full feature names
            if full_feature_names:
                result_df = result_df.rename(
                    columns={
                        f: f"{fv.name}__{f}"
                        for f in existing_feats
                        if f in result_df.columns
                    }
                )

        # Re-attach original entity columns
        for col in original_entity_df.columns:
            if col not in result_df.columns:
                result_df[col] = original_entity_df[col]

        # Ensure event_timestamp is present
        if (
            "event_timestamp" not in result_df.columns
            and "event_timestamp" in original_entity_df.columns
        ):
            result_df["event_timestamp"] = pd.to_datetime(
                original_entity_df["event_timestamp"], utc=True, errors="coerce"
            ).dt.floor("s")

        if (
            "event_timestamp" not in result_df.columns
            and timestamp_field in result_df.columns
        ):
            result_df["event_timestamp"] = result_df[timestamp_field]

        # Drop dummy entity columns
        for dummy_col in added_dummy_columns:
            if dummy_col in result_df.columns:
                result_df = result_df.drop(columns=[dummy_col])

        # Reorder columns: entity + timestamp + features (in requested order)
        entity_columns = [
            c for c in original_entity_df.columns if c != "event_timestamp"
        ]
        # Build the list of output feature columns in the correct order
        output_feature_columns = []
        for ref in feature_refs:
            fv_name, feat = ref.split(":", 1)
            col_name = f"{fv_name}__{feat}" if full_feature_names else feat
            output_feature_columns.append(col_name)

        # Ensure all requested features are present, fill with NaN if missing
        for col in output_feature_columns:
            if col not in result_df.columns:
                result_df[col] = np.nan

        final_columns = entity_columns + ["event_timestamp"] + output_feature_columns
        result_df = result_df.reindex(columns=final_columns)

        # Convert list/numpy.ndarray columns to tuples for deduplication
        def make_hashable_for_dedup(df, columns):
            for col in columns:
                if col in df.columns:
                    if df[col].apply(lambda x: isinstance(x, (np.ndarray, list))).any():
                        df[col] = df[col].apply(
                            lambda x: tuple(x)
                            if isinstance(x, (np.ndarray, list))
                            else x
                        )
            return df

        list_columns = [
            col
            for col in final_columns
            if col in result_df.columns
            and result_df[col].apply(lambda x: isinstance(x, (np.ndarray, list))).any()
        ]
        result_df = make_hashable_for_dedup(result_df, list_columns)

        # Deduplicate
        result_df = result_df.drop_duplicates().reset_index(drop=True)

        # Convert tuple columns back to lists
        for col in list_columns:
            if col in result_df.columns:
                result_df[col] = result_df[col].apply(
                    lambda x: list(x) if isinstance(x, tuple) else x
                )

        # Return retrieval job
        storage_path = config.offline_store.storage_path
        if not storage_path:
            raise ValueError("Storage path must be set in config")

        job = RayRetrievalJob(result_df, staging_location=storage_path)
        job._full_feature_names = full_feature_names
        job._on_demand_feature_views = on_demand_feature_views
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
                return RayOfflineStore._create_filtered_dataset(
                    source_path, timestamp_field, start_date, end_date
                )
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
                return RayOfflineStore._create_filtered_dataset(
                    source_path, timestamp_field, start_date, end_date
                )
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
