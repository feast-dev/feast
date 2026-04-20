import json
import os
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Union

import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd
import pyarrow
import pyarrow.compute as pc
import pyarrow.dataset
import pyarrow.parquet as pq
import pytz

from feast.data_source import DataSource
from feast.errors import (
    FeastJoinKeysDuringMaterialization,
    SavedDatasetLocationAlreadyExists,
)
from feast.feature_logging import LoggingConfig, LoggingSource
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, FeatureView
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
    DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL,
    get_pyarrow_schema_from_batch_source,
)
from feast.infra.registry.base_registry import BaseRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.utils import (
    _get_requested_feature_views_to_features_dict,
    compute_non_entity_date_range,
)

# DaskRetrievalJob will cast string objects to string[pyarrow] from dask version 2023.7.1
# This is not the desired behavior for our use case, so we set the convert-string option to False
# See (https://github.com/dask/dask/issues/10881#issuecomment-1923327936)
dask.config.set({"dataframe.convert-string": False})


class DaskOfflineStoreConfig(FeastConfigBaseModel):
    """Offline store config for dask store"""

    type: Union[Literal["dask"], Literal["file"]] = "dask"
    """ Offline store type selector"""


class DaskRetrievalJob(RetrievalJob):
    def __init__(
        self,
        evaluation_function: Callable,
        full_feature_names: bool,
        repo_path: str,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
        metadata: Optional[RetrievalMetadata] = None,
    ):
        """Initialize a lazy historical retrieval job"""

        # The evaluation function executes a stored procedure to compute a historical retrieval.
        self.evaluation_function = evaluation_function
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = on_demand_feature_views or []
        self._metadata = metadata
        self.repo_path = repo_path

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        return self._on_demand_feature_views

    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        # Only execute the evaluation function to build the final historical retrieval dataframe at the last moment.
        df = self.evaluation_function().compute()
        df = df.reset_index(drop=True)
        return df

    def _to_arrow_internal(self, timeout: Optional[int] = None):
        # Only execute the evaluation function to build the final historical retrieval dataframe at the last moment.
        df = self.evaluation_function().compute()
        return pyarrow.Table.from_pandas(df)

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: Optional[bool] = False,
        timeout: Optional[int] = None,
    ):
        assert isinstance(storage, SavedDatasetFileStorage)

        # Check if the specified location already exists.
        if not allow_overwrite and os.path.exists(storage.file_options.uri):
            raise SavedDatasetLocationAlreadyExists(location=storage.file_options.uri)
        absolute_path = FileSource.get_uri_for_file_path(
            repo_path=self.repo_path, uri=storage.file_options.uri
        )

        filesystem, path = FileSource.create_filesystem_and_path(
            str(absolute_path),
            storage.file_options.s3_endpoint_override,
        )

        if path.endswith(".parquet"):
            pyarrow.parquet.write_table(
                self.to_arrow(), where=path, filesystem=filesystem
            )
        else:
            # otherwise assume destination is directory
            pyarrow.parquet.write_to_dataset(
                self.to_arrow(), root_path=path, filesystem=filesystem
            )

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        return self._metadata

    def supports_remote_storage_export(self) -> bool:
        return False


class DaskOfflineStore(OfflineStore):
    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Optional[Union[pd.DataFrame, dd.DataFrame, str]],
        registry: BaseRegistry,
        project: str,
        full_feature_names: bool = False,
        **kwargs,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, DaskOfflineStoreConfig)
        for fv in feature_views:
            assert isinstance(fv.batch_source, FileSource)

        non_entity_mode = entity_df is None

        if non_entity_mode:
            start_date, end_date = compute_non_entity_date_range(
                feature_views,
                start_date=kwargs.get("start_date"),
                end_date=kwargs.get("end_date"),
            )
            entity_df = pd.DataFrame(
                {DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL: [end_date]}
            )
        else:
            if not isinstance(entity_df, pd.DataFrame) and not isinstance(
                entity_df, dd.DataFrame
            ):
                raise ValueError(
                    f"Please provide an entity_df of type pd.DataFrame or dask.dataframe.DataFrame instead of type {type(entity_df)}"
                )
        entity_df_event_timestamp_col = DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL  # local modifiable copy of global variable
        if entity_df_event_timestamp_col not in entity_df.columns:
            datetime_columns = entity_df.select_dtypes(
                include=["datetime", "datetimetz"]
            ).columns
            if len(datetime_columns) == 1:
                print(
                    f"Using {datetime_columns[0]} as the event timestamp. To specify a column explicitly, please name it {DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL}."
                )
                entity_df_event_timestamp_col = datetime_columns[0]
            else:
                raise ValueError(
                    f"Please provide an entity_df with a column named {DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL} representing the time of events."
                )
        (
            feature_views_to_features,
            on_demand_feature_views_to_features,
        ) = _get_requested_feature_views_to_features_dict(
            feature_refs,
            feature_views,
            registry.list_on_demand_feature_views(config.project),
        )

        entity_df_event_timestamp_range = (
            (start_date, end_date)
            if non_entity_mode
            else _get_entity_df_event_timestamp_range(
                entity_df, entity_df_event_timestamp_col
            )
        )

        # Create lazy function that is only called from the RetrievalJob object
        def evaluate_historical_retrieval():
            # Create a copy of entity_df to prevent modifying the original
            entity_df_with_features = entity_df.copy()

            entity_df_event_timestamp_col_type = entity_df_with_features.dtypes[
                entity_df_event_timestamp_col
            ]

            # TODO: need to figure out why the value of entity_df_event_timestamp_col_type.tz is pytz.UTC
            if (
                not hasattr(entity_df_event_timestamp_col_type, "tz")
                or entity_df_event_timestamp_col_type.tz != pytz.UTC
            ):
                # Make sure all event timestamp fields are tz-aware. We default tz-naive fields to UTC
                entity_df_with_features[entity_df_event_timestamp_col] = (
                    entity_df_with_features[entity_df_event_timestamp_col].apply(
                        lambda x: (
                            x
                            if x.tzinfo is not None
                            else x.replace(tzinfo=timezone.utc)
                        )
                    )
                )

                # Convert event timestamp column to datetime and normalize time zone to UTC
                # This is necessary to avoid issues with pd.merge_asof
                if isinstance(entity_df_with_features, dd.DataFrame):
                    entity_df_with_features[entity_df_event_timestamp_col] = (
                        dd.to_datetime(
                            entity_df_with_features[entity_df_event_timestamp_col],
                            utc=True,
                        )
                    )
                else:
                    entity_df_with_features[entity_df_event_timestamp_col] = (
                        pd.to_datetime(
                            entity_df_with_features[entity_df_event_timestamp_col],
                            utc=True,
                        )
                    )

            # Sort event timestamp values
            entity_df_with_features = entity_df_with_features.sort_values(
                entity_df_event_timestamp_col
            )

            all_join_keys = []

            # Load feature view data from sources and join them incrementally
            for feature_view, features in feature_views_to_features.items():
                timestamp_field = feature_view.batch_source.timestamp_field
                created_timestamp_column = (
                    feature_view.batch_source.created_timestamp_column
                )

                # Build a list of entity columns to join on (from the right table)
                join_keys = []

                for entity_column in feature_view.entity_columns:
                    join_key = feature_view.projection.join_key_map.get(
                        entity_column.name, entity_column.name
                    )
                    join_keys.append(join_key)

                right_entity_key_columns = [
                    timestamp_field,
                    created_timestamp_column,
                ] + join_keys
                right_entity_key_columns = [c for c in right_entity_key_columns if c]

                all_join_keys = list(set(all_join_keys + join_keys))

                df_to_join = _read_datasource(
                    feature_view.batch_source, config.repo_path
                )

                df_to_join, timestamp_field = _field_mapping(
                    df_to_join,
                    feature_view,
                    features,
                    right_entity_key_columns,
                    entity_df_event_timestamp_col,
                    timestamp_field,
                    full_feature_names,
                )

                # In non-entity mode, if the synthetic entity_df lacks join keys, cross join to build a snapshot
                # of all entities as-of the requested timestamp, then rely on TTL and deduplication to select
                # the appropriate latest rows per entity.
                current_join_keys = join_keys
                if non_entity_mode:
                    current_join_keys = []

                df_to_join = _merge(
                    entity_df_with_features, df_to_join, current_join_keys
                )

                df_to_join = _normalize_timestamp(
                    df_to_join, timestamp_field, created_timestamp_column
                )

                df_to_join = _filter_ttl(
                    df_to_join,
                    feature_view,
                    entity_df_event_timestamp_col,
                    timestamp_field,
                )

                df_to_join = _drop_duplicates(
                    df_to_join,
                    all_join_keys,
                    timestamp_field,
                    created_timestamp_column,
                    entity_df_event_timestamp_col,
                )

                entity_df_with_features = _drop_columns(
                    df_to_join, features, timestamp_field, created_timestamp_column
                )

                # Ensure that we delete dataframes to free up memory
                del df_to_join

            return entity_df_with_features.persist()

        job = DaskRetrievalJob(
            evaluation_function=evaluate_historical_retrieval,
            full_feature_names=full_feature_names,
            on_demand_feature_views=OnDemandFeatureView.get_requested_odfvs(
                feature_refs, project, registry
            ),
            metadata=RetrievalMetadata(
                features=feature_refs,
                keys=list(set(entity_df.columns) - {entity_df_event_timestamp_col}),
                min_event_timestamp=entity_df_event_timestamp_range[0],
                max_event_timestamp=entity_df_event_timestamp_range[1],
            ),
            repo_path=str(config.repo_path),
        )
        return job

    @staticmethod
    def pull_latest_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, DaskOfflineStoreConfig)
        assert isinstance(data_source, FileSource)

        def evaluate_func():
            df = DaskOfflineStore.evaluate_offline_job(
                config=config,
                data_source=data_source,
                join_key_columns=join_key_columns,
                timestamp_field=timestamp_field,
                created_timestamp_column=created_timestamp_column,
                start_date=start_date,
                end_date=end_date,
            )
            ts_columns = (
                [timestamp_field, created_timestamp_column]
                if created_timestamp_column
                else [timestamp_field]
            )
            columns_to_extract = set(
                join_key_columns + feature_name_columns + ts_columns
            )
            if join_key_columns:
                df = df.drop_duplicates(
                    join_key_columns, keep="last", ignore_index=True
                )
            else:
                df[DUMMY_ENTITY_ID] = DUMMY_ENTITY_VAL
                columns_to_extract.add(DUMMY_ENTITY_ID)

            if feature_name_columns:
                df = df[list(columns_to_extract)]

            return df.persist()

        # When materializing a single feature view, we don't need full feature names. On demand transforms aren't materialized
        return DaskRetrievalJob(
            evaluation_function=evaluate_func,
            full_feature_names=False,
            repo_path=str(config.repo_path),
        )

    @staticmethod
    def evaluate_offline_job(
        config: RepoConfig,
        data_source: FileSource,
        join_key_columns: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> dd.DataFrame:
        # Create lazy function that is only called from the RetrievalJob object
        source_df = _read_datasource(data_source, config.repo_path)

        source_df = _normalize_timestamp(
            source_df, timestamp_field, created_timestamp_column
        )

        source_columns = set(source_df.columns)
        if not set(join_key_columns).issubset(source_columns):
            raise FeastJoinKeysDuringMaterialization(
                data_source.path, set(join_key_columns), source_columns
            )

        # try-catch block is added to deal with this issue https://github.com/dask/dask/issues/8939.
        # TODO(kevjumba): remove try catch when fix is merged upstream in Dask.
        try:
            if created_timestamp_column:
                source_df = source_df.sort_values(
                    by=created_timestamp_column,
                )

            source_df = source_df.sort_values(by=timestamp_field)

        except ZeroDivisionError:
            # Use 1 partition to get around case where everything in timestamp column is the same so the partition algorithm doesn't
            # try to divide by zero.
            if created_timestamp_column:
                source_df = source_df.sort_values(
                    by=created_timestamp_column, npartitions=1
                )

            source_df = source_df.sort_values(by=timestamp_field, npartitions=1)

        # TODO: The old implementation is inclusive of start_date and exclusive of end_date.
        # Which is inconsistent with other offline stores.
        if start_date or end_date:
            if start_date and end_date:
                source_df = source_df[
                    source_df[timestamp_field].between(
                        start_date, end_date, inclusive="both"
                    )
                ]
            elif start_date:
                source_df = source_df[source_df[timestamp_field] >= start_date]
            elif end_date:
                source_df = source_df[source_df[timestamp_field] <= end_date]

        source_df = source_df.persist()
        return source_df

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
        assert isinstance(config.offline_store, DaskOfflineStoreConfig)
        assert isinstance(data_source, FileSource)

        def evaluate_func():
            df = DaskOfflineStore.evaluate_offline_job(
                config=config,
                data_source=data_source,
                join_key_columns=join_key_columns,
                timestamp_field=timestamp_field,
                created_timestamp_column=created_timestamp_column,
                start_date=start_date,
                end_date=end_date,
            )
            ts_columns = (
                [timestamp_field, created_timestamp_column]
                if created_timestamp_column
                else [timestamp_field]
            )
            columns_to_extract = set(
                join_key_columns + feature_name_columns + ts_columns
            )
            if not join_key_columns:
                df[DUMMY_ENTITY_ID] = DUMMY_ENTITY_VAL
                columns_to_extract.add(DUMMY_ENTITY_ID)
            # TODO: Decides if we want to field mapping for pull_latest_from_table_or_query
            # This is default for other offline store.
            if feature_name_columns:
                df = df[list(columns_to_extract)]

            return df.persist()

        # When materializing a single feature view, we don't need full feature names. On demand transforms aren't materialized
        return DaskRetrievalJob(
            evaluation_function=evaluate_func,
            full_feature_names=False,
            repo_path=str(config.repo_path),
        )

    @staticmethod
    def write_logged_features(
        config: RepoConfig,
        data: Union[pyarrow.Table, Path],
        source: LoggingSource,
        logging_config: LoggingConfig,
        registry: BaseRegistry,
    ):
        assert isinstance(config.offline_store, DaskOfflineStoreConfig)
        destination = logging_config.destination
        assert isinstance(destination, FileLoggingDestination)

        if isinstance(data, Path):
            # Since this code will be mostly used from Go-created thread, it's better to avoid producing new threads
            data = pyarrow.parquet.read_table(data, use_threads=False, pre_buffer=False)

        if config.repo_path is not None and not Path(destination.path).is_absolute():
            absolute_path = config.repo_path / destination.path
        else:
            absolute_path = Path(destination.path)

        filesystem, path = FileSource.create_filesystem_and_path(
            str(absolute_path),
            destination.s3_endpoint_override,
        )

        pyarrow.dataset.write_dataset(
            data,
            base_dir=path,
            basename_template=f"{uuid.uuid4().hex}-{{i}}.parquet",
            partitioning=destination.partition_by,
            filesystem=filesystem,
            use_threads=False,
            format=pyarrow.dataset.ParquetFileFormat(),
            existing_data_behavior="overwrite_or_ignore",
        )

    @staticmethod
    def offline_write_batch(
        config: RepoConfig,
        feature_view: FeatureView,
        table: pyarrow.Table,
        progress: Optional[Callable[[int], Any]],
    ):
        assert isinstance(config.offline_store, DaskOfflineStoreConfig)
        assert isinstance(feature_view.batch_source, FileSource)

        pa_schema, column_names = get_pyarrow_schema_from_batch_source(
            config, feature_view.batch_source
        )
        if column_names != table.column_names:
            raise ValueError(
                f"The input pyarrow table has schema {table.schema} with the incorrect columns {table.column_names}. "
                f"The schema is expected to be {pa_schema} with the columns (in this exact order) to be {column_names}."
            )

        file_options = feature_view.batch_source.file_options

        absolute_path = FileSource.get_uri_for_file_path(
            repo_path=config.repo_path, uri=file_options.uri
        )

        filesystem, path = FileSource.create_filesystem_and_path(
            str(absolute_path), file_options.s3_endpoint_override
        )
        prev_table = pyarrow.parquet.read_table(
            path, filesystem=filesystem, memory_map=True
        )
        if table.schema != prev_table.schema:
            table = table.cast(prev_table.schema)
        new_table = pyarrow.concat_tables([table, prev_table])
        writer = pyarrow.parquet.ParquetWriter(
            path, table.schema, filesystem=filesystem
        )
        writer.write_table(new_table)
        writer.close()

    @staticmethod
    def compute_monitoring_metrics(
        config: RepoConfig,
        data_source: DataSource,
        feature_columns: List[Tuple[str, str]],
        timestamp_field: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        histogram_bins: int = 20,
        top_n: int = 10,
    ) -> List[Dict[str, Any]]:
        assert isinstance(config.offline_store, DaskOfflineStoreConfig)
        assert isinstance(data_source, FileSource)

        table = _dask_read_batch_arrow(data_source, config.repo_path)
        table = _dask_filter_arrow_by_timestamp(
            table, timestamp_field, start_date, end_date
        )

        results: List[Dict[str, Any]] = []
        for name, ftype in feature_columns:
            if name not in table.column_names:
                continue
            col = table[name]
            if ftype == "numeric":
                m = _dask_compute_numeric_metrics(col, histogram_bins)
            elif ftype == "categorical":
                m = _dask_compute_categorical_metrics(col, top_n)
            else:
                continue
            m["feature_name"] = name
            results.append(m)
        return results

    @staticmethod
    def get_monitoring_max_timestamp(
        config: RepoConfig,
        data_source: DataSource,
        timestamp_field: str,
    ) -> Optional[datetime]:
        assert isinstance(config.offline_store, DaskOfflineStoreConfig)
        assert isinstance(data_source, FileSource)

        absolute_path = FileSource.get_uri_for_file_path(
            repo_path=config.repo_path,
            uri=data_source.file_options.uri,
        )
        filesystem, path = FileSource.create_filesystem_and_path(
            str(absolute_path), data_source.file_options.s3_endpoint_override
        )
        try:
            t = pq.read_table(path, filesystem=filesystem, columns=[timestamp_field])
        except Exception:
            return None
        if t.num_rows == 0:
            return None
        arr = t[timestamp_field]
        mx = pc.max(arr)
        val = mx.as_py()
        if val is None:
            return None
        if isinstance(val, datetime):
            return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
        if isinstance(val, date):
            return datetime.combine(val, datetime.min.time(), tzinfo=timezone.utc)
        return None

    @staticmethod
    def ensure_monitoring_tables(config: RepoConfig) -> None:
        assert isinstance(config.offline_store, DaskOfflineStoreConfig)
        base = os.path.join(_dask_monitoring_base(config), _DASK_MON_DIR)
        os.makedirs(base, exist_ok=True)

        tables = [
            (_DASK_FEATURE_METRICS_FILE, _DASK_MON_FEATURE_COLUMNS),
            (_DASK_VIEW_METRICS_FILE, _DASK_MON_VIEW_COLUMNS),
            (_DASK_SERVICE_METRICS_FILE, _DASK_MON_SERVICE_COLUMNS),
        ]
        for fname, columns in tables:
            fpath = _dask_monitoring_path(config, fname)
            if not os.path.isfile(fpath):
                os.makedirs(os.path.dirname(fpath), exist_ok=True)
                pd.DataFrame(columns=columns).to_parquet(fpath, index=False)

    @staticmethod
    def save_monitoring_metrics(
        config: RepoConfig,
        metric_type: str,
        metrics: List[Dict[str, Any]],
    ) -> None:
        if not metrics:
            return
        assert isinstance(config.offline_store, DaskOfflineStoreConfig)

        fname, columns, pk = _dask_mon_table_meta(metric_type)
        path = _dask_monitoring_path(config, fname)
        _dask_parquet_upsert(path, columns, pk, metrics)

    @staticmethod
    def query_monitoring_metrics(
        config: RepoConfig,
        project: str,
        metric_type: str,
        filters: Optional[Dict[str, Any]] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        assert isinstance(config.offline_store, DaskOfflineStoreConfig)

        fname, columns, _ = _dask_mon_table_meta(metric_type)
        path = _dask_monitoring_path(config, fname)
        return _dask_parquet_query(
            path, columns, project, filters, start_date, end_date
        )

    @staticmethod
    def clear_monitoring_baseline(
        config: RepoConfig,
        project: str,
        feature_view_name: Optional[str] = None,
        feature_name: Optional[str] = None,
        data_source_type: Optional[str] = None,
    ) -> None:
        assert isinstance(config.offline_store, DaskOfflineStoreConfig)

        path = _dask_monitoring_path(config, _DASK_FEATURE_METRICS_FILE)
        tab = _dask_read_parquet_if_exists(path)
        if tab is None or tab.num_rows == 0:
            return

        df = tab.to_pandas()
        mask = df["project_id"] == project
        if feature_view_name is not None:
            mask = mask & (df["feature_view_name"] == feature_view_name)
        if feature_name is not None:
            mask = mask & (df["feature_name"] == feature_name)
        if data_source_type is not None:
            mask = mask & (df["data_source_type"] == data_source_type)
        mask = mask & (df["is_baseline"].isin([True, 1]))
        df.loc[mask, "is_baseline"] = False
        pq.write_table(pyarrow.Table.from_pandas(df, preserve_index=False), path)


_DASK_MON_DIR = "feast_monitoring"
_DASK_FEATURE_METRICS_FILE = "feature_metrics.parquet"
_DASK_VIEW_METRICS_FILE = "feature_view_metrics.parquet"
_DASK_SERVICE_METRICS_FILE = "feature_service_metrics.parquet"

_DASK_MON_FEATURE_COLUMNS = [
    "project_id",
    "feature_view_name",
    "feature_name",
    "metric_date",
    "granularity",
    "data_source_type",
    "computed_at",
    "is_baseline",
    "feature_type",
    "row_count",
    "null_count",
    "null_rate",
    "mean",
    "stddev",
    "min_val",
    "max_val",
    "p50",
    "p75",
    "p90",
    "p95",
    "p99",
    "histogram",
]
_DASK_MON_FEATURE_PK = [
    "project_id",
    "feature_view_name",
    "feature_name",
    "metric_date",
    "granularity",
    "data_source_type",
]

_DASK_MON_VIEW_COLUMNS = [
    "project_id",
    "feature_view_name",
    "metric_date",
    "granularity",
    "data_source_type",
    "computed_at",
    "is_baseline",
    "total_row_count",
    "total_features",
    "features_with_nulls",
    "avg_null_rate",
    "max_null_rate",
]
_DASK_MON_VIEW_PK = [
    "project_id",
    "feature_view_name",
    "metric_date",
    "granularity",
    "data_source_type",
]

_DASK_MON_SERVICE_COLUMNS = [
    "project_id",
    "feature_service_name",
    "metric_date",
    "granularity",
    "data_source_type",
    "computed_at",
    "is_baseline",
    "total_feature_views",
    "total_features",
    "avg_null_rate",
    "max_null_rate",
]
_DASK_MON_SERVICE_PK = [
    "project_id",
    "feature_service_name",
    "metric_date",
    "granularity",
    "data_source_type",
]


def _dask_monitoring_base(config: RepoConfig) -> str:
    base = config.repo_path
    return str(base) if base else "."


def _dask_monitoring_path(config: RepoConfig, filename: str) -> str:
    return os.path.join(_dask_monitoring_base(config), _DASK_MON_DIR, filename)


def _dask_mon_table_meta(metric_type: str):
    if metric_type == "feature":
        return (
            _DASK_FEATURE_METRICS_FILE,
            _DASK_MON_FEATURE_COLUMNS,
            _DASK_MON_FEATURE_PK,
        )
    if metric_type == "feature_view":
        return _DASK_VIEW_METRICS_FILE, _DASK_MON_VIEW_COLUMNS, _DASK_MON_VIEW_PK
    if metric_type == "feature_service":
        return (
            _DASK_SERVICE_METRICS_FILE,
            _DASK_MON_SERVICE_COLUMNS,
            _DASK_MON_SERVICE_PK,
        )
    raise ValueError(f"Unknown metric_type '{metric_type}'")


def _dask_read_parquet_if_exists(path: str) -> Optional[pyarrow.Table]:
    if not os.path.isfile(path):
        return None
    return pq.read_table(path)


def _dask_read_batch_arrow(
    data_source: FileSource, repo_path: Optional[Path]
) -> pyarrow.Table:
    absolute_path = FileSource.get_uri_for_file_path(
        repo_path=repo_path,
        uri=data_source.file_options.uri,
    )
    filesystem, path = FileSource.create_filesystem_and_path(
        str(absolute_path), data_source.file_options.s3_endpoint_override
    )
    return pq.read_table(path, filesystem=filesystem)


def _dask_filter_arrow_by_timestamp(
    table: pyarrow.Table,
    timestamp_field: str,
    start_date: Optional[datetime],
    end_date: Optional[datetime],
) -> pyarrow.Table:
    if start_date is None and end_date is None:
        return table
    arr = table[timestamp_field]
    mask = None
    if start_date is not None:
        mask = pc.greater_equal(arr, pyarrow.scalar(start_date))
    if end_date is not None:
        m2 = pc.less_equal(arr, pyarrow.scalar(end_date))
        mask = m2 if mask is None else pc.and_(mask, m2)
    return table.filter(mask)


def _dask_compute_numeric_metrics(
    column: pyarrow.ChunkedArray, histogram_bins: int
) -> Dict[str, Any]:
    total = column.length
    null_count = column.null_count
    result: Dict[str, Any] = {
        "feature_type": "numeric",
        "row_count": total,
        "null_count": null_count,
        "null_rate": null_count / total if total > 0 else 0.0,
        "mean": None,
        "stddev": None,
        "min_val": None,
        "max_val": None,
        "p50": None,
        "p75": None,
        "p90": None,
        "p95": None,
        "p99": None,
        "histogram": None,
    }

    valid = pc.drop_null(column)
    if len(valid) == 0:
        return result

    float_array = pc.cast(valid, pyarrow.float64())
    result["mean"] = pc.mean(float_array).as_py()
    result["stddev"] = pc.stddev(float_array, ddof=1).as_py()

    min_max = pc.min_max(float_array)
    result["min_val"] = min_max["min"].as_py()
    result["max_val"] = min_max["max"].as_py()

    quantiles = pc.quantile(float_array, q=[0.50, 0.75, 0.90, 0.95, 0.99])
    q_values = quantiles.to_pylist()
    result["p50"] = q_values[0]
    result["p75"] = q_values[1]
    result["p90"] = q_values[2]
    result["p95"] = q_values[3]
    result["p99"] = q_values[4]

    np_array = float_array.to_numpy()
    counts, bin_edges = np.histogram(np_array, bins=histogram_bins)
    result["histogram"] = {
        "bins": bin_edges.tolist(),
        "counts": counts.tolist(),
        "bin_width": float(bin_edges[1] - bin_edges[0]) if len(bin_edges) > 1 else 0,
    }

    return result


def _dask_compute_categorical_metrics(
    column: pyarrow.ChunkedArray, top_n: int
) -> Dict[str, Any]:
    total = column.length
    null_count = column.null_count
    result: Dict[str, Any] = {
        "feature_type": "categorical",
        "row_count": total,
        "null_count": null_count,
        "null_rate": null_count / total if total > 0 else 0.0,
        "mean": None,
        "stddev": None,
        "min_val": None,
        "max_val": None,
        "p50": None,
        "p75": None,
        "p90": None,
        "p95": None,
        "p99": None,
        "histogram": None,
    }

    valid = pc.drop_null(column)
    if len(valid) == 0:
        return result

    value_counts = pc.value_counts(valid)
    entries = [
        {"value": vc["values"].as_py(), "count": vc["counts"].as_py()}
        for vc in value_counts
    ]
    entries.sort(key=lambda x: x["count"], reverse=True)

    unique_count = len(entries)
    top_entries = entries[:top_n]
    other_count = sum(e["count"] for e in entries[top_n:])

    result["histogram"] = {
        "values": top_entries,
        "other_count": other_count,
        "unique_count": unique_count,
    }

    return result


def _dask_parquet_upsert(
    path: str,
    columns: List[str],
    pk_cols: List[str],
    new_rows: List[Dict[str, Any]],
) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)

    prepared: List[Dict[str, Any]] = []
    for row in new_rows:
        r = dict(row)
        if (
            "histogram" in r
            and r["histogram"] is not None
            and not isinstance(r["histogram"], str)
        ):
            r["histogram"] = json.dumps(r["histogram"])
        prepared.append(r)

    new_df = pd.DataFrame(prepared, columns=columns)
    existing = _dask_read_parquet_if_exists(path)
    if existing is not None:
        old_df = existing.to_pandas()
        combined = pd.concat([old_df, new_df], ignore_index=True)
    else:
        combined = new_df

    combined = combined.drop_duplicates(subset=pk_cols, keep="last")
    table = pyarrow.Table.from_pandas(combined, preserve_index=False)
    pq.write_table(table, path)


def _dask_parquet_query(
    path: str,
    columns: List[str],
    project: str,
    filters: Optional[Dict[str, Any]],
    start_date: Optional[date],
    end_date: Optional[date],
) -> List[Dict[str, Any]]:
    tab = _dask_read_parquet_if_exists(path)
    if tab is None or tab.num_rows == 0:
        return []

    df = tab.to_pandas()
    df = df[df["project_id"] == project]
    if filters:
        for key, value in filters.items():
            if value is not None:
                df = df[df[key] == value]
    if start_date is not None:
        df = df[df["metric_date"] >= start_date]
    if end_date is not None:
        df = df[df["metric_date"] <= end_date]
    df = df.sort_values("metric_date", ascending=True)

    results = []
    for _, row in df.iterrows():
        record = {c: row.get(c) for c in columns}
        if "histogram" in record and isinstance(record["histogram"], str):
            try:
                record["histogram"] = json.loads(record["histogram"])
            except json.JSONDecodeError:
                pass
        if "metric_date" in record and hasattr(record["metric_date"], "isoformat"):
            record["metric_date"] = record["metric_date"].isoformat()
        if "computed_at" in record and hasattr(record["computed_at"], "isoformat"):
            record["computed_at"] = record["computed_at"].isoformat()
        results.append(record)

    return results


def _get_entity_df_event_timestamp_range(
    entity_df: Union[pd.DataFrame, str],
    entity_df_event_timestamp_col: str,
) -> Tuple[datetime, datetime]:
    if not isinstance(entity_df, pd.DataFrame):
        raise ValueError(
            f"Please provide an entity_df of type {type(pd.DataFrame)} instead of type {type(entity_df)}"
        )

    entity_df_event_timestamp = entity_df.loc[
        :, entity_df_event_timestamp_col
    ].infer_objects()
    if pd.api.types.is_string_dtype(entity_df_event_timestamp):
        entity_df_event_timestamp = pd.to_datetime(entity_df_event_timestamp, utc=True)

    return (
        entity_df_event_timestamp.min().to_pydatetime(),
        entity_df_event_timestamp.max().to_pydatetime(),
    )


def _read_datasource(data_source, repo_path) -> dd.DataFrame:
    storage_options = (
        {
            "client_kwargs": {
                "endpoint_url": data_source.file_options.s3_endpoint_override
            }
        }
        if data_source.file_options.s3_endpoint_override
        else None
    )

    path = FileSource.get_uri_for_file_path(
        repo_path=repo_path,
        uri=data_source.file_options.uri,
    )

    return dd.read_parquet(
        path,
        storage_options=storage_options,
    )


def _run_dask_field_mapping(
    table: dd.DataFrame,
    field_mapping: Dict[str, str],
):
    if field_mapping:
        # run field mapping in the forward direction
        table = table.rename(columns=field_mapping)
        table = table.persist()

    return table


def _field_mapping(
    df_to_join: dd.DataFrame,
    feature_view: FeatureView,
    features: List[str],
    right_entity_key_columns: List[str],
    entity_df_event_timestamp_col: str,
    timestamp_field: str,
    full_feature_names: bool,
) -> Tuple[dd.DataFrame, str]:
    # Rename columns by the field mapping dictionary if it exists
    if (
        feature_view.batch_source is not None
        and feature_view.batch_source.field_mapping
    ):
        df_to_join = _run_dask_field_mapping(
            df_to_join, feature_view.batch_source.field_mapping
        )
    # Rename entity columns by the join_key_map dictionary if it exists
    if feature_view.projection.join_key_map:
        df_to_join = _run_dask_field_mapping(
            df_to_join, feature_view.projection.join_key_map
        )

    # Build a list of all the features we should select from this source
    feature_names = []
    columns_map = {}
    for feature in features:
        # Modify the separator for feature refs in column names to double underscore. We are using
        # double underscore as separator for consistency with other databases like BigQuery,
        # where there are very few characters available for use as separators
        if full_feature_names:
            formatted_feature_name = (
                f"{feature_view.projection.name_to_use()}__{feature}"
            )
        else:
            formatted_feature_name = feature
        # Add the feature name to the list of columns
        feature_names.append(formatted_feature_name)
        columns_map[feature] = formatted_feature_name

    # Ensure that the source dataframe feature column includes the feature view name as a prefix
    df_to_join = _run_dask_field_mapping(df_to_join, columns_map)

    # Select only the columns we need to join from the feature dataframe
    df_to_join = df_to_join[right_entity_key_columns + feature_names]
    df_to_join = df_to_join.persist()

    # Make sure to not have duplicated columns
    if entity_df_event_timestamp_col == timestamp_field:
        df_to_join = _run_dask_field_mapping(
            df_to_join,
            {timestamp_field: f"__{timestamp_field}"},
        )
        timestamp_field = f"__{timestamp_field}"

    return df_to_join.persist(), timestamp_field


def _merge(
    entity_df_with_features: dd.DataFrame,
    df_to_join: dd.DataFrame,
    join_keys: List[str],
) -> dd.DataFrame:
    # tmp join keys needed for cross join with null join table view
    tmp_join_keys = []
    if not join_keys:
        entity_df_with_features["__tmp"] = 1
        df_to_join["__tmp"] = 1
        tmp_join_keys = ["__tmp"]

    # Get only data with requested entities
    df_to_join = dd.merge(
        entity_df_with_features,
        df_to_join,
        left_on=join_keys or tmp_join_keys,
        right_on=join_keys or tmp_join_keys,
        suffixes=("", "__"),
        how="left",
    )

    if tmp_join_keys:
        df_to_join = df_to_join.drop(tmp_join_keys, axis=1).persist()
    else:
        df_to_join = df_to_join.persist()

    return df_to_join


def _normalize_timestamp(
    df_to_join: dd.DataFrame,
    timestamp_field: str,
    created_timestamp_column: Optional[str] = None,
) -> dd.DataFrame:
    df_to_join_types = df_to_join.dtypes
    timestamp_field_type = df_to_join_types[timestamp_field]

    if created_timestamp_column:
        created_timestamp_column_type = df_to_join_types[created_timestamp_column]

    # TODO: need to figure out why the value of timestamp_field_type.tz is pytz.UTC
    if not hasattr(timestamp_field_type, "tz") or timestamp_field_type.tz != pytz.UTC:
        # if you are querying for the event timestamp field, we have to deduplicate
        if len(df_to_join[timestamp_field].shape) > 1:
            df_to_join, dups = _df_column_uniquify(df_to_join)
            df_to_join = df_to_join.drop(columns=dups)

        # Make sure all timestamp fields are tz-aware. We default tz-naive fields to UTC
        df_to_join[timestamp_field] = df_to_join[timestamp_field].apply(
            lambda x: x if x.tzinfo else x.replace(tzinfo=timezone.utc),
            meta=(timestamp_field, "datetime64[ns, UTC]"),
        )

    # TODO: need to figure out why the value of created_timestamp_column_type.tz is pytz.UTC
    if created_timestamp_column and (
        not hasattr(created_timestamp_column_type, "tz")
        or created_timestamp_column_type.tz != pytz.UTC
    ):
        if len(df_to_join[created_timestamp_column].shape) > 1:
            # if you are querying for the created timestamp field, we have to deduplicate
            df_to_join, dups = _df_column_uniquify(df_to_join)
            df_to_join = df_to_join.drop(columns=dups)

        df_to_join[created_timestamp_column] = df_to_join[
            created_timestamp_column
        ].apply(
            lambda x: x if x.tzinfo else x.replace(tzinfo=timezone.utc),
            meta=(timestamp_field, "datetime64[ns, UTC]"),
        )

    return df_to_join.persist()


def _filter_ttl(
    df_to_join: dd.DataFrame,
    feature_view: FeatureView,
    entity_df_event_timestamp_col: str,
    timestamp_field: str,
) -> dd.DataFrame:
    # Filter rows by defined timestamp tolerance
    if feature_view.ttl and feature_view.ttl.total_seconds() != 0:
        df_to_join = df_to_join[
            # do not drop entity rows if one of the sources returns NaNs
            df_to_join[timestamp_field].isna()
            | (
                (
                    df_to_join[timestamp_field]
                    >= df_to_join[entity_df_event_timestamp_col] - feature_view.ttl
                )
                & (
                    df_to_join[timestamp_field]
                    <= df_to_join[entity_df_event_timestamp_col]
                )
            )
        ]

        df_to_join = df_to_join.persist()
    else:
        df_to_join = df_to_join[
            # do not drop entity rows if one of the sources returns NaNs
            df_to_join[timestamp_field].isna()
            | (df_to_join[timestamp_field] <= df_to_join[entity_df_event_timestamp_col])
        ]

        df_to_join = df_to_join.persist()

    return df_to_join


def _drop_duplicates(
    df_to_join: dd.DataFrame,
    all_join_keys: List[str],
    timestamp_field: str,
    created_timestamp_column: str,
    entity_df_event_timestamp_col: str,
) -> dd.DataFrame:
    column_order = df_to_join.columns

    # try-catch block is added to deal with this issue https://github.com/dask/dask/issues/8939.
    # TODO(kevjumba): remove try catch when fix is merged upstream in Dask.
    try:
        if created_timestamp_column:
            df_to_join = df_to_join.sort_values(
                by=created_timestamp_column, na_position="first"
            )
            df_to_join = df_to_join.persist()

        df_to_join = df_to_join.sort_values(by=timestamp_field, na_position="first")
        df_to_join = df_to_join.persist()

    except ZeroDivisionError:
        # Use 1 partition to get around case where everything in timestamp column is the same so the partition algorithm doesn't
        # try to divide by zero.
        if created_timestamp_column:
            df_to_join = df_to_join[column_order].sort_values(
                by=created_timestamp_column, na_position="first", npartitions=1
            )
            df_to_join = df_to_join.persist()

        df_to_join = df_to_join[column_order].sort_values(
            by=timestamp_field, na_position="first", npartitions=1
        )
        df_to_join = df_to_join.persist()

    df_to_join = df_to_join.drop_duplicates(
        all_join_keys + [entity_df_event_timestamp_col],
        keep="last",
        ignore_index=True,
    )

    return df_to_join.persist()


def _drop_columns(
    df_to_join: dd.DataFrame,
    features: List[str],
    timestamp_field: str,
    created_timestamp_column: str,
) -> dd.DataFrame:
    entity_df_with_features = df_to_join
    timestamp_columns = [
        timestamp_field,
        created_timestamp_column,
    ]
    for column in timestamp_columns:
        if column and column not in features:
            entity_df_with_features = entity_df_with_features.drop(
                [column], axis=1
            ).persist()

    return entity_df_with_features


def _df_column_uniquify(df: dd.DataFrame) -> Tuple[dd.DataFrame, List[str]]:
    df_columns = df.columns
    new_columns = []
    duplicate_cols = []
    for item in df_columns:
        counter = 0
        newitem = item
        while newitem in new_columns:
            counter += 1
            newitem = "{}_{}".format(item, counter)
            if counter > 0:
                duplicate_cols.append(newitem)
        new_columns.append(newitem)
    df.columns = new_columns
    return df, duplicate_cols
