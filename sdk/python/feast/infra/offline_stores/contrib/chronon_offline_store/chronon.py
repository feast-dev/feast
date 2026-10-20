import json
import os
import posixpath
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Optional, Union

import pandas as pd
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
from pydantic import StrictStr

from feast import utils
from feast.data_format import ParquetFormat
from feast.data_source import DataSource
from feast.errors import SavedDatasetLocationAlreadyExists
from feast.feature_logging import LoggingConfig, LoggingSource
from feast.feature_view import FeatureView
from feast.infra.offline_stores.file_source import FileSource, SavedDatasetFileStorage
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.offline_stores.offline_utils import (
    DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL,
    assert_expected_columns_in_entity_df,
    get_expected_join_keys,
    infer_event_timestamp_from_entity_df,
)
from feast.infra.registry.base_registry import BaseRegistry
from feast.monitoring.monitoring_utils import (
    MONITORING_DIR,
    MONITORING_PARQUET_FILES,
    MONITORING_TIMESTAMP_FIELDS,
    monitoring_parquet_meta,
    normalize_monitoring_row,
)
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage


class ChrononOfflineStoreConfig(FeastConfigBaseModel):
    type: Literal["chronon"] = "chronon"
    path: Optional[StrictStr] = None


class ChrononRetrievalJob(RetrievalJob):
    def __init__(
        self,
        evaluation_function: Callable[[], pd.DataFrame],
        full_feature_names: bool,
        metadata: Optional[RetrievalMetadata] = None,
        repo_path: str = ".",
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
    ) -> None:
        self.evaluation_function = evaluation_function
        self._full_feature_names = full_feature_names
        self._metadata = metadata
        self.repo_path = repo_path
        self._on_demand_feature_views = on_demand_feature_views or []

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        return self._on_demand_feature_views

    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        return self.evaluation_function().reset_index(drop=True)

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pa.Table:
        return pa.Table.from_pandas(self._to_df_internal(timeout=timeout))

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: bool = False,
        timeout: Optional[int] = None,
    ) -> None:
        if not isinstance(storage, SavedDatasetFileStorage) or not isinstance(
            storage.file_options.file_format, ParquetFormat
        ):
            raise ValueError("Chronon retrieval results require Parquet file storage.")
        uri = FileSource.get_uri_for_file_path(
            repo_path=Path(self.repo_path), uri=storage.file_options.uri
        )
        filesystem, path = FileSource.create_filesystem_and_path(
            str(uri), storage.file_options.s3_endpoint_override
        )
        if filesystem is None:
            filesystem, path = pafs.FileSystem.from_uri(str(uri))
        info = filesystem.get_file_info(path)
        if info.type != pafs.FileType.NotFound and not allow_overwrite:
            raise SavedDatasetLocationAlreadyExists(location=str(uri))
        # Evaluate before replacing an existing result, including ODFV transforms.
        table = self.to_arrow(timeout=timeout)
        if path.endswith(".parquet"):
            filesystem.create_dir(posixpath.dirname(path), recursive=True)
            pq.write_table(table, path, filesystem=filesystem)
        else:
            if info.type == pafs.FileType.Directory:
                filesystem.delete_dir_contents(path)
            filesystem.create_dir(path, recursive=True)
            pq.write_table(
                table, posixpath.join(path, "part-0.parquet"), filesystem=filesystem
            )

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        return self._metadata


def _get_chronon_source(feature_view: FeatureView):
    source = feature_view.batch_source
    if source is None or source.__class__.__name__ != "ChrononSource":
        raise ValueError(
            f"Feature view '{feature_view.name}' is not backed by a ChrononSource."
        )
    return source


def _load_materialized_dataframe(
    config: RepoConfig, feature_view: FeatureView, columns: List[str]
) -> pd.DataFrame:
    source = _get_chronon_source(feature_view)
    resolved_path = FileSource.get_uri_for_file_path(
        repo_path=config.repo_path, uri=source.materialization_path
    )
    # Projection uses physical Parquet names; mapping happens after the read.
    reverse_mapping = {value: key for key, value in source.field_mapping.items()}
    physical_columns = list(
        dict.fromkeys(reverse_mapping.get(col, col) for col in columns)
    )
    dataframe = pd.read_parquet(resolved_path, columns=physical_columns)
    if source.field_mapping:
        dataframe = dataframe.rename(columns=source.field_mapping)
    return utils.make_df_tzaware(dataframe)


def _output_feature_name(
    feature_view: FeatureView, feature_name: str, full_feature_names: bool
) -> str:
    if full_feature_names:
        return f"{feature_view.projection.name_to_use()}__{feature_name}"
    return feature_name


def _get_required_timestamp_field(feature_view: FeatureView) -> str:
    source = _get_chronon_source(feature_view)
    timestamp_field = source.timestamp_field
    if not timestamp_field:
        raise ValueError(
            f"ChrononSource for feature view '{feature_view.name}' must define "
            "`timestamp_field` for historical retrieval."
        )
    return timestamp_field


class ChrononOfflineStore(OfflineStore):
    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Optional[Union[pd.DataFrame, str]],
        registry: BaseRegistry,
        project: str,
        full_feature_names: bool = False,
        **kwargs,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, ChrononOfflineStoreConfig)

        requested_features, requested_odfvs = (
            utils._get_requested_feature_views_to_features_dict(
                feature_refs,
                feature_views,
                registry.list_on_demand_feature_views(project),
            )
        )
        if entity_df is None and any(
            odfv.get_request_data_schema() for odfv in requested_odfvs
        ):
            raise ValueError(
                "On-demand request data must be supplied in a pandas entity_df."
            )
        if entity_df is not None and not isinstance(entity_df, pd.DataFrame):
            raise ValueError(
                "ChrononOfflineStore currently supports pandas entity_df inputs or "
                "time-range retrieval without entity_df."
            )

        metadata = RetrievalMetadata(
            features=feature_refs,
            keys=(
                list(entity_df.columns)
                if isinstance(entity_df, pd.DataFrame)
                else [DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL]
            ),
            min_event_timestamp=kwargs.get("start_date"),
            max_event_timestamp=kwargs.get("end_date"),
        )

        if entity_df is None:
            if len(requested_features) != 1:
                raise ValueError(
                    "ChrononOfflineStore non-entity retrieval currently supports "
                    "exactly one feature view at a time."
                )

            def evaluate_non_entity_retrieval() -> pd.DataFrame:
                feature_view, selected_features = next(iter(requested_features.items()))
                timestamp_col = _get_required_timestamp_field(feature_view)
                dataframe = _load_materialized_dataframe(
                    config,
                    feature_view,
                    [
                        *(entity.name for entity in feature_view.entity_columns),
                        timestamp_col,
                        *selected_features,
                    ],
                )
                start_date = kwargs.get("start_date")
                end_date = kwargs.get("end_date")
                if start_date is not None:
                    dataframe = dataframe[
                        dataframe[timestamp_col] >= utils.make_tzaware(start_date)
                    ]
                if end_date is not None:
                    dataframe = dataframe[
                        dataframe[timestamp_col] <= utils.make_tzaware(end_date)
                    ]
                keep_columns = [
                    *(entity.name for entity in feature_view.entity_columns),
                    timestamp_col,
                    *(selected_features),
                ]
                dataframe = dataframe.loc[
                    :, [c for c in keep_columns if c in dataframe.columns]
                ]
                rename_map = {
                    feature: _output_feature_name(
                        feature_view, feature, full_feature_names
                    )
                    for feature in selected_features
                }
                return dataframe.rename(columns=rename_map)

            return ChrononRetrievalJob(
                evaluation_function=evaluate_non_entity_retrieval,
                full_feature_names=full_feature_names,
                metadata=metadata,
                repo_path=str(config.repo_path),
                on_demand_feature_views=list(requested_odfvs),
            )

        entity_df = utils.make_df_tzaware(entity_df)
        entity_event_timestamp_col = infer_event_timestamp_from_entity_df(
            entity_df.dtypes.to_dict()
        )
        expected_join_keys = get_expected_join_keys(project, feature_views, registry)
        assert_expected_columns_in_entity_df(
            entity_df.dtypes.to_dict(),
            expected_join_keys,
            entity_event_timestamp_col,
        )

        def evaluate_historical_retrieval() -> pd.DataFrame:
            result = entity_df.copy()
            for feature_view, selected_features in requested_features.items():
                source = _get_chronon_source(feature_view)
                timestamp_col = _get_required_timestamp_field(feature_view)
                created_col = source.created_timestamp_column
                left_keys = []
                right_keys = []
                for entity_column in feature_view.entity_columns:
                    left_keys.append(
                        feature_view.projection.join_key_map.get(
                            entity_column.name, entity_column.name
                        )
                    )
                    right_keys.append(entity_column.name)

                source_df = _load_materialized_dataframe(
                    config,
                    feature_view,
                    [
                        *right_keys,
                        timestamp_col,
                        *([created_col] if created_col else []),
                        *selected_features,
                    ],
                )
                sort_columns = right_keys + [timestamp_col]
                if created_col and created_col in source_df.columns:
                    sort_columns.append(created_col)
                source_df = source_df.sort_values(sort_columns).drop_duplicates(
                    subset=right_keys + [timestamp_col], keep="last"
                )

                merge_columns = right_keys + [timestamp_col] + list(selected_features)
                merge_frame = source_df.loc[
                    :, [col for col in merge_columns if col in source_df.columns]
                ].copy()
                rename_map = dict(zip(right_keys, left_keys))
                rename_map.update(
                    {
                        feature: _output_feature_name(
                            feature_view, feature, full_feature_names
                        )
                        for feature in selected_features
                    }
                )
                merge_frame = merge_frame.rename(columns=rename_map)
                row_id_col = "__chronon_row_id"
                while row_id_col in result.columns or row_id_col in merge_frame.columns:
                    row_id_col += "_"
                result = result.assign(**{row_id_col: range(len(result))})
                if result.empty:
                    for feature in selected_features:
                        output_column = _output_feature_name(
                            feature_view, feature, full_feature_names
                        )
                        dtype = (
                            source_df[feature].dtype
                            if feature in source_df.columns
                            else "object"
                        )
                        result[output_column] = pd.Series(dtype=dtype)
                    result = result.drop(columns=[row_id_col])
                    continue
                right_frames = (
                    merge_frame.groupby(
                        left_keys, sort=False, dropna=False, observed=True
                    )
                    if left_keys
                    else [((), merge_frame)]
                )
                right_groups = {
                    key if isinstance(key, tuple) else (key,): group.drop(
                        columns=left_keys, errors="ignore"
                    )
                    for key, group in right_frames
                }
                joined_frames = []
                left_frames = (
                    result.groupby(left_keys, sort=False, dropna=False, observed=True)
                    if left_keys
                    else [((), result)]
                )
                for key, left_group in left_frames:
                    group_key = key if isinstance(key, tuple) else (key,)
                    right_group = right_groups.get(group_key, merge_frame.iloc[0:0])
                    right_group = right_group.drop(columns=left_keys, errors="ignore")
                    joined = pd.merge_asof(
                        left_group.sort_values(entity_event_timestamp_col),
                        right_group.sort_values(timestamp_col),
                        left_on=entity_event_timestamp_col,
                        right_on=timestamp_col,
                        direction="backward",
                        tolerance=feature_view.ttl or None,
                    )
                    joined_frames.append(joined)

                result = (
                    pd.concat(joined_frames, ignore_index=True)
                    .sort_values(row_id_col)
                    .drop(columns=[row_id_col])
                    .reset_index(drop=True)
                )
                if (
                    timestamp_col != entity_event_timestamp_col
                    and timestamp_col in result.columns
                ):
                    result = result.drop(columns=[timestamp_col])
            return result

        return ChrononRetrievalJob(
            evaluation_function=evaluate_historical_retrieval,
            full_feature_names=full_feature_names,
            metadata=metadata,
            repo_path=str(config.repo_path),
            on_demand_feature_views=list(requested_odfvs),
        )

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
        if data_source.__class__.__name__ != "ChrononSource":
            raise ValueError("ChrononOfflineStore only supports ChrononSource inputs.")

        def evaluate_pull_latest() -> pd.DataFrame:
            feature_view = FeatureView(
                name=data_source.name,
                entities=[],
                schema=[],
                source=data_source,
            )
            dataframe = _load_materialized_dataframe(
                config,
                feature_view,
                [
                    *join_key_columns,
                    timestamp_field,
                    *([created_timestamp_column] if created_timestamp_column else []),
                    *feature_name_columns,
                ],
            )
            dataframe = dataframe[
                (dataframe[timestamp_field] >= utils.make_tzaware(start_date))
                & (dataframe[timestamp_field] <= utils.make_tzaware(end_date))
            ]
            sort_columns = [timestamp_field]
            if (
                created_timestamp_column
                and created_timestamp_column in dataframe.columns
            ):
                sort_columns.append(created_timestamp_column)
            dataframe = dataframe.sort_values(sort_columns).drop_duplicates(
                subset=join_key_columns, keep="last"
            )
            return dataframe[
                join_key_columns + feature_name_columns + [timestamp_field]
            ]

        return ChrononRetrievalJob(
            evaluation_function=evaluate_pull_latest,
            full_feature_names=False,
            repo_path=str(config.repo_path),
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
        if data_source.__class__.__name__ != "ChrononSource":
            raise ValueError("ChrononOfflineStore only supports ChrononSource inputs.")

        def evaluate_pull_all() -> pd.DataFrame:
            feature_view = FeatureView(
                name=data_source.name,
                entities=[],
                schema=[],
                source=data_source,
            )
            dataframe = _load_materialized_dataframe(
                config,
                feature_view,
                [
                    *join_key_columns,
                    timestamp_field,
                    *([created_timestamp_column] if created_timestamp_column else []),
                    *feature_name_columns,
                ],
            )
            if start_date is not None:
                dataframe = dataframe[
                    dataframe[timestamp_field] >= utils.make_tzaware(start_date)
                ]
            if end_date is not None:
                dataframe = dataframe[
                    dataframe[timestamp_field] <= utils.make_tzaware(end_date)
                ]
            keep_columns = join_key_columns + feature_name_columns + [timestamp_field]
            return dataframe.loc[:, [c for c in keep_columns if c in dataframe.columns]]

        return ChrononRetrievalJob(
            evaluation_function=evaluate_pull_all,
            full_feature_names=False,
            repo_path=str(config.repo_path),
        )

    @staticmethod
    def write_logged_features(
        config: RepoConfig,
        data: Union[pa.Table, Path],
        source: LoggingSource,
        logging_config: LoggingConfig,
        registry: BaseRegistry,
    ):
        raise NotImplementedError(
            "ChrononOfflineStore does not support Feast-managed logged feature writes."
        )

    @staticmethod
    def offline_write_batch(
        config: RepoConfig,
        feature_view: FeatureView,
        table: pa.Table,
        progress: Optional[Callable[[int], Any]],
    ):
        raise NotImplementedError(
            "ChrononOfflineStore does not support Feast-managed offline writes."
        )

    @staticmethod
    def ensure_monitoring_tables(config: RepoConfig) -> None:
        assert isinstance(config.offline_store, ChrononOfflineStoreConfig)
        base = os.path.join(_chronon_monitoring_base(config), MONITORING_DIR)
        os.makedirs(base, exist_ok=True)

        tables = [
            monitoring_parquet_meta(t)
            for t in ("feature", "feature_view", "feature_service", "job")
        ]
        for fname, columns, _ in tables:
            fpath = _chronon_monitoring_path(config, fname)
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
        assert isinstance(config.offline_store, ChrononOfflineStoreConfig)

        fname, columns, pk = _chronon_mon_table_meta(metric_type)
        path = _chronon_monitoring_path(config, fname)
        _chronon_parquet_upsert(path, columns, pk, metrics)

    @staticmethod
    def query_monitoring_metrics(
        config: RepoConfig,
        project: str,
        metric_type: str,
        filters: Optional[Dict[str, Any]] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        assert isinstance(config.offline_store, ChrononOfflineStoreConfig)

        fname, columns, _ = _chronon_mon_table_meta(metric_type)
        path = _chronon_monitoring_path(config, fname)
        return _chronon_parquet_query(
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
        assert isinstance(config.offline_store, ChrononOfflineStoreConfig)

        path = _chronon_monitoring_path(config, MONITORING_PARQUET_FILES["feature"])
        tab = _chronon_read_parquet_if_exists(path)
        if tab is None or tab.num_rows == 0:
            return

        df = tab.to_pandas()
        mask = df["project_id"] == project
        if feature_view_name is not None and "feature_view_name" in df.columns:
            mask = mask & (df["feature_view_name"] == feature_view_name)
        if feature_name is not None and "feature_name" in df.columns:
            mask = mask & (df["feature_name"] == feature_name)
        if data_source_type is not None and "data_source_type" in df.columns:
            mask = mask & (df["data_source_type"] == data_source_type)
        if "is_baseline" in df.columns:
            mask = mask & (df["is_baseline"].isin([True, 1]))
            df.loc[mask, "is_baseline"] = False
            pq.write_table(pa.Table.from_pandas(df, preserve_index=False), path)

    @staticmethod
    def get_monitoring_max_timestamp(
        config: RepoConfig,
        data_source: DataSource,
        timestamp_field: str,
    ) -> Optional[datetime]:
        if data_source.__class__.__name__ != "ChrononSource":
            raise NotImplementedError(
                "ChrononOfflineStore only supports ChrononSource inputs."
            )

        source: Any = data_source
        resolved_path = FileSource.get_uri_for_file_path(
            repo_path=config.repo_path, uri=source.materialization_path
        )
        if not Path(resolved_path).exists():
            return None

        reverse_mapping = {value: key for key, value in source.field_mapping.items()}
        physical_col = reverse_mapping.get(timestamp_field, timestamp_field)

        try:
            import pyarrow.compute as pc
            import pyarrow.dataset as ds

            dataset = ds.dataset(resolved_path, format="parquet", partitioning="hive")
            if physical_col not in dataset.schema.names:
                return None
            table = dataset.to_table(columns=[physical_col])
            if table.num_rows == 0:
                return None
            max_val = pc.max(table.column(physical_col)).as_py()
            if max_val is None:
                return None
            if isinstance(max_val, datetime):
                return (
                    max_val if max_val.tzinfo else max_val.replace(tzinfo=timezone.utc)
                )
            if isinstance(max_val, date):
                return datetime.combine(
                    max_val, datetime.min.time(), tzinfo=timezone.utc
                )
            return None
        except Exception:
            return None


def _chronon_monitoring_base(config: RepoConfig) -> str:
    store_path = getattr(config.offline_store, "path", None)
    if store_path:
        return str(store_path)
    base = config.repo_path
    return str(base) if base else "."


def _chronon_monitoring_path(config: RepoConfig, filename: str) -> str:
    return os.path.join(_chronon_monitoring_base(config), MONITORING_DIR, filename)


def _chronon_mon_table_meta(metric_type: str):
    return monitoring_parquet_meta(metric_type)


def _chronon_read_parquet_if_exists(path: str) -> Optional[pa.Table]:
    if not os.path.isfile(path):
        return None
    try:
        return pq.read_table(path)
    except Exception:
        return None


def _chronon_parquet_upsert(
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
        for json_col in ("parameters", "result_summary"):
            if (
                json_col in r
                and r[json_col] is not None
                and not isinstance(r[json_col], str)
            ):
                r[json_col] = json.dumps(r[json_col])
        prepared.append(r)

    new_df = pd.DataFrame(prepared)
    for col in columns:
        if col not in new_df.columns:
            new_df[col] = None
    new_df = new_df[columns]

    existing = _chronon_read_parquet_if_exists(path)
    if existing is not None and existing.num_rows > 0:
        old_df = existing.to_pandas()
        for col in columns:
            if col not in old_df.columns:
                old_df[col] = None
        old_df = old_df[columns]
        if len(old_df) == 0:
            combined = new_df
        elif len(new_df) == 0:
            combined = old_df
        else:
            combined = pd.concat([old_df, new_df], ignore_index=True)
    else:
        combined = new_df

    combined = combined.drop_duplicates(subset=pk_cols, keep="last")
    table = pa.Table.from_pandas(combined, preserve_index=False)
    pq.write_table(table, path)


def _chronon_parquet_query(
    path: str,
    columns: List[str],
    project: str,
    filters: Optional[Dict[str, Any]],
    start_date: Optional[date],
    end_date: Optional[date],
) -> List[Dict[str, Any]]:
    tab = _chronon_read_parquet_if_exists(path)
    if tab is None or tab.num_rows == 0:
        return []

    df = tab.to_pandas()
    if project and "project_id" in df.columns:
        df = df[df["project_id"] == project]
    if filters:
        for key, value in filters.items():
            if value is not None and key in df.columns:
                if key == "is_baseline":
                    df = df[df["is_baseline"].isin([bool(value), int(value)])]
                else:
                    df = df[df[key] == value]
    if "metric_date" in df.columns:
        if start_date is not None:
            df = df[pd.to_datetime(df["metric_date"]).dt.date >= start_date]
        if end_date is not None:
            df = df[pd.to_datetime(df["metric_date"]).dt.date <= end_date]
        df = df.sort_values("metric_date", ascending=True)
    elif "job_id" in df.columns:
        df = df.sort_values("job_id", ascending=True)

    results = []
    available_cols = [c for c in columns if c in df.columns]
    for _, row in df.iterrows():
        record = {c: row.get(c) for c in available_cols}
        normalize_monitoring_row(record)
        for key in MONITORING_TIMESTAMP_FIELDS:
            val = record.get(key)
            if (
                val is not None
                and not isinstance(val, str)
                and hasattr(val, "isoformat")
            ):
                record[key] = val.isoformat()
        results.append(record)
    return results
