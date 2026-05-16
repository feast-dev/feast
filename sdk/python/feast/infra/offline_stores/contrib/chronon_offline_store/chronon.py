from datetime import datetime
from pathlib import Path
from typing import Any, Callable, List, Literal, Optional, Union

import pandas as pd
import pyarrow as pa
from pydantic import StrictStr

from feast import utils
from feast.data_source import DataSource
from feast.feature_logging import LoggingConfig, LoggingSource
from feast.feature_view import FeatureView
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.offline_stores.offline_utils import (
    DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL,
    assert_expected_columns_in_entity_df,
    get_expected_join_keys,
)
from feast.infra.registry.base_registry import BaseRegistry
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
    ):
        self.evaluation_function = evaluation_function
        self._full_feature_names = full_feature_names
        self._metadata = metadata

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        return []

    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        return self.evaluation_function().reset_index(drop=True)

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pa.Table:
        return pa.Table.from_pandas(self._to_df_internal(timeout=timeout))

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: bool = False,
        timeout: Optional[int] = None,
    ):
        raise NotImplementedError(
            "ChrononRetrievalJob does not currently support persisted saved datasets."
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
    config: RepoConfig, feature_view: FeatureView
) -> pd.DataFrame:
    source = _get_chronon_source(feature_view)
    resolved_path = FileSource.get_uri_for_file_path(
        repo_path=config.repo_path, uri=source.materialization_path
    )
    dataframe = pd.read_parquet(resolved_path)
    if source.field_mapping:
        dataframe = dataframe.rename(columns=source.field_mapping)
    return utils.make_df_tzaware(dataframe)


def _output_feature_name(
    feature_view: FeatureView, feature_name: str, full_feature_names: bool
) -> str:
    if full_feature_names:
        return f"{feature_view.projection.name_to_use()}__{feature_name}"
    return feature_name


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
        if requested_odfvs:
            raise ValueError(
                "ChrononOfflineStore does not support on-demand feature views."
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
                dataframe = _load_materialized_dataframe(config, feature_view)
                timestamp_col = feature_view.batch_source.timestamp_field  # type: ignore[union-attr]
                if timestamp_col:
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
                keep_columns = list(
                    {
                        *(entity.name for entity in feature_view.entity_columns),
                        timestamp_col,
                        *(selected_features),
                    }
                )
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
            )

        entity_df = utils.make_df_tzaware(entity_df)
        entity_event_timestamp_col = DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL
        expected_join_keys = get_expected_join_keys(project, feature_views, registry)
        assert_expected_columns_in_entity_df(
            entity_df.dtypes.to_dict(),
            expected_join_keys,
            entity_event_timestamp_col,
        )

        def evaluate_historical_retrieval() -> pd.DataFrame:
            result = entity_df.copy()
            for feature_view, selected_features in requested_features.items():
                source_df = _load_materialized_dataframe(config, feature_view)
                timestamp_col = feature_view.batch_source.timestamp_field  # type: ignore[union-attr]
                created_col = feature_view.batch_source.created_timestamp_column  # type: ignore[union-attr]
                left_keys = []
                right_keys = []
                for entity_column in feature_view.entity_columns:
                    left_keys.append(
                        feature_view.projection.join_key_map.get(
                            entity_column.name, entity_column.name
                        )
                    )
                    right_keys.append(entity_column.name)

                if created_col and created_col in source_df.columns:
                    source_df = source_df.sort_values(created_col).drop_duplicates(
                        subset=right_keys + [timestamp_col], keep="last"
                    )

                merge_columns = right_keys + [timestamp_col] + list(selected_features)
                merge_frame = source_df.loc[
                    :, [col for col in merge_columns if col in source_df.columns]
                ].copy()
                merge_frame = merge_frame.rename(
                    columns={
                        feature: _output_feature_name(
                            feature_view, feature, full_feature_names
                        )
                        for feature in selected_features
                    }
                )

                result = result.merge(
                    merge_frame,
                    how="left",
                    left_on=left_keys + [entity_event_timestamp_col],
                    right_on=right_keys + [timestamp_col],
                    sort=False,
                )

                drop_columns = [
                    col
                    for col in right_keys + [timestamp_col]
                    if col in result.columns
                    and col not in left_keys + [entity_event_timestamp_col]
                ]
                if drop_columns:
                    result = result.drop(columns=drop_columns)
            return result

        return ChrononRetrievalJob(
            evaluation_function=evaluate_historical_retrieval,
            full_feature_names=full_feature_names,
            metadata=metadata,
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
            dataframe = _load_materialized_dataframe(config, feature_view)
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
            dataframe = _load_materialized_dataframe(config, feature_view)
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
