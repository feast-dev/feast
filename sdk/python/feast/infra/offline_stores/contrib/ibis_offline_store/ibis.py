from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import ibis
import ibis.selectors as s
import numpy as np
import pandas as pd
import pyarrow
from ibis.expr.types import Table

from feast.data_source import DataSource
from feast.feature_logging import LoggingConfig, LoggingSource
from feast.feature_view import FeatureView
from feast.infra.offline_stores import offline_utils
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.registry.base_registry import BaseRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage


def _get_entity_schema(entity_df: pd.DataFrame) -> Dict[str, np.dtype]:
    return dict(zip(entity_df.columns, entity_df.dtypes))


class IbisOfflineStore(OfflineStore):
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
        raise NotImplementedError()

    def _get_entity_df_event_timestamp_range(
        entity_df: pd.DataFrame, entity_df_event_timestamp_col: str
    ) -> Tuple[datetime, datetime]:
        entity_df_event_timestamp = entity_df.loc[
            :, entity_df_event_timestamp_col
        ].infer_objects()
        if pd.api.types.is_string_dtype(entity_df_event_timestamp):
            entity_df_event_timestamp = pd.to_datetime(
                entity_df_event_timestamp, utc=True
            )
        entity_df_event_timestamp_range = (
            entity_df_event_timestamp.min().to_pydatetime(),
            entity_df_event_timestamp.max().to_pydatetime(),
        )

        return entity_df_event_timestamp_range

    @staticmethod
    def _get_historical_features_one(
        feature_view: FeatureView,
        entity_table: Table,
        feature_refs: List[str],
        full_feature_names: bool,
        timestamp_range: Tuple,
        acc_table: Table,
        event_timestamp_col: str,
    ) -> Table:
        fv_table: Table = ibis.read_parquet(feature_view.batch_source.name)
        fv_table = fv_table.relabel(feature_view.batch_source.field_mapping)
        timestamp_field = feature_view.batch_source.timestamp_field

        full_name_prefix = feature_view.projection.name_alias or feature_view.name

        feature_refs = [
            fr.split(":")[1]
            for fr in feature_refs
            if fr.startswith(f"{full_name_prefix}:")
        ]

        timestamp_range_end = ibis.literal(
            timestamp_range[1].strftime("%Y-%m-%d %H:%M:%S.%f")
        ).cast("timestamp")
        fv_table = fv_table.filter(
            (
                fv_table[timestamp_field] <= timestamp_range_end
            )  # TODO ((fv_table[timestamp_field] > timestamp_range[0] - fv.ttl) if fv.ttl and fv.ttl > timedelta(0,0,0,0,0,0,0) else ibis.literal(True))
        )

        # join_key_map = feature_view.projection.join_key_map or {e.name: e.name for e in feature_view.entity_columns}
        # predicates = [fv_table[k] == entity_table[v] for k, v in join_key_map.items()]

        if feature_view.projection.join_key_map:
            predicates = [
                fv_table[k] == entity_table[v]
                for k, v in feature_view.projection.join_key_map.items()
            ]
        else:
            predicates = [
                fv_table[e.name] == entity_table[e.name]
                for e in feature_view.entity_columns
            ]

        predicates.append(
            fv_table[timestamp_field] <= entity_table[event_timestamp_col]
        )

        fv_table = fv_table.inner_join(
            entity_table, predicates, lname="", rname="{name}_y"
        )

        fv_table = (
            fv_table.group_by(by="entity_row_id")
            .order_by(ibis.desc(fv_table[timestamp_field]))
            .mutate(rn=ibis.row_number())
        )

        fv_table = fv_table.filter(fv_table["rn"] == ibis.literal(0))

        select_cols = ["entity_row_id"]
        select_cols.extend(feature_refs)
        fv_table = fv_table.select(select_cols)

        if full_feature_names:
            fv_table = fv_table.relabel(
                {feature: f"{full_name_prefix}__{feature}" for feature in feature_refs}
            )

        acc_table = acc_table.left_join(
            fv_table,
            predicates=[fv_table.entity_row_id == acc_table.entity_row_id],
            lname="",
            rname="{name}_yyyy",
        )

        acc_table = acc_table.drop(s.endswith("_yyyy"))

        return acc_table

    @staticmethod
    def _to_utc(entity_df: pd.DataFrame, event_timestamp_col):
        entity_df_event_timestamp = entity_df.loc[
            :, event_timestamp_col
        ].infer_objects()
        if pd.api.types.is_string_dtype(entity_df_event_timestamp):
            entity_df_event_timestamp = pd.to_datetime(
                entity_df_event_timestamp, utc=True
            )

        entity_df[event_timestamp_col] = entity_df_event_timestamp
        return entity_df

    @staticmethod
    def _generate_row_id(
        entity_table: Table, feature_views: List[FeatureView], event_timestamp_col
    ) -> Table:
        all_entities = [event_timestamp_col]
        for fv in feature_views:
            if fv.projection.join_key_map:
                all_entities.extend(fv.projection.join_key_map.values())
            else:
                all_entities.extend([e.name for e in fv.entity_columns])

        r = ibis.literal("")

        for e in set(all_entities):
            r = r.concat(entity_table[e].cast("string"))

        entity_table = entity_table.mutate(entity_row_id=r)

        return entity_table

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
        entity_schema = _get_entity_schema(
            entity_df=entity_df,
        )
        event_timestamp_col = offline_utils.infer_event_timestamp_from_entity_df(
            entity_schema=entity_schema,
        )

        timestamp_range = IbisOfflineStore._get_entity_df_event_timestamp_range(
            entity_df, event_timestamp_col
        )
        entity_df = IbisOfflineStore._to_utc(entity_df, event_timestamp_col)

        entity_table = ibis.memtable(entity_df)
        entity_table = IbisOfflineStore._generate_row_id(
            entity_table, feature_views, event_timestamp_col
        )

        res: Table = entity_table

        for fv in feature_views:
            res = IbisOfflineStore._get_historical_features_one(
                fv,
                entity_table,
                feature_refs,
                full_feature_names,
                timestamp_range,
                res,
                event_timestamp_col,
            )

        res = res.drop("entity_row_id")

        return IbisRetrievalJob(
            res,
            OnDemandFeatureView.get_requested_odfvs(feature_refs, project, registry),
            full_feature_names,
        )

    @staticmethod
    def pull_all_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        raise NotImplementedError()

    @staticmethod
    def write_logged_features(
        config: RepoConfig,
        data: Union[pyarrow.Table, Path],
        source: LoggingSource,
        logging_config: LoggingConfig,
        registry: BaseRegistry,
    ):
        raise NotImplementedError()

    @staticmethod
    def offline_write_batch(
        config: RepoConfig,
        feature_view: FeatureView,
        table: pyarrow.Table,
        progress: Optional[Callable[[int], Any]],
    ):
        raise NotImplementedError()


class IbisRetrievalJob(RetrievalJob):
    def __init__(self, table, on_demand_feature_views, full_feature_names) -> None:
        super().__init__()
        self.table = table
        self._on_demand_feature_views: List[
            OnDemandFeatureView
        ] = on_demand_feature_views
        self._full_feature_names = full_feature_names

    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        return self.table.execute()

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pyarrow.Table:
        return self.table.to_pyarrow()

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        return self._on_demand_feature_views

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: bool = False,
        timeout: Optional[int] = None,
    ):
        pass

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        pass
