import os
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import ibis
import ibis.selectors as s
import numpy as np
import pandas as pd
import pyarrow
from ibis.expr import datatypes as dt
from ibis.expr.types import Table
from pytz import utc

from feast.data_source import DataSource
from feast.errors import SavedDatasetLocationAlreadyExists
from feast.feature_logging import LoggingConfig, LoggingSource
from feast.feature_view import FeatureView
from feast.infra.offline_stores import offline_utils
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
    get_pyarrow_schema_from_batch_source,
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
            r = r.concat(entity_table[e].cast("string"))  # type: ignore

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

        # TODO get range with ibis
        timestamp_range = IbisOfflineStore._get_entity_df_event_timestamp_range(
            entity_df, event_timestamp_col
        )

        entity_df = IbisOfflineStore._to_utc(entity_df, event_timestamp_col)

        entity_table = ibis.memtable(entity_df)
        entity_table = IbisOfflineStore._generate_row_id(
            entity_table, feature_views, event_timestamp_col
        )

        def read_fv(
            feature_view: FeatureView, feature_refs: List[str], full_feature_names: bool
        ) -> Tuple:
            fv_table: Table = ibis.read_parquet(feature_view.batch_source.name)

            for old_name, new_name in feature_view.batch_source.field_mapping.items():
                if old_name in fv_table.columns:
                    fv_table = fv_table.rename({new_name: old_name})

            timestamp_field = feature_view.batch_source.timestamp_field

            # TODO mutate only if tz-naive
            fv_table = fv_table.mutate(
                **{
                    timestamp_field: fv_table[timestamp_field].cast(
                        dt.Timestamp(timezone="UTC")
                    )
                }
            )

            full_name_prefix = feature_view.projection.name_alias or feature_view.name

            feature_refs = [
                fr.split(":")[1]
                for fr in feature_refs
                if fr.startswith(f"{full_name_prefix}:")
            ]

            if full_feature_names:
                fv_table = fv_table.rename(
                    {
                        f"{full_name_prefix}__{feature}": feature
                        for feature in feature_refs
                    }
                )

                feature_refs = [
                    f"{full_name_prefix}__{feature}" for feature in feature_refs
                ]

            return (
                fv_table,
                feature_view.batch_source.timestamp_field,
                feature_view.batch_source.created_timestamp_column,
                feature_view.projection.join_key_map
                or {e.name: e.name for e in feature_view.entity_columns},
                feature_refs,
                feature_view.ttl,
            )

        res = point_in_time_join(
            entity_table=entity_table,
            feature_tables=[
                read_fv(feature_view, feature_refs, full_feature_names)
                for feature_view in feature_views
            ],
            event_timestamp_col=event_timestamp_col,
        )

        return IbisRetrievalJob(
            res,
            OnDemandFeatureView.get_requested_odfvs(feature_refs, project, registry),
            full_feature_names,
            metadata=RetrievalMetadata(
                features=feature_refs,
                keys=list(set(entity_df.columns) - {event_timestamp_col}),
                min_event_timestamp=timestamp_range[0],
                max_event_timestamp=timestamp_range[1],
            ),
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
        assert isinstance(data_source, FileSource)

        fields = join_key_columns + feature_name_columns + [timestamp_field]
        start_date = start_date.astimezone(tz=utc)
        end_date = end_date.astimezone(tz=utc)

        table = ibis.read_parquet(data_source.path)

        table = table.select(*fields)

        # TODO get rid of this fix
        if "__log_date" in table.columns:
            table = table.drop("__log_date")

        table = table.filter(
            ibis.and_(
                table[timestamp_field] >= ibis.literal(start_date),
                table[timestamp_field] <= ibis.literal(end_date),
            )
        )

        return IbisRetrievalJob(
            table=table,
            on_demand_feature_views=[],
            full_feature_names=False,
            metadata=None,
        )

    @staticmethod
    def write_logged_features(
        config: RepoConfig,
        data: Union[pyarrow.Table, Path],
        source: LoggingSource,
        logging_config: LoggingConfig,
        registry: BaseRegistry,
    ):
        destination = logging_config.destination
        assert isinstance(destination, FileLoggingDestination)

        if isinstance(data, Path):
            table = ibis.read_parquet(data)
        else:
            table = ibis.memtable(data)

        if destination.partition_by:
            kwargs = {"partition_by": destination.partition_by}
        else:
            kwargs = {}

        # TODO always write to directory
        table.to_parquet(
            f"{destination.path}/{uuid.uuid4().hex}-{{i}}.parquet", **kwargs
        )

    @staticmethod
    def offline_write_batch(
        config: RepoConfig,
        feature_view: FeatureView,
        table: pyarrow.Table,
        progress: Optional[Callable[[int], Any]],
    ):
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
        prev_table = ibis.read_parquet(file_options.uri).to_pyarrow()
        if table.schema != prev_table.schema:
            table = table.cast(prev_table.schema)
        new_table = pyarrow.concat_tables([table, prev_table])

        ibis.memtable(new_table).to_parquet(file_options.uri)


class IbisRetrievalJob(RetrievalJob):
    def __init__(
        self, table, on_demand_feature_views, full_feature_names, metadata
    ) -> None:
        super().__init__()
        self.table = table
        self._on_demand_feature_views: List[
            OnDemandFeatureView
        ] = on_demand_feature_views
        self._full_feature_names = full_feature_names
        self._metadata = metadata

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
        assert isinstance(storage, SavedDatasetFileStorage)
        if not allow_overwrite and os.path.exists(storage.file_options.uri):
            raise SavedDatasetLocationAlreadyExists(location=storage.file_options.uri)

        filesystem, path = FileSource.create_filesystem_and_path(
            storage.file_options.uri,
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


def point_in_time_join(
    entity_table: Table,
    feature_tables: List[Tuple[Table, str, str, Dict[str, str], List[str], timedelta]],
    event_timestamp_col="event_timestamp",
):
    # TODO handle ttl
    all_entities = [event_timestamp_col]
    for (
        feature_table,
        timestamp_field,
        created_timestamp_field,
        join_key_map,
        _,
        _,
    ) in feature_tables:
        all_entities.extend(join_key_map.values())

    r = ibis.literal("")

    for e in set(all_entities):
        r = r.concat(entity_table[e].cast("string"))  # type: ignore

    entity_table = entity_table.mutate(entity_row_id=r)

    acc_table = entity_table

    for (
        feature_table,
        timestamp_field,
        created_timestamp_field,
        join_key_map,
        feature_refs,
        ttl,
    ) in feature_tables:
        predicates = [
            feature_table[k] == entity_table[v] for k, v in join_key_map.items()
        ]

        predicates.append(
            feature_table[timestamp_field] <= entity_table[event_timestamp_col],
        )

        if ttl:
            predicates.append(
                feature_table[timestamp_field]
                >= entity_table[event_timestamp_col] - ibis.literal(ttl)
            )

        feature_table = feature_table.inner_join(
            entity_table, predicates, lname="", rname="{name}_y"
        )

        feature_table = feature_table.drop(s.endswith("_y"))

        order_by_fields = [ibis.desc(feature_table[timestamp_field])]
        if created_timestamp_field:
            order_by_fields.append(ibis.desc(feature_table[created_timestamp_field]))

        feature_table = (
            feature_table.group_by(by="entity_row_id")
            .order_by(order_by_fields)
            .mutate(rn=ibis.row_number())
        )

        feature_table = feature_table.filter(
            feature_table["rn"] == ibis.literal(0)
        ).drop("rn")

        select_cols = ["entity_row_id"]
        select_cols.extend(feature_refs)
        feature_table = feature_table.select(select_cols)

        acc_table = acc_table.left_join(
            feature_table,
            predicates=[feature_table.entity_row_id == acc_table.entity_row_id],
            lname="",
            rname="{name}_yyyy",
        )

        acc_table = acc_table.drop(s.endswith("_yyyy"))

    acc_table = acc_table.drop("entity_row_id")

    return acc_table
