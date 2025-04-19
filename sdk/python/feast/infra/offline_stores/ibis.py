import random
import string
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import ibis
import ibis.selectors as s
import numpy as np
import pandas as pd
import pyarrow
from ibis.expr import datatypes as dt
from ibis.expr.types import Table

from feast.data_source import DataSource
from feast.feature_logging import LoggingConfig, LoggingSource
from feast.feature_view import FeatureView
from feast.infra.offline_stores import offline_utils
from feast.infra.offline_stores.file_source import (
    FileLoggingDestination,
)
from feast.infra.offline_stores.offline_store import (
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


def pull_latest_from_table_or_query_ibis(
    config: RepoConfig,
    data_source: DataSource,
    join_key_columns: List[str],
    feature_name_columns: List[str],
    timestamp_field: str,
    created_timestamp_column: Optional[str],
    start_date: datetime,
    end_date: datetime,
    data_source_reader: Callable[[DataSource, str], Table],
    data_source_writer: Callable[[pyarrow.Table, DataSource, str], None],
    staging_location: Optional[str] = None,
    staging_location_endpoint_override: Optional[str] = None,
) -> RetrievalJob:
    fields = join_key_columns + feature_name_columns + [timestamp_field]
    if created_timestamp_column:
        fields.append(created_timestamp_column)
    start_date = start_date.astimezone(tz=timezone.utc)
    end_date = end_date.astimezone(tz=timezone.utc)

    table = data_source_reader(data_source, str(config.repo_path))

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

    table = deduplicate(
        table=table,
        group_by_cols=join_key_columns,
        event_timestamp_col=timestamp_field,
        created_timestamp_col=created_timestamp_column,
    )

    return IbisRetrievalJob(
        table=table,
        on_demand_feature_views=[],
        full_feature_names=False,
        metadata=None,
        data_source_writer=data_source_writer,
        staging_location=staging_location,
        staging_location_endpoint_override=staging_location_endpoint_override,
        repo_path=str(config.repo_path),
    )


def _get_entity_df_event_timestamp_range(
    entity_df: pd.DataFrame, entity_df_event_timestamp_col: str
) -> Tuple[datetime, datetime]:
    entity_df_event_timestamp = entity_df.loc[
        :, entity_df_event_timestamp_col
    ].infer_objects()
    if pd.api.types.is_string_dtype(
        entity_df_event_timestamp
    ) or pd.api.types.is_object_dtype(entity_df_event_timestamp):
        entity_df_event_timestamp = pd.to_datetime(entity_df_event_timestamp, utc=True)
    entity_df_event_timestamp_range = (
        entity_df_event_timestamp.min().to_pydatetime(),
        entity_df_event_timestamp.max().to_pydatetime(),
    )

    return entity_df_event_timestamp_range


def _to_utc(entity_df: pd.DataFrame, event_timestamp_col):
    entity_df_event_timestamp = entity_df.loc[:, event_timestamp_col].infer_objects()

    if pd.api.types.is_string_dtype(
        entity_df_event_timestamp
    ) or pd.api.types.is_object_dtype(entity_df_event_timestamp):
        entity_df_event_timestamp = pd.to_datetime(entity_df_event_timestamp, utc=True)

    entity_df[event_timestamp_col] = entity_df_event_timestamp
    return entity_df


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


def get_historical_features_ibis(
    config: RepoConfig,
    feature_views: List[FeatureView],
    feature_refs: List[str],
    entity_df: Union[pd.DataFrame, str],
    registry: BaseRegistry,
    project: str,
    data_source_reader: Callable[[DataSource, str], Table],
    data_source_writer: Callable[[pyarrow.Table, DataSource, str], None],
    full_feature_names: bool = False,
    staging_location: Optional[str] = None,
    staging_location_endpoint_override: Optional[str] = None,
    event_expire_timestamp_fn=None,
) -> RetrievalJob:
    entity_schema = _get_entity_schema(
        entity_df=entity_df,
    )
    event_timestamp_col = offline_utils.infer_event_timestamp_from_entity_df(
        entity_schema=entity_schema,
    )

    # TODO get range with ibis
    timestamp_range = _get_entity_df_event_timestamp_range(
        entity_df, event_timestamp_col
    )

    entity_df = _to_utc(entity_df, event_timestamp_col)

    entity_table = ibis.memtable(entity_df)
    entity_table = _generate_row_id(entity_table, feature_views, event_timestamp_col)

    def read_fv(
        feature_view: FeatureView, feature_refs: List[str], full_feature_names: bool
    ) -> Tuple:
        fv_table: Table = data_source_reader(
            feature_view.batch_source, str(config.repo_path)
        )

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
                {f"{full_name_prefix}__{feature}": feature for feature in feature_refs}
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
        event_expire_timestamp_fn=event_expire_timestamp_fn,
    )

    odfvs = OnDemandFeatureView.get_requested_odfvs(feature_refs, project, registry)

    substrait_odfvs = [fv for fv in odfvs if fv.mode == "substrait"]
    for odfv in substrait_odfvs:
        res = odfv.transform_ibis(res, full_feature_names)

    return IbisRetrievalJob(
        res,
        [fv for fv in odfvs if fv.mode != "substrait"],
        full_feature_names,
        metadata=RetrievalMetadata(
            features=feature_refs,
            keys=list(set(entity_df.columns) - {event_timestamp_col}),
            min_event_timestamp=timestamp_range[0],
            max_event_timestamp=timestamp_range[1],
        ),
        data_source_writer=data_source_writer,
        staging_location=staging_location,
        staging_location_endpoint_override=staging_location_endpoint_override,
        repo_path=str(config.repo_path),
    )


def pull_all_from_table_or_query_ibis(
    config: RepoConfig,
    data_source: DataSource,
    join_key_columns: List[str],
    feature_name_columns: List[str],
    timestamp_field: str,
    data_source_reader: Callable[[DataSource, str], Table],
    data_source_writer: Callable[[pyarrow.Table, DataSource, str], None],
    created_timestamp_column: Optional[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    staging_location: Optional[str] = None,
    staging_location_endpoint_override: Optional[str] = None,
) -> RetrievalJob:
    timestamp_fields = [timestamp_field]
    if created_timestamp_column:
        timestamp_fields.append(created_timestamp_column)
    fields = join_key_columns + feature_name_columns + timestamp_fields
    if start_date:
        start_date = start_date.astimezone(tz=timezone.utc)
    if end_date:
        end_date = end_date.astimezone(tz=timezone.utc)

    table = data_source_reader(data_source, str(config.repo_path))

    table = table.select(*fields)

    # TODO get rid of this fix
    if "__log_date" in table.columns:
        table = table.drop("__log_date")

    table = table.filter(
        ibis.and_(
            table[timestamp_field] >= ibis.literal(start_date)
            if start_date
            else ibis.literal(True),
            table[timestamp_field] <= ibis.literal(end_date)
            if end_date
            else ibis.literal(True),
        )
    )

    return IbisRetrievalJob(
        table=table,
        on_demand_feature_views=[],
        full_feature_names=False,
        metadata=None,
        data_source_writer=data_source_writer,
        staging_location=staging_location,
        staging_location_endpoint_override=staging_location_endpoint_override,
        repo_path=str(config.repo_path),
    )


def write_logged_features_ibis(
    config: RepoConfig,
    data: Union[pyarrow.Table, Path],
    source: LoggingSource,
    logging_config: LoggingConfig,
    registry: BaseRegistry,
):
    destination = logging_config.destination
    assert isinstance(destination, FileLoggingDestination)

    table = ibis.read_parquet(data) if isinstance(data, Path) else ibis.memtable(data)

    if destination.partition_by:
        kwargs = {"partition_by": destination.partition_by}
    else:
        kwargs = {}

    # TODO always write to directory
    table.to_parquet(f"{destination.path}/{uuid.uuid4().hex}-{{i}}.parquet", **kwargs)


def offline_write_batch_ibis(
    config: RepoConfig,
    feature_view: FeatureView,
    table: pyarrow.Table,
    progress: Optional[Callable[[int], Any]],
    data_source_writer: Callable[[pyarrow.Table, DataSource, str], None],
):
    pa_schema, column_names = get_pyarrow_schema_from_batch_source(
        config, feature_view.batch_source
    )
    if column_names != table.column_names:
        raise ValueError(
            f"The input pyarrow table has schema {table.schema} with the incorrect columns {table.column_names}. "
            f"The schema is expected to be {pa_schema} with the columns (in this exact order) to be {column_names}."
        )

    data_source_writer(
        ibis.memtable(table), feature_view.batch_source, str(config.repo_path)
    )


def deduplicate(
    table: Table,
    group_by_cols: List[str],
    event_timestamp_col: str,
    created_timestamp_col: Optional[str],
):
    order_by_fields = [ibis.desc(table[event_timestamp_col])]
    if created_timestamp_col:
        order_by_fields.append(ibis.desc(table[created_timestamp_col]))

    window = ibis.window(group_by=group_by_cols, order_by=order_by_fields, following=0)
    table = table.mutate(rn=ibis.row_number().over(window))

    return table.filter(table["rn"] == ibis.literal(0)).drop("rn")


def point_in_time_join(
    entity_table: Table,
    feature_tables: List[Tuple[Table, str, str, Dict[str, str], List[str], timedelta]],
    event_timestamp_col="event_timestamp",
    event_expire_timestamp_fn=None,
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
        if ttl:
            if not event_expire_timestamp_fn:
                feature_table = feature_table.mutate(
                    event_expire_timestamp=feature_table[timestamp_field]
                    + ibis.literal(ttl)
                )
            else:
                alias = "".join(random.choices(string.ascii_uppercase, k=10))

                feature_table = feature_table.alias(alias=alias).sql(
                    f"SELECT *, {event_expire_timestamp_fn(timestamp_field, ttl)} AS event_expire_timestamp FROM {alias}"
                )

        predicates = [
            feature_table[k] == entity_table[v] for k, v in join_key_map.items()
        ]

        predicates.append(
            feature_table[timestamp_field] <= entity_table[event_timestamp_col],
        )

        if ttl:
            predicates.append(
                feature_table["event_expire_timestamp"]
                >= entity_table[event_timestamp_col]
            )

        feature_table = feature_table.inner_join(
            entity_table, predicates, lname="", rname="{name}_y"
        )

        feature_table = feature_table.drop(s.endswith("_y"))

        feature_table = deduplicate(
            table=feature_table,
            group_by_cols=["entity_row_id"],
            event_timestamp_col=timestamp_field,
            created_timestamp_col=created_timestamp_field,
        )

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


def list_s3_files(path: str, endpoint_url: str) -> List[str]:
    import boto3

    s3 = boto3.client("s3", endpoint_url=endpoint_url)
    if path.startswith("s3://"):
        path = path[len("s3://") :]
    bucket, prefix = path.split("/", 1)
    objects = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    contents = objects["Contents"]
    files = [
        f"s3://{bucket}/{content['Key']}"
        for content in contents
        if content["Key"].endswith("parquet")
    ]
    return files


class IbisRetrievalJob(RetrievalJob):
    def __init__(
        self,
        table,
        on_demand_feature_views,
        full_feature_names,
        metadata,
        data_source_writer,
        staging_location,
        staging_location_endpoint_override,
        repo_path,
    ) -> None:
        super().__init__()
        self.table = table
        self._on_demand_feature_views: List[OnDemandFeatureView] = (
            on_demand_feature_views
        )
        self._full_feature_names = full_feature_names
        self._metadata = metadata
        self.data_source_writer = data_source_writer
        self.staging_location = staging_location
        self.staging_location_endpoint_override = staging_location_endpoint_override
        self.repo_path = repo_path

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
        self.data_source_writer(
            self.table,
            storage.to_data_source(),
            self.repo_path,
            "overwrite",
            allow_overwrite,
        )

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        return self._metadata

    def supports_remote_storage_export(self) -> bool:
        return self.staging_location is not None

    def to_remote_storage(self) -> List[str]:
        path = self.staging_location + f"/{str(uuid.uuid4())}"

        storage_options = {"AWS_ENDPOINT_URL": self.staging_location_endpoint_override}

        self.table.to_delta(path, storage_options=storage_options)

        return list_s3_files(path, self.staging_location_endpoint_override)
