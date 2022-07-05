from datetime import datetime
from typing import Callable, Dict, List, Literal, Optional, Tuple, Union

import dask.dataframe as dd
import pandas as pd
import pyarrow as pa
from tqdm import tqdm

from feast import Entity, FeatureView, RepoConfig, ValueType
from feast.feature_view import DUMMY_ENTITY_ID
from .batch_materialization_engine import (
    BatchMaterializationEngine,
    MaterializationJob,
    MaterializationTask,
)
from feast.infra.offline_stores.offline_store import OfflineStore
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel
from feast.type_map import python_values_to_proto_values
from ...registry import BaseRegistry


class LocalMaterializationJob(MaterializationJob):
    def __init__(self) -> None:
        super().__init__()

    def status(self) -> str:
        pass

    def should_be_retried(self) -> str:
        pass

    def job_id(self) -> str:
        pass

    def url(self) -> Optional[str]:
        pass


DEFAULT_BATCH_SIZE = 10_000


class LocalMaterializationEngineConfig(FeastConfigBaseModel):
    """Batch Materialization Engine config for local in-process engine"""

    type: Literal["local"] = "local"
    """ Type selector"""


class LocalMaterializationEngine(BatchMaterializationEngine):
    def __init__(
        self,
        *,
        repo_config: RepoConfig,
        offline_store: OfflineStore,
        online_store: OnlineStore,
        **kwargs,
    ):
        super().__init__(
            repo_config=repo_config,
            offline_store=offline_store,
            online_store=online_store,
            **kwargs,
        )

    def materialize(self, tasks: List[MaterializationTask]) -> List[MaterializationJob]:
        return []

    def materialize_one(
        self,
        feature_view: FeatureView,
        start_date: datetime,
        end_date: datetime,
        project: str,
        tqdm_builder: Callable[[int], tqdm],
    ):
        entities = []
        for entity_name in feature_view.entities:
            entities.append(self.registry.get_entity(entity_name, project))

        (
            join_key_columns,
            feature_name_columns,
            timestamp_field,
            created_timestamp_column,
        ) = _get_column_names(feature_view, entities)

        offline_job = self.offline_store.pull_latest_from_table_or_query(
            config=self.repo_config,
            data_source=feature_view.batch_source,
            join_key_columns=join_key_columns,
            feature_name_columns=feature_name_columns,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            start_date=start_date,
            end_date=end_date,
        )

        table = offline_job.to_arrow()

        if feature_view.batch_source.field_mapping is not None:
            table = _run_field_mapping(table, feature_view.batch_source.field_mapping)

        join_key_to_value_type = {
            entity.name: entity.dtype.to_value_type()
            for entity in feature_view.entity_columns
        }

        with tqdm_builder(table.num_rows) as pbar:
            for batch in table.to_batches(DEFAULT_BATCH_SIZE):
                rows_to_write = _convert_arrow_to_proto(
                    batch, feature_view, join_key_to_value_type
                )
                self.online_store.online_write_batch(
                    self.repo_config,
                    feature_view,
                    rows_to_write,
                    lambda x: pbar.update(x),
                )


def _get_column_names(
    feature_view: FeatureView, entities: List[Entity]
) -> Tuple[List[str], List[str], str, Optional[str]]:
    """
    If a field mapping exists, run it in reverse on the join keys,
    feature names, event timestamp column, and created timestamp column
    to get the names of the relevant columns in the offline feature store table.

    Returns:
        Tuple containing the list of reverse-mapped join_keys,
        reverse-mapped feature names, reverse-mapped event timestamp column,
        and reverse-mapped created timestamp column that will be passed into
        the query to the offline store.
    """
    # if we have mapped fields, use the original field names in the call to the offline store
    timestamp_field = feature_view.batch_source.timestamp_field
    feature_names = [feature.name for feature in feature_view.features]
    created_timestamp_column = feature_view.batch_source.created_timestamp_column
    join_keys = [
        entity.join_key for entity in entities if entity.join_key != DUMMY_ENTITY_ID
    ]
    if feature_view.batch_source.field_mapping is not None:
        reverse_field_mapping = {
            v: k for k, v in feature_view.batch_source.field_mapping.items()
        }
        timestamp_field = (
            reverse_field_mapping[timestamp_field]
            if timestamp_field in reverse_field_mapping.keys()
            else timestamp_field
        )
        created_timestamp_column = (
            reverse_field_mapping[created_timestamp_column]
            if created_timestamp_column
            and created_timestamp_column in reverse_field_mapping.keys()
            else created_timestamp_column
        )
        join_keys = [
            reverse_field_mapping[col] if col in reverse_field_mapping.keys() else col
            for col in join_keys
        ]
        feature_names = [
            reverse_field_mapping[col] if col in reverse_field_mapping.keys() else col
            for col in feature_names
        ]

    # We need to exclude join keys and timestamp columns from the list of features, after they are mapped to
    # their final column names via the `field_mapping` field of the source.
    feature_names = [
        name
        for name in feature_names
        if name not in join_keys
        and name != timestamp_field
        and name != created_timestamp_column
    ]
    return (
        join_keys,
        feature_names,
        timestamp_field,
        created_timestamp_column,
    )


def _run_field_mapping(table: pa.Table, field_mapping: Dict[str, str],) -> pa.Table:
    # run field mapping in the forward direction
    cols = table.column_names
    mapped_cols = [
        field_mapping[col] if col in field_mapping.keys() else col for col in cols
    ]
    table = table.rename_columns(mapped_cols)
    return table


def _run_dask_field_mapping(
    table: dd.DataFrame, field_mapping: Dict[str, str],
):
    if field_mapping:
        # run field mapping in the forward direction
        table = table.rename(columns=field_mapping)
        table = table.persist()

    return table


def _coerce_datetime(ts):
    """
    Depending on underlying time resolution, arrow to_pydict() sometimes returns pd
    timestamp type (for nanosecond resolution), and sometimes you get standard python datetime
    (for microsecond resolution).
    While pd timestamp class is a subclass of python datetime, it doesn't always behave the
    same way. We convert it to normal datetime so that consumers downstream don't have to deal
    with these quirks.
    """
    if isinstance(ts, pd.Timestamp):
        return ts.to_pydatetime()
    else:
        return ts


def _convert_arrow_to_proto(
    table: Union[pa.Table, pa.RecordBatch],
    feature_view: FeatureView,
    join_keys: Dict[str, ValueType],
) -> List[Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]]:
    # Avoid ChunkedArrays which guarentees `zero_copy_only` availiable.
    if isinstance(table, pa.Table):
        table = table.to_batches()[0]

    columns = [
        (field.name, field.dtype.to_value_type()) for field in feature_view.features
    ] + list(join_keys.items())

    proto_values_by_column = {
        column: python_values_to_proto_values(
            table.column(column).to_numpy(zero_copy_only=False), value_type
        )
        for column, value_type in columns
    }

    entity_keys = [
        EntityKeyProto(
            join_keys=join_keys,
            entity_values=[proto_values_by_column[k][idx] for k in join_keys],
        )
        for idx in range(table.num_rows)
    ]

    # Serialize the features per row
    feature_dict = {
        feature.name: proto_values_by_column[feature.name]
        for feature in feature_view.features
    }
    features = [dict(zip(feature_dict, vars)) for vars in zip(*feature_dict.values())]

    # Convert event_timestamps
    event_timestamps = [
        _coerce_datetime(val)
        for val in pd.to_datetime(
            table.column(feature_view.batch_source.timestamp_field).to_numpy(
                zero_copy_only=False
            )
        )
    ]

    # Convert created_timestamps if they exist
    if feature_view.batch_source.created_timestamp_column:
        created_timestamps = [
            _coerce_datetime(val)
            for val in pd.to_datetime(
                table.column(
                    feature_view.batch_source.created_timestamp_column
                ).to_numpy(zero_copy_only=False)
            )
        ]
    else:
        created_timestamps = [None] * table.num_rows

    return list(zip(entity_keys, features, event_timestamps, created_timestamps))
