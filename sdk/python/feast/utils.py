import os
import typing
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd
import pyarrow
from dask import dataframe as dd
from dateutil.tz import tzlocal
from pytz import utc

from feast.constants import FEAST_FS_YAML_FILE_PATH_ENV_NAME
from feast.entity import Entity
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.type_map import python_values_to_proto_values
from feast.value_type import ValueType

if typing.TYPE_CHECKING:
    from feast.feature_view import FeatureView
    from feast.on_demand_feature_view import OnDemandFeatureView


def make_tzaware(t: datetime) -> datetime:
    """We assume tz-naive datetimes are UTC"""
    if t.tzinfo is None:
        return t.replace(tzinfo=utc)
    else:
        return t


def make_df_tzaware(t: pd.DataFrame) -> pd.DataFrame:
    """Make all datetime type columns tzaware; leave everything else intact."""
    df = t.copy()  # don't modify incoming dataframe inplace
    for column in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[column]):
            df[column] = pd.to_datetime(df[column], utc=True)
    return df


def to_naive_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts
    else:
        return ts.astimezone(utc).replace(tzinfo=None)


def maybe_local_tz(t: datetime) -> datetime:
    if t.tzinfo is None:
        return t.replace(tzinfo=tzlocal())
    else:
        return t


def get_default_yaml_file_path(repo_path: Path) -> Path:
    if FEAST_FS_YAML_FILE_PATH_ENV_NAME in os.environ:
        yaml_path = os.environ[FEAST_FS_YAML_FILE_PATH_ENV_NAME]
        return Path(yaml_path)
    else:
        return repo_path / "feature_store.yaml"


def _get_requested_feature_views_to_features_dict(
    feature_refs: List[str],
    feature_views: List["FeatureView"],
    on_demand_feature_views: List["OnDemandFeatureView"],
) -> Tuple[Dict["FeatureView", List[str]], Dict["OnDemandFeatureView", List[str]]]:
    """Create a dict of FeatureView -> List[Feature] for all requested features.
    Set full_feature_names to True to have feature names prefixed by their feature view name."""

    feature_views_to_feature_map: Dict["FeatureView", List[str]] = defaultdict(list)
    on_demand_feature_views_to_feature_map: Dict[
        "OnDemandFeatureView", List[str]
    ] = defaultdict(list)

    for ref in feature_refs:
        ref_parts = ref.split(":")
        feature_view_from_ref = ref_parts[0]
        feature_from_ref = ref_parts[1]

        found = False
        for fv in feature_views:
            if fv.projection.name_to_use() == feature_view_from_ref:
                found = True
                feature_views_to_feature_map[fv].append(feature_from_ref)
        for odfv in on_demand_feature_views:
            if odfv.projection.name_to_use() == feature_view_from_ref:
                found = True
                on_demand_feature_views_to_feature_map[odfv].append(feature_from_ref)

        if not found:
            raise ValueError(f"Could not find feature view from reference {ref}")

    return feature_views_to_feature_map, on_demand_feature_views_to_feature_map


def _get_column_names(
    feature_view: "FeatureView", entities: List[Entity]
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

    from feast.feature_view import DUMMY_ENTITY_ID

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


def _run_pyarrow_field_mapping(
    table: pyarrow.Table,
    field_mapping: Dict[str, str],
) -> pyarrow.Table:
    # run field mapping in the forward direction
    cols = table.column_names
    mapped_cols = [
        field_mapping[col] if col in field_mapping.keys() else col for col in cols
    ]
    table = table.rename_columns(mapped_cols)
    return table


def _run_dask_field_mapping(
    table: dd.DataFrame,
    field_mapping: Dict[str, str],
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
    table: Union[pyarrow.Table, pyarrow.RecordBatch],
    feature_view: "FeatureView",
    join_keys: Dict[str, ValueType],
) -> List[Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]]:
    # Avoid ChunkedArrays which guarantees `zero_copy_only` available.
    if isinstance(table, pyarrow.Table):
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
