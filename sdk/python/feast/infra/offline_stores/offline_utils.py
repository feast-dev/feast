import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, KeysView, List, Literal, Optional, Set, Tuple, Union

import numpy as np
import pandas as pd
import pyarrow as pa
from jinja2 import BaseLoader, Environment
from pandas import Timestamp

from feast.data_source import DataSource
from feast.errors import (
    EntityTimestampInferenceException,
    FeastEntityDFMissingColumnsError,
)
from feast.feature_view import FeatureView
from feast.importer import import_class
from feast.infra.offline_stores.offline_store import OfflineStore
from feast.infra.registry.base_registry import BaseRegistry
from feast.repo_config import RepoConfig
from feast.type_map import feast_value_type_to_pa
from feast.utils import _get_requested_feature_views_to_features_dict, to_naive_utc

DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL = "event_timestamp"


def infer_event_timestamp_from_entity_df(entity_schema: Dict[str, np.dtype]) -> str:
    if DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL in entity_schema.keys():
        return DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL

    datetime_columns = [
        column
        for column, dtype in entity_schema.items()
        if pd.core.dtypes.common.is_datetime64_any_dtype(dtype)
    ]

    if len(datetime_columns) == 1:
        print(
            f"Using {datetime_columns[0]} as the event timestamp. To specify a column explicitly, please name it {DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL}."
        )
        return datetime_columns[0]
    else:
        raise EntityTimestampInferenceException(DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL)


def assert_expected_columns_in_entity_df(
    entity_schema: Dict[str, np.dtype],
    join_keys: Set[str],
    entity_df_event_timestamp_col: str,
):
    entity_columns = set(entity_schema.keys())
    expected_columns = {entity_df_event_timestamp_col}
    missing_keys = expected_columns - entity_columns

    if len(missing_keys) != 0:
        raise FeastEntityDFMissingColumnsError(expected_columns, missing_keys)


# TODO: Remove project and registry from the interface and call sites.
def get_expected_join_keys(
    project: str, feature_views: List[FeatureView], registry: BaseRegistry
) -> Set[str]:
    join_keys = set()
    for feature_view in feature_views:
        for entity_column in feature_view.entity_columns:
            join_key = feature_view.projection.join_key_map.get(
                entity_column.name, entity_column.name
            )
            join_keys.add(join_key)
    return join_keys


def get_entity_df_timestamp_bounds(
    entity_df: pd.DataFrame, event_timestamp_col: str
) -> Tuple[Timestamp, Timestamp]:
    event_timestamp_series = entity_df[event_timestamp_col]
    return event_timestamp_series.min(), event_timestamp_series.max()


@dataclass(frozen=True)
class FeatureViewQueryContext:
    """Context object used to template a BigQuery and Redshift point-in-time SQL query"""

    name: str
    ttl: int
    entities: List[str]
    features: List[str]  # feature reference format
    field_mapping: Dict[str, str]
    timestamp_field: str
    created_timestamp_column: Optional[str]
    table_subquery: str
    entity_selections: List[str]
    min_event_timestamp: Optional[str]
    max_event_timestamp: str
    date_partition_column: Optional[
        str
    ]  # this attribute is added because partition pruning affects Athena's query performance.


def get_feature_view_query_context(
    feature_refs: List[str],
    feature_views: List[FeatureView],
    registry: BaseRegistry,
    project: str,
    entity_df_timestamp_range: Tuple[datetime, datetime],
) -> List[FeatureViewQueryContext]:
    """
    Build a query context containing all information required to template a BigQuery and
    Redshift point-in-time SQL query
    """
    (
        feature_views_to_feature_map,
        on_demand_feature_views_to_features,
    ) = _get_requested_feature_views_to_features_dict(
        feature_refs, feature_views, registry.list_on_demand_feature_views(project)
    )

    query_context = []
    for feature_view, features in feature_views_to_feature_map.items():
        reverse_field_mapping = {
            v: k for k, v in feature_view.batch_source.field_mapping.items()
        }

        join_keys: List[str] = []
        entity_selections: List[str] = []
        for entity_column in feature_view.entity_columns:
            join_key = feature_view.projection.join_key_map.get(
                entity_column.name, entity_column.name
            )
            join_keys.append(join_key)
            entity_selections.append(
                f"{reverse_field_mapping.get(entity_column.name, entity_column.name)} "
                f"AS {join_key}"
            )

        if isinstance(feature_view.ttl, timedelta):
            ttl_seconds = int(feature_view.ttl.total_seconds())
        else:
            ttl_seconds = 0

        features = [reverse_field_mapping.get(feature, feature) for feature in features]
        timestamp_field = reverse_field_mapping.get(
            feature_view.batch_source.timestamp_field,
            feature_view.batch_source.timestamp_field,
        )
        created_timestamp_column = reverse_field_mapping.get(
            feature_view.batch_source.created_timestamp_column,
            feature_view.batch_source.created_timestamp_column,
        )

        date_partition_column = reverse_field_mapping.get(
            feature_view.batch_source.date_partition_column,
            feature_view.batch_source.date_partition_column,
        )

        max_event_timestamp = to_naive_utc(entity_df_timestamp_range[1]).isoformat()
        min_event_timestamp = None
        if feature_view.ttl:
            min_event_timestamp = to_naive_utc(
                entity_df_timestamp_range[0] - feature_view.ttl
            ).isoformat()

        context = FeatureViewQueryContext(
            name=feature_view.projection.name_to_use(),
            ttl=ttl_seconds,
            entities=join_keys,
            features=features,
            field_mapping=feature_view.batch_source.field_mapping,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            # TODO: Make created column optional and not hardcoded
            table_subquery=feature_view.batch_source.get_table_query_string(),
            entity_selections=entity_selections,
            min_event_timestamp=min_event_timestamp,
            max_event_timestamp=max_event_timestamp,
            date_partition_column=date_partition_column,
        )
        query_context.append(context)

    return query_context


def build_point_in_time_query(
    feature_view_query_contexts: List[FeatureViewQueryContext],
    left_table_query_string: str,
    entity_df_event_timestamp_col: str,
    entity_df_columns: KeysView[str],
    query_template: str,
    full_feature_names: bool = False,
) -> str:
    """Build point-in-time query between each feature view table and the entity dataframe for Bigquery and Redshift"""
    env = Environment(loader=BaseLoader())
    env.filters["backticks"] = enclose_in_backticks
    template = env.from_string(source=query_template)

    final_output_feature_names = list(entity_df_columns)
    final_output_feature_names.extend(
        [
            (
                f"{fv.name}__{fv.field_mapping.get(feature, feature)}"
                if full_feature_names
                else fv.field_mapping.get(feature, feature)
            )
            for fv in feature_view_query_contexts
            for feature in fv.features
        ]
    )

    # Add additional fields to dict
    template_context = {
        "left_table_query_string": left_table_query_string,
        "entity_df_event_timestamp_col": entity_df_event_timestamp_col,
        "unique_entity_keys": set(
            [entity for fv in feature_view_query_contexts for entity in fv.entities]
        ),
        "featureviews": [asdict(context) for context in feature_view_query_contexts],
        "full_feature_names": full_feature_names,
        "final_output_feature_names": final_output_feature_names,
    }

    query = template.render(template_context)
    return query


def get_temp_entity_table_name() -> str:
    """Returns a random table name for uploading the entity dataframe"""
    return "feast_entity_df_" + uuid.uuid4().hex


def get_offline_store_from_config(offline_store_config: Any) -> OfflineStore:
    """Creates an offline store corresponding to the given offline store config."""
    module_name = offline_store_config.__module__
    qualified_name = type(offline_store_config).__name__
    class_name = qualified_name.replace("Config", "")
    offline_store_class = import_class(module_name, class_name, "OfflineStore")
    return offline_store_class()


def get_pyarrow_schema_from_batch_source(
    config: RepoConfig, batch_source: DataSource, timestamp_unit: str = "us"
) -> Tuple[pa.Schema, List[str]]:
    """Returns the pyarrow schema and column names for the given batch source."""
    column_names_and_types = batch_source.get_table_column_names_and_types(config)

    pa_schema = []
    column_names = []
    for column_name, column_type in column_names_and_types:
        pa_schema.append(
            (
                column_name,
                feast_value_type_to_pa(
                    batch_source.source_datatype_to_feast_value_type()(column_type),
                    timestamp_unit=timestamp_unit,
                ),
            )
        )
        column_names.append(column_name)

    return pa.schema(pa_schema), column_names


def enclose_in_backticks(value):
    # Check if the input is a list
    if isinstance(value, list):
        return [f"`{v}`" for v in value]
    else:
        return f"`{value}`"


def get_timestamp_filter_sql(
    start_date: Optional[Union[datetime, str]] = None,
    end_date: Optional[Union[datetime, str]] = None,
    timestamp_field: Optional[str] = DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL,
    date_partition_column: Optional[str] = None,
    tz: Optional[timezone] = None,
    cast_style: Literal[
        "timestamp", "timestamp_func", "timestamptz", "raw"
    ] = "timestamp",
    date_time_separator: str = "T",
    quote_fields: bool = True,
) -> str:
    """
    Returns SQL filter condition (no WHERE) with flexible timestamp casting.

    Args:
        start_date: datetime or ISO8601 strings
        end_date: datetime or ISO8601 strings
        timestamp_field: main timestamp column
        date_partition_column: optional partition column (for pruning)
        tz: optional timezone for datetime inputs
        cast_style: one of:
            - "timestamp": TIMESTAMP '...'                  → Common Sql engine Snowflake, Redshift etc.
            - "timestamp_func": TIMESTAMP('...')         → BigQuery, Couchbase etc.
            - "timestamptz": '...'::timestamptz          → PostgreSQL
            - "raw": '...'                               → no cast, string only
        date_time_separator: separator for datetime strings (default is "T")
            (e.g. "2023-10-01T00:00:00" or "2023-10-01 00:00:00")
        quote_fields: whether to quote the timestamp and partition column names

    Returns:
        SQL filter string without WHERE
    """

    def quote_column_if_needed(column: Optional[str]) -> Optional[str]:
        if not column or not quote_fields:
            return column
        return f'"{column}"'

    def format_casted_ts(val: Union[str, datetime]) -> str:
        if isinstance(val, datetime):
            if tz:
                val = val.astimezone(tz)
            val_str = val.isoformat(sep=date_time_separator)
        else:
            val_str = val

        if cast_style == "timestamp":
            return f"TIMESTAMP '{val_str}'"
        elif cast_style == "timestamp_func":
            return f"TIMESTAMP('{val_str}')"
        elif cast_style == "timestamptz":
            return f"'{val_str}'::{cast_style}"
        else:
            return f"'{val_str}'"

    def format_date(val: Union[str, datetime]) -> str:
        if isinstance(val, datetime):
            if tz:
                val = val.astimezone(tz)
            return val.strftime("%Y-%m-%d")
        return val

    ts_field = quote_column_if_needed(timestamp_field)
    dp_field = quote_column_if_needed(date_partition_column)

    filters = []

    # Timestamp filters
    if start_date and end_date:
        filters.append(
            f"{ts_field} BETWEEN {format_casted_ts(start_date)} AND {format_casted_ts(end_date)}"
        )
    elif start_date:
        filters.append(f"{ts_field} >= {format_casted_ts(start_date)}")
    elif end_date:
        filters.append(f"{ts_field} <= {format_casted_ts(end_date)}")

    # Partition pruning
    if date_partition_column:
        if start_date:
            filters.append(f"{dp_field} >= '{format_date(start_date)}'")
        if end_date:
            filters.append(f"{dp_field} <= '{format_date(end_date)}'")

    return " AND ".join(filters) if filters else ""
