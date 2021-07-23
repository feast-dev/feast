import importlib
import uuid
from dataclasses import asdict, dataclass
from datetime import timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
from jinja2 import BaseLoader, Environment
from pandas import Timestamp

import feast
from feast.errors import (
    EntityTimestampInferenceException,
    FeastClassImportError,
    FeastEntityDFMissingColumnsError,
    FeastModuleImportError,
)
from feast.infra.offline_stores.offline_store import OfflineStore
from feast.infra.provider import _get_requested_feature_views_to_features_dict
from feast.registry import Registry

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
    expected_columns = join_keys | {entity_df_event_timestamp_col}
    missing_keys = expected_columns - entity_columns

    if len(missing_keys) != 0:
        raise FeastEntityDFMissingColumnsError(expected_columns, missing_keys)


def get_expected_join_keys(
    project: str, feature_views: List["feast.FeatureView"], registry: Registry
) -> Set[str]:
    join_keys = set()
    for feature_view in feature_views:
        entities = feature_view.entities
        for entity_name in entities:
            entity = registry.get_entity(entity_name, project)
            join_keys.add(entity.join_key)
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
    event_timestamp_column: str
    created_timestamp_column: Optional[str]
    table_subquery: str
    entity_selections: List[str]


def get_feature_view_query_context(
    feature_refs: List[str],
    feature_views: List["feast.FeatureView"],
    registry: Registry,
    project: str,
) -> List[FeatureViewQueryContext]:
    """Build a query context containing all information required to template a BigQuery and Redshift point-in-time SQL query"""

    feature_views_to_feature_map = _get_requested_feature_views_to_features_dict(
        feature_refs, feature_views
    )

    query_context = []
    for feature_view, features in feature_views_to_feature_map.items():
        join_keys = []
        entity_selections = []
        reverse_field_mapping = {
            v: k for k, v in feature_view.input.field_mapping.items()
        }
        for entity_name in feature_view.entities:
            entity = registry.get_entity(entity_name, project)
            join_keys.append(entity.join_key)
            join_key_column = reverse_field_mapping.get(
                entity.join_key, entity.join_key
            )
            entity_selections.append(f"{join_key_column} AS {entity.join_key}")

        if isinstance(feature_view.ttl, timedelta):
            ttl_seconds = int(feature_view.ttl.total_seconds())
        else:
            ttl_seconds = 0

        event_timestamp_column = feature_view.input.event_timestamp_column
        created_timestamp_column = feature_view.input.created_timestamp_column

        context = FeatureViewQueryContext(
            name=feature_view.name,
            ttl=ttl_seconds,
            entities=join_keys,
            features=features,
            event_timestamp_column=reverse_field_mapping.get(
                event_timestamp_column, event_timestamp_column
            ),
            created_timestamp_column=reverse_field_mapping.get(
                created_timestamp_column, created_timestamp_column
            ),
            # TODO: Make created column optional and not hardcoded
            table_subquery=feature_view.input.get_table_query_string(),
            entity_selections=entity_selections,
        )
        query_context.append(context)
    return query_context


def build_point_in_time_query(
    feature_view_query_contexts: List[FeatureViewQueryContext],
    left_table_query_string: str,
    entity_df_event_timestamp_col: str,
    query_template: str,
    full_feature_names: bool = False,
):
    """Build point-in-time query between each feature view table and the entity dataframe for Bigquery and Redshift"""
    template = Environment(loader=BaseLoader()).from_string(source=query_template)

    # Add additional fields to dict
    template_context = {
        "left_table_query_string": left_table_query_string,
        "entity_df_event_timestamp_col": entity_df_event_timestamp_col,
        "unique_entity_keys": set(
            [entity for fv in feature_view_query_contexts for entity in fv.entities]
        ),
        "featureviews": [asdict(context) for context in feature_view_query_contexts],
        "full_feature_names": full_feature_names,
    }

    query = template.render(template_context)
    return query


def get_temp_entity_table_name() -> str:
    """Returns a random table name for uploading the entity dataframe"""
    return "feast_entity_df_" + uuid.uuid4().hex


def get_offline_store_from_config(offline_store_config: Any,) -> OfflineStore:
    """Get the offline store from offline store config"""

    module_name = offline_store_config.__module__
    qualified_name = type(offline_store_config).__name__
    store_class_name = qualified_name.replace("Config", "")
    try:
        module = importlib.import_module(module_name)
    except Exception as e:
        # The original exception can be anything - either module not found,
        # or any other kind of error happening during the module import time.
        # So we should include the original error as well in the stack trace.
        raise FeastModuleImportError(module_name, "OfflineStore") from e

    # Try getting the provider class definition
    try:
        offline_store_class = getattr(module, store_class_name)
    except AttributeError:
        # This can only be one type of error, when class_name attribute does not exist in the module
        # So we don't have to include the original exception here
        raise FeastClassImportError(
            module_name, store_class_name, class_type="OfflineStore"
        ) from None
    return offline_store_class()
