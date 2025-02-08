from datetime import datetime
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Union

import pandas as pd
from django.db.models import Q
from typeguard import typechecked

from feast.data_source import DataSource
from feast.feature_view import FeatureView
from feast.infra.offline_stores.contrib.django_offline_store.django_source import (
    DjangoSource,
)
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    ValidationReference,
)
from feast.repo_config import RepoConfig


class DjangoOfflineStoreConfig(RepoConfig):
    """Offline store config for Django"""

    type: Literal["django"] = "django"
    connection_string: Optional[str] = None


@typechecked
class DjangoOfflineStore(OfflineStore):
    """Django offline store implementation"""

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
        assert isinstance(config.offline_store, DjangoOfflineStoreConfig)
        assert isinstance(data_source, DjangoSource)

        filters = Q()
        if timestamp_field:
            filters &= Q(**{f"{timestamp_field}__gte": start_date})
            filters &= Q(**{f"{timestamp_field}__lte": end_date})

        queryset = data_source._model.objects.filter(filters)
        df = pd.DataFrame.from_records(queryset.values())

        return DjangoRetrievalJob(df)

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: Any,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, DjangoOfflineStoreConfig)
        assert isinstance(entity_df, pd.DataFrame)

        entity_df_event_timestamp_col = (
            DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL
            if DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL in entity_df.columns
            else None
        )

        entity_df_event_timestamp_range = _get_entity_df_event_timestamp_range(
            entity_df, entity_df_event_timestamp_col
        )

        all_feature_refs = _get_requested_feature_views_to_features_dict(
            feature_refs, feature_views
        )

        job = _get_historical_features_from_feature_views(
            config=config,
            feature_views=feature_views,
            feature_refs=all_feature_refs,
            entity_df=entity_df,
            entity_df_event_timestamp_range=entity_df_event_timestamp_range,
            full_feature_names=full_feature_names,
        )
        return job


class DjangoRetrievalJob(RetrievalJob):
    """Django retrieval job"""

    def __init__(self, evaluation_function: Union[pd.DataFrame, Callable]):
        """Initialize DjangoRetrievalJob"""
        self.evaluation_function = evaluation_function

    def to_df(
        self, validation_reference: Optional[ValidationReference] = None, timeout: Optional[int] = None
    ) -> pd.DataFrame:
        if isinstance(self.evaluation_function, pd.DataFrame):
            return self.evaluation_function
        return self.evaluation_function()


DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL = "event_timestamp"


def _get_entity_df_event_timestamp_range(
    entity_df: pd.DataFrame, timestamp_field: Optional[str]
) -> Tuple[datetime, datetime]:
    if timestamp_field and timestamp_field in entity_df.columns:
        timestamps = entity_df[timestamp_field]
        return timestamps.min(), timestamps.max()
    return datetime.min, datetime.max


def _get_requested_feature_views_to_features_dict(
    feature_refs: List[str], feature_views: List[FeatureView]
) -> Dict[str, List[str]]:
    feature_views_to_feature_map: Dict[str, List[str]] = {}
    for ref in feature_refs:
        view_name = ref.split(":")[0]
        feature = ref.split(":")[1]
        if view_name not in feature_views_to_feature_map:
            feature_views_to_feature_map[view_name] = []
        feature_views_to_feature_map[view_name].append(feature)
    return feature_views_to_feature_map


def _get_historical_features_from_feature_views(
    config: RepoConfig,
    feature_views: List[FeatureView],
    feature_refs: Dict[str, List[str]],
    entity_df: pd.DataFrame,
    entity_df_event_timestamp_range: Tuple[datetime, datetime],
    full_feature_names: bool = False,
) -> RetrievalJob:
    feature_views_to_features = {
        view.name: feature_refs.get(view.name, [])
        for view in feature_views
        if view.name in feature_refs
    }

    all_feature_data = []
    for feature_view in feature_views:
        if feature_view.name not in feature_views_to_features:
            continue

        source = feature_view.batch_source
        assert isinstance(source, DjangoSource)

        start_date, end_date = entity_df_event_timestamp_range
        job = DjangoOfflineStore.pull_latest_from_table_or_query(
            config=config,
            data_source=source,
            join_key_columns=feature_view.entities,
            feature_name_columns=feature_views_to_features[feature_view.name],
            timestamp_field=source.timestamp_field,
            created_timestamp_column=source.created_timestamp_column,
            start_date=start_date,
            end_date=end_date,
        )
        feature_data = job.to_df()

        if full_feature_names:
            feature_data.columns = [
                f"{feature_view.name}__{col}" for col in feature_data.columns
            ]

        all_feature_data.append(feature_data)

    if not all_feature_data:
        return DjangoRetrievalJob(pd.DataFrame())

    df = pd.concat(all_feature_data, axis=1)
    return DjangoRetrievalJob(df)
