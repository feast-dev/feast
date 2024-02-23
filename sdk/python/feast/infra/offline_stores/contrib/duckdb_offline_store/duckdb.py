from typing import List, Union

import ibis
import pandas as pd
from pydantic import StrictStr

from feast.feature_view import FeatureView
from feast.infra.offline_stores.contrib.ibis_offline_store.ibis import IbisOfflineStore
from feast.infra.offline_stores.offline_store import RetrievalJob
from feast.infra.registry.base_registry import BaseRegistry
from feast.repo_config import FeastConfigBaseModel, RepoConfig


class DuckDBOfflineStoreConfig(FeastConfigBaseModel):
    type: StrictStr = "duckdb"
    # """ Offline store type selector"""


class DuckDBOfflineStore(IbisOfflineStore):
    @staticmethod
    def setup_ibis_backend():
        ibis.set_backend("duckdb")

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
        DuckDBOfflineStore.setup_ibis_backend()

        return IbisOfflineStore.get_historical_features(
            config=config,
            feature_views=feature_views,
            feature_refs=feature_refs,
            entity_df=entity_df,
            registry=registry,
            project=project,
            full_feature_names=full_feature_names,
        )
