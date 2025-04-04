from abc import ABC
from dataclasses import dataclass
from typing import Union, List

import pandas as pd
import pyarrow as pa

from feast import RepoConfig, BatchFeatureView, StreamFeatureView
from feast.infra.materialization.batch_materialization_engine import MaterializationTask, MaterializationJob
from feast.infra.offline_stores.offline_store import OfflineStore
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.registry.registry import Registry


@dataclass
class HistoricalRetrievalTask:
    entity_df: Union[pd.DataFrame, str]
    feature_views: List[Union[BatchFeatureView, StreamFeatureView]]
    full_feature_names: bool
    registry: Registry
    config: RepoConfig


class ComputeEngine(ABC):
    """
    The interface that Feast uses to control the compute system that handles materialization and get_historical_features.
    """

    def __init__(
            self,
            *,
            registry: Registry,
            repo_config: RepoConfig,
            offline_store: OfflineStore,
            online_store: OnlineStore,
            **kwargs,
    ):
        self.registry = registry
        self.repo_config = repo_config
        self.offline_store = offline_store
        self.online_store = online_store

    def materialize(self,
                    task: MaterializationTask) -> MaterializationJob:
        raise NotImplementedError

    def get_historical_features(self,
                                task: HistoricalRetrievalTask) -> pa.Table:
        raise NotImplementedError
