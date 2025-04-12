from abc import ABC
from dataclasses import dataclass
from datetime import datetime
from typing import Union

import pandas as pd
import pyarrow as pa

from feast import BatchFeatureView, RepoConfig, StreamFeatureView
from feast.infra.materialization.batch_materialization_engine import (
    MaterializationJob,
    MaterializationTask,
)
from feast.infra.offline_stores.offline_store import OfflineStore
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.registry.registry import Registry


@dataclass
class HistoricalRetrievalTask:
    entity_df: Union[pd.DataFrame, str]
    feature_view: Union[BatchFeatureView, StreamFeatureView]
    full_feature_name: bool
    registry: Registry
    config: RepoConfig
    start_time: datetime
    end_time: datetime


class ComputeEngine(ABC):
    """
    The interface that Feast uses to control the compute system that handles materialization and get_historical_features.
    Each engine must implement:
        - materialize(): to generate and persist features
        - get_historical_features(): to perform point-in-time correct joins
    Engines should use FeatureBuilder and DAGNode abstractions to build modular, pluggable workflows.
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

    def materialize(self, task: MaterializationTask) -> MaterializationJob:
        raise NotImplementedError

    def get_historical_features(self, task: HistoricalRetrievalTask) -> pa.Table:
        raise NotImplementedError
