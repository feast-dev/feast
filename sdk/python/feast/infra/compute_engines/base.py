from abc import ABC
from typing import Union

import pyarrow as pa

from feast import RepoConfig
from feast.infra.common.materialization_job import (
    MaterializationJob,
    MaterializationTask,
)
from feast.infra.common.retrieval_task import HistoricalRetrievalTask
from feast.infra.compute_engines.dag.context import ColumnInfo, ExecutionContext
from feast.infra.offline_stores.offline_store import OfflineStore
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.registry.registry import Registry
from feast.utils import _get_column_names


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

    def get_execution_context(
        self,
        task: Union[MaterializationTask, HistoricalRetrievalTask],
    ) -> ExecutionContext:
        entity_defs = [
            self.registry.get_entity(name, task.project)
            for name in task.feature_view.entities
        ]
        entity_df = None
        if hasattr(task, "entity_df") and task.entity_df is not None:
            entity_df = task.entity_df

        column_info = self.get_column_info(task)
        return ExecutionContext(
            project=task.project,
            repo_config=self.repo_config,
            offline_store=self.offline_store,
            online_store=self.online_store,
            entity_defs=entity_defs,
            column_info=column_info,
            entity_df=entity_df,
        )

    def get_column_info(
        self,
        task: Union[MaterializationTask, HistoricalRetrievalTask],
    ) -> ColumnInfo:
        join_keys, feature_cols, ts_col, created_ts_col = _get_column_names(
            task.feature_view, self.registry.list_entities(task.project)
        )
        return ColumnInfo(
            join_keys=join_keys,
            feature_cols=feature_cols,
            ts_col=ts_col,
            created_ts_col=created_ts_col,
        )
