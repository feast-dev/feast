from abc import ABC, abstractmethod
from typing import Union

import pyarrow as pa
from typing import Sequence, Union
from feast import RepoConfig
from feast.infra.common.materialization_job import (
    MaterializationJob,
    MaterializationTask,
)
from feast.entity import Entity
from feast.infra.common.retrieval_task import HistoricalRetrievalTask
from feast.infra.compute_engines.dag.context import ColumnInfo, ExecutionContext
from feast.infra.offline_stores.offline_store import OfflineStore
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.registry.registry import Registry
from feast.utils import _get_column_names
from feast.feature_view import FeatureView
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.stream_feature_view import StreamFeatureView
from feast.batch_feature_view import BatchFeatureView
from feast.infra.registry.base_registry import BaseRegistry


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
        repo_config: RepoConfig,
        offline_store: OfflineStore,
        online_store: OnlineStore,
        **kwargs,
    ):
        self.repo_config = repo_config
        self.offline_store = offline_store
        self.online_store = online_store

    @abstractmethod
    def update(
        self,
        project: str,
        views_to_delete: Sequence[
            Union[BatchFeatureView, StreamFeatureView, FeatureView]
        ],
        views_to_keep: Sequence[
            Union[BatchFeatureView, StreamFeatureView, FeatureView, OnDemandFeatureView]
        ],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
    ):
        """
        Prepares cloud resources required for batch materialization for the specified set of Feast objects.

        Args:
            project: Feast project to which the objects belong.
            views_to_delete: Feature views whose corresponding infrastructure should be deleted.
            views_to_keep: Feature views whose corresponding infrastructure should not be deleted, and
                may need to be updated.
            entities_to_delete: Entities whose corresponding infrastructure should be deleted.
            entities_to_keep: Entities whose corresponding infrastructure should not be deleted, and
                may need to be updated.
        """
        pass

    @abstractmethod
    def teardown_infra(
        self,
        project: str,
        fvs: Sequence[Union[BatchFeatureView, StreamFeatureView, FeatureView]],
        entities: Sequence[Entity],
    ):
        """
        Tears down all cloud resources used by the materialization engine for the specified set of Feast objects.

        Args:
            project: Feast project to which the objects belong.
            fvs: Feature views whose corresponding infrastructure should be deleted.
            entities: Entities whose corresponding infrastructure should be deleted.
        """
        pass

    def materialize(self,
                    registry: BaseRegistry,
                    task: MaterializationTask) -> MaterializationJob:
        raise NotImplementedError

    def get_historical_features(self,
                                registry: BaseRegistry,
                                task: HistoricalRetrievalTask) -> pa.Table:
        raise NotImplementedError

    def get_execution_context(
        self,
        registry: BaseRegistry,
        task: Union[MaterializationTask, HistoricalRetrievalTask],
    ) -> ExecutionContext:
        entity_defs = [
            registry.get_entity(name, task.project)
            for name in task.feature_view.entities
        ]
        entity_df = None
        if hasattr(task, "entity_df") and task.entity_df is not None:
            entity_df = task.entity_df

        column_info = self.get_column_info(registry, task)
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
        registry: BaseRegistry,
        task: Union[MaterializationTask, HistoricalRetrievalTask],
    ) -> ColumnInfo:
        join_keys, feature_cols, ts_col, created_ts_col = _get_column_names(
            task.feature_view, registry.list_entities(task.project)
        )
        return ColumnInfo(
            join_keys=join_keys,
            feature_cols=feature_cols,
            ts_col=ts_col,
            created_ts_col=created_ts_col,
        )
