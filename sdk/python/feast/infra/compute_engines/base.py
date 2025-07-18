from abc import ABC, abstractmethod
from typing import List, Sequence, Union

import pyarrow as pa

from feast import RepoConfig
from feast.batch_feature_view import BatchFeatureView
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.infra.common.materialization_job import (
    MaterializationJob,
    MaterializationTask,
)
from feast.infra.common.retrieval_task import HistoricalRetrievalTask
from feast.infra.compute_engines.dag.context import ExecutionContext
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.registry.base_registry import BaseRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.stream_feature_view import StreamFeatureView


class ComputeEngine(ABC):
    """
    The interface that Feast uses to control to compute system that handles materialization and get_historical_features.
    Each engine must implement:
        - materialize(): to generate and persist features
        - get_historical_features(): to perform historical retrieval of features
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

    def materialize(
        self,
        registry: BaseRegistry,
        tasks: Union[MaterializationTask, List[MaterializationTask]],
        **kwargs,
    ) -> List[MaterializationJob]:
        if isinstance(tasks, MaterializationTask):
            tasks = [tasks]
        return [self._materialize_one(registry, task, **kwargs) for task in tasks]

    def _materialize_one(
        self,
        registry: BaseRegistry,
        task: MaterializationTask,
        **kwargs,
    ) -> MaterializationJob:
        raise NotImplementedError(
            "Materialization is not implemented for this compute engine."
        )

    def get_historical_features(
        self, registry: BaseRegistry, task: HistoricalRetrievalTask
    ) -> Union[RetrievalJob, pa.Table]:
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

        return ExecutionContext(
            project=task.project,
            repo_config=self.repo_config,
            offline_store=self.offline_store,
            online_store=self.online_store,
            entity_defs=entity_defs,
            entity_df=entity_df,
        )

    def _get_feature_view_engine_config(
        self, feature_view: Union[BatchFeatureView, StreamFeatureView, FeatureView]
    ) -> dict:
        """
        Merge repo-level default batch engine config with runtime engine overrides defined in the feature view.

        Priority:
        1. Repo config (`self.repo_config.batch_engine_config`) - baseline
        2. FeatureView overrides (`batch_engine` for BatchFeatureView, `stream_engine` for StreamFeatureView`) - highest priority

        Args:
            feature_view: A BatchFeatureView or StreamFeatureView.

        Returns:
            dict: The merged engine configuration.
        """
        default_conf = self.repo_config.batch_engine_config or {}

        runtime_conf = None
        if isinstance(feature_view, BatchFeatureView):
            runtime_conf = feature_view.batch_engine
        elif isinstance(feature_view, StreamFeatureView):
            runtime_conf = feature_view.stream_engine

        if runtime_conf is not None and not isinstance(runtime_conf, dict):
            raise TypeError(
                f"Engine config for {feature_view.name} must be a dict, got {type(runtime_conf)}."
            )

        return {**default_conf, **runtime_conf} if runtime_conf else dict(default_conf)
