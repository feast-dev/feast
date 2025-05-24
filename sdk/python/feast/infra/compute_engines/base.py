from abc import ABC, abstractmethod
from typing import List, Optional, Sequence, Union

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
from feast.infra.compute_engines.dag.context import ColumnInfo, ExecutionContext
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.registry.base_registry import BaseRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.stream_feature_view import StreamFeatureView
from feast.utils import _get_column_names


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
        entities = []
        for entity_name in task.feature_view.entities:
            entities.append(registry.get_entity(entity_name, task.project))

        join_keys, feature_cols, ts_col, created_ts_col = _get_column_names(
            task.feature_view, entities
        )
        field_mapping = self.get_field_mapping(task.feature_view)

        return ColumnInfo(
            join_keys=join_keys,
            feature_cols=feature_cols,
            ts_col=ts_col,
            created_ts_col=created_ts_col,
            field_mapping=field_mapping,
        )

    def get_field_mapping(
        self, feature_view: Union[BatchFeatureView, StreamFeatureView, FeatureView]
    ) -> Optional[dict]:
        """
        Get the field mapping for a feature view.
        Args:
            feature_view: The feature view to get the field mapping for.

        Returns:
            A dictionary mapping field names to column names.
        """
        if feature_view.stream_source:
            return feature_view.stream_source.field_mapping
        if feature_view.batch_source:
            return feature_view.batch_source.field_mapping
        return None
