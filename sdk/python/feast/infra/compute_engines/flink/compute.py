from __future__ import annotations

import logging
from typing import Any, Dict, Literal, Optional, Sequence, Union

from feast import (
    BatchFeatureView,
    Entity,
    FeatureView,
    OnDemandFeatureView,
    StreamFeatureView,
)
from feast.infra.common.materialization_job import (
    MaterializationJob,
    MaterializationJobStatus,
    MaterializationTask,
)
from feast.infra.common.retrieval_task import HistoricalRetrievalTask
from feast.infra.compute_engines.base import ComputeEngine
from feast.infra.compute_engines.flink.feature_builder import FlinkFeatureBuilder
from feast.infra.compute_engines.flink.job import (
    FlinkDAGRetrievalJob,
    FlinkMaterializationJob,
)
from feast.infra.compute_engines.flink.utils import (
    cleanup_flink_temporary_views,
    create_flink_table_environment,
)
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.registry.base_registry import BaseRegistry
from feast.repo_config import FeastConfigBaseModel, RepoConfig

logger = logging.getLogger(__name__)


class FlinkComputeEngineConfig(FeastConfigBaseModel):
    """Configuration for the Apache Flink compute engine."""

    type: Literal["flink.engine"] = "flink.engine"
    """Flink compute engine type selector."""

    execution_mode: Literal["batch", "streaming"] = "batch"
    """PyFlink TableEnvironment execution mode."""

    parallelism: Optional[int] = None
    """Default Flink parallelism for jobs created by this engine."""

    table_config: Optional[Dict[str, str]] = None
    """Additional PyFlink table configuration entries."""

    pandas_split_num: int = 1
    """Number of PyFlink Arrow source splits for pandas entity DataFrames."""


class FlinkComputeEngine(ComputeEngine):
    def __init__(
        self,
        *,
        repo_config: RepoConfig,
        offline_store: OfflineStore,
        online_store: OnlineStore,
        table_environment: Optional[Any] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            repo_config=repo_config,
            offline_store=offline_store,
            online_store=online_store,
            **kwargs,
        )
        self.config = repo_config.batch_engine
        assert isinstance(self.config, FlinkComputeEngineConfig)
        self.table_env = table_environment or create_flink_table_environment(
            self.config
        )

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
    ) -> None:
        """Flink compute engine does not provision Feast-managed infrastructure."""
        pass

    def teardown_infra(
        self,
        project: str,
        fvs: Sequence[Union[BatchFeatureView, StreamFeatureView, FeatureView]],
        entities: Sequence[Entity],
    ) -> None:
        """Flink compute engine does not tear down Feast-managed infrastructure."""
        pass

    def _materialize_one(
        self, registry: BaseRegistry, task: MaterializationTask, **kwargs
    ) -> MaterializationJob:
        job_id = f"{task.feature_view.name}-{task.start_time}-{task.end_time}"
        context = self.get_execution_context(registry, task)

        try:
            builder = FlinkFeatureBuilder(
                registry=registry,
                table_env=self.table_env,
                task=task,
                split_num=self.config.pandas_split_num,
            )
            plan = builder.build()
            plan.execute(context)
            return FlinkMaterializationJob(
                job_id=job_id,
                status=MaterializationJobStatus.SUCCEEDED,
            )
        except Exception as exc:
            logger.error("Flink materialization failed for %s: %s", job_id, exc)
            return FlinkMaterializationJob(
                job_id=job_id,
                status=MaterializationJobStatus.ERROR,
                error=exc,
            )
        finally:
            cleanup_flink_temporary_views(self.table_env)

    def get_historical_features(
        self, registry: BaseRegistry, task: HistoricalRetrievalTask
    ) -> RetrievalJob:
        context = self.get_execution_context(registry, task)
        try:
            builder = FlinkFeatureBuilder(
                registry=registry,
                table_env=self.table_env,
                task=task,
                split_num=self.config.pandas_split_num,
            )
            plan = builder.build()
            return FlinkDAGRetrievalJob(
                plan=plan,
                context=context,
                table_env=self.table_env,
                full_feature_names=task.full_feature_name,
            )
        except Exception as exc:
            logger.error(
                "Flink historical retrieval setup failed for %s: %s",
                task.feature_view.name,
                exc,
            )
            return FlinkDAGRetrievalJob(
                plan=None,
                context=context,
                table_env=self.table_env,
                full_feature_names=task.full_feature_name,
                error=exc,
            )
