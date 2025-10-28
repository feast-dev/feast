from typing import Literal, Optional, Sequence, Union

from feast import (
    BatchFeatureView,
    Entity,
    FeatureView,
    OnDemandFeatureView,
    StreamFeatureView,
)
from feast.infra.common.materialization_job import (
    MaterializationJobStatus,
    MaterializationTask,
)
from feast.infra.common.retrieval_task import HistoricalRetrievalTask
from feast.infra.compute_engines.backends.base import DataFrameBackend
from feast.infra.compute_engines.backends.factory import BackendFactory
from feast.infra.compute_engines.base import ComputeEngine
from feast.infra.compute_engines.dag.context import ExecutionContext
from feast.infra.compute_engines.local.feature_builder import LocalFeatureBuilder
from feast.infra.compute_engines.local.job import (
    LocalMaterializationJob,
    LocalRetrievalJob,
)
from feast.infra.registry.base_registry import BaseRegistry
from feast.repo_config import FeastConfigBaseModel


class LocalComputeEngineConfig(FeastConfigBaseModel):
    """Configuration for Local Compute Engine."""

    type: Literal["local"] = "local"
    """Local Compute Engine type selector"""

    backend: Optional[str] = None
    """Backend to use for DataFrame operations (e.g., 'pandas', 'polars')"""


class LocalComputeEngine(ComputeEngine):
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
        pass

    def teardown_infra(
        self,
        project: str,
        fvs: Sequence[Union[BatchFeatureView, StreamFeatureView, FeatureView]],
        entities: Sequence[Entity],
    ):
        pass

    def __init__(self, backend: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)
        self.backend_name = backend
        self._backend = BackendFactory.from_name(backend) if backend else None

    def _get_backend(self, context: ExecutionContext) -> DataFrameBackend:
        if self._backend:
            return self._backend
        backend = BackendFactory.infer_from_entity_df(context.entity_df)
        if backend is not None:
            return backend
        raise ValueError("Could not infer backend from context.entity_df")

    def _materialize_one(
        self, registry: BaseRegistry, task: MaterializationTask, **kwargs
    ) -> LocalMaterializationJob:
        job_id = f"{task.feature_view.name}-{task.start_time}-{task.end_time}"
        context = self.get_execution_context(registry, task)
        backend = self._get_backend(context)

        try:
            builder = LocalFeatureBuilder(registry, task, backend=backend)
            plan = builder.build()
            plan.execute(context)
            return LocalMaterializationJob(
                job_id=job_id,
                status=MaterializationJobStatus.SUCCEEDED,
            )

        except Exception as e:
            return LocalMaterializationJob(
                job_id=job_id,
                status=MaterializationJobStatus.ERROR,
                error=e,
            )

    def get_historical_features(
        self, registry: BaseRegistry, task: HistoricalRetrievalTask
    ) -> LocalRetrievalJob:
        context = self.get_execution_context(registry, task)
        backend = self._get_backend(context)

        try:
            builder = LocalFeatureBuilder(registry, task=task, backend=backend)
            plan = builder.build()
            return LocalRetrievalJob(
                plan=plan,
                context=context,
                full_feature_names=task.full_feature_name,
            )
        except Exception as e:
            return LocalRetrievalJob(
                plan=plan,
                context=context,
                full_feature_names=task.full_feature_name,
                error=e,
            )
