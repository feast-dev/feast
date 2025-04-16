from typing import Optional

from feast.infra.common.materialization_job import (
    MaterializationJobStatus,
    MaterializationTask,
)
from feast.infra.common.retrieval_task import HistoricalRetrievalTask
from feast.infra.compute_engines.base import ComputeEngine
from feast.infra.compute_engines.dag.context import ExecutionContext
from feast.infra.compute_engines.local.backends.base import DataFrameBackend
from feast.infra.compute_engines.local.backends.factory import BackendFactory
from feast.infra.compute_engines.local.feature_builder import LocalFeatureBuilder
from feast.infra.compute_engines.local.job import LocalRetrievalJob
from feast.infra.materialization.local_engine import LocalMaterializationJob


class LocalComputeEngine(ComputeEngine):
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

    def materialize(self, task: MaterializationTask) -> LocalMaterializationJob:
        job_id = f"{task.feature_view.name}-{task.start_time}-{task.end_time}"
        context = self.get_execution_context(task)
        backend = self._get_backend(context)

        try:
            builder = LocalFeatureBuilder(task, backend=backend)
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
        self, task: HistoricalRetrievalTask
    ) -> LocalRetrievalJob:
        context = self.get_execution_context(task)
        backend = self._get_backend(context)

        try:
            builder = LocalFeatureBuilder(task=task, backend=backend)
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
