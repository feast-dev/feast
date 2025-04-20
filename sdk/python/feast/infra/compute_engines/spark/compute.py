from feast.infra.common.materialization_job import (
    MaterializationJob,
    MaterializationJobStatus,
    MaterializationTask,
)
from feast.infra.common.retrieval_task import HistoricalRetrievalTask
from feast.infra.compute_engines.base import ComputeEngine
from feast.infra.compute_engines.spark.feature_builder import SparkFeatureBuilder
from feast.infra.compute_engines.spark.job import SparkDAGRetrievalJob
from feast.infra.compute_engines.spark.utils import get_or_create_new_spark_session
from feast.infra.materialization.contrib.spark.spark_materialization_engine import (
    SparkMaterializationJob,
)
from feast.infra.offline_stores.offline_store import RetrievalJob


class SparkComputeEngine(ComputeEngine):
    def __init__(
        self,
        offline_store,
        online_store,
        registry,
        repo_config,
        **kwargs,
    ):
        super().__init__(
            offline_store=offline_store,
            online_store=online_store,
            registry=registry,
            repo_config=repo_config,
            **kwargs,
        )
        self.spark_session = get_or_create_new_spark_session()

    def materialize(self, task: MaterializationTask) -> MaterializationJob:
        job_id = f"{task.feature_view.name}-{task.start_time}-{task.end_time}"

        # ✅ 1. Build typed execution context
        context = self.get_execution_context(task)

        try:
            # ✅ 2. Construct Feature Builder and run it
            builder = SparkFeatureBuilder(
                spark_session=self.spark_session,
                task=task,
            )
            plan = builder.build()
            plan.execute(context)

            # ✅ 3. Report success
            return SparkMaterializationJob(
                job_id=job_id, status=MaterializationJobStatus.SUCCEEDED
            )

        except Exception as e:
            # 🛑 Handle failure
            return SparkMaterializationJob(
                job_id=job_id, status=MaterializationJobStatus.ERROR, error=e
            )

    def get_historical_features(self, task: HistoricalRetrievalTask) -> RetrievalJob:
        if isinstance(task.entity_df, str):
            raise NotImplementedError("SQL-based entity_df is not yet supported in DAG")

        # ✅ 1. Build typed execution context
        context = self.get_execution_context(task)

        try:
            # ✅ 2. Construct Feature Builder and run it
            builder = SparkFeatureBuilder(
                spark_session=self.spark_session,
                task=task,
            )
            plan = builder.build()

            return SparkDAGRetrievalJob(
                plan=plan,
                spark_session=self.spark_session,
                context=context,
                config=self.repo_config,
                full_feature_names=task.full_feature_name,
            )
        except Exception as e:
            # 🛑 Handle failure
            return SparkDAGRetrievalJob(
                plan=None,
                spark_session=self.spark_session,
                context=context,
                config=self.repo_config,
                full_feature_names=task.full_feature_name,
                error=e,
            )
