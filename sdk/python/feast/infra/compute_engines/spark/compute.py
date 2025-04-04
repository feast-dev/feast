import pyarrow as pa
from feast.infra.compute_engines.base import ComputeEngine, HistoricalRetrievalTask
from feast.infra.compute_engines.spark.spark_dag_builder import SparkDAGBuilder
from feast.infra.materialization.batch_materialization_engine import MaterializationTask, MaterializationJob, \
    MaterializationJobStatus
from feast.infra.materialization.contrib.spark.spark_materialization_engine import SparkMaterializationJob
from feast.infra.compute_engines.dag.model import ExecutionContext


class SparkComputeEngine(ComputeEngine):
    def materialize(self, task: MaterializationTask) -> MaterializationJob:
        job_id = f"{task.feature_view.name}-{task.start_time}-{task.end_time}"

        try:
            # ✅ 1. Build typed execution context
            entities = []
            for entity_name in task.feature_view.entities:
                entities.append(self.registry.get_entity(entity_name, task.project))

            context = ExecutionContext(
                project=task.project,
                repo_config=self.repo_config,
                offline_store=self.offline_store,
                online_store=self.online_store,
                entity_defs=entities
            )

            # ✅ 2. Construct DAG and run it
            builder = SparkDAGBuilder(
                feature_view=task.feature_view,
                task=task,
            )
            plan = builder.build()
            plan.execute(context)

            # ✅ 3. Report success
            return SparkMaterializationJob(
                job_id=job_id,
                status=MaterializationJobStatus.SUCCEEDED
            )

        except Exception as e:
            # 🛑 Handle failure
            return SparkMaterializationJob(
                job_id=job_id,
                status=MaterializationJobStatus.ERROR,
                error=e
            )

    def get_historical_features(self, task: HistoricalRetrievalTask) -> pa.Table:
        # ✅ 1. Validate input
        assert len(task.feature_views) == 1, "Multi-view support not yet implemented"
        feature_view = task.feature_views[0]

        if isinstance(task.entity_df, str):
            raise NotImplementedError("SQL-based entity_df is not yet supported in DAG")

        # ✅ 2. Build typed execution context
        entity_defs = [
            task.registry.get_entity(name, task.config.project)
            for name in feature_view.entities
        ]

        context = ExecutionContext(
            project=task.config.project,
            repo_config=task.config,
            offline_store=self.offline_store,
            online_store=self.online_store,
            entity_defs=entity_defs,
            entity_df=task.entity_df,
        )

        # ✅ 3. Construct and execute DAG
        builder = SparkDAGBuilder(feature_view=feature_view, task=task)
        plan = builder.build()

        result = plan.execute(context=context)
        spark_df = result.data  # should be a Spark DataFrame

        # ✅ 4. Return as Arrow
        return spark_df.toPandas().to_arrow()
