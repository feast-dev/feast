import logging
from datetime import datetime
from typing import Dict, Literal, Optional, Sequence, Union, cast

from pydantic import StrictStr
from pyspark.sql import SparkSession

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
from feast.infra.common.serde import SerializedArtifacts
from feast.infra.compute_engines.base import ComputeEngine
from feast.infra.compute_engines.spark.feature_builder import SparkFeatureBuilder
from feast.infra.compute_engines.spark.job import (
    SparkDAGRetrievalJob,
    SparkMaterializationJob,
)
from feast.infra.compute_engines.spark.utils import (
    get_or_create_new_spark_session,
    map_in_pandas,
)
from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
    SparkRetrievalJob,
)
from feast.infra.offline_stores.offline_store import RetrievalJob
from feast.infra.registry.base_registry import BaseRegistry
from feast.repo_config import FeastConfigBaseModel
from feast.utils import _get_column_names


class SparkComputeEngineConfig(FeastConfigBaseModel):
    type: Literal["spark.engine"] = "spark.engine"
    """ Spark Compute type selector"""

    spark_conf: Optional[Dict[str, str]] = None
    """ Configuration overlay for the spark session """

    staging_location: Optional[StrictStr] = None
    """ Remote path for batch materialization jobs"""

    region: Optional[StrictStr] = None
    """ AWS Region if applicable for s3-based staging locations"""

    partitions: int = 0
    """Number of partitions to use when writing data to online store. If 0, no repartitioning is done"""


class SparkComputeEngine(ComputeEngine):
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

    def _get_feature_view_spark_session(
        self, feature_view: Union[BatchFeatureView, StreamFeatureView, FeatureView]
    ) -> SparkSession:
        spark_conf = self._get_feature_view_engine_config(feature_view)
        return get_or_create_new_spark_session(spark_conf)

    def _materialize_one(
        self,
        registry: BaseRegistry,
        task: MaterializationTask,
        from_offline_store: bool = False,
        **kwargs,
    ) -> MaterializationJob:
        if from_offline_store:
            return self._materialize_from_offline_store(
                registry=registry,
                feature_view=task.feature_view,
                start_date=task.start_time,
                end_date=task.end_time,
                project=task.project,
            )

        job_id = f"{task.feature_view.name}-{task.start_time}-{task.end_time}"

        # ✅ 1. Build typed execution context
        context = self.get_execution_context(registry, task)

        spark_session = self._get_feature_view_spark_session(task.feature_view)

        try:
            # ✅ 2. Construct Feature Builder and run it
            builder = SparkFeatureBuilder(
                registry=registry,
                spark_session=spark_session,
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

    def _materialize_from_offline_store(
        self,
        registry: BaseRegistry,
        feature_view: Union[BatchFeatureView, StreamFeatureView, FeatureView],
        start_date: datetime,
        end_date: datetime,
        project: str,
    ):
        """
        Legacy materialization method for Spark Compute Engine. This method is used to materialize features from the
        offline store to the online store directly.
        """
        logging.warning(
            "Materializing from offline store will be deprecated in the future. Please use the new "
            "materialization API."
        )
        entities = []
        for entity_name in feature_view.entities:
            entities.append(registry.get_entity(entity_name, project))

        (
            join_key_columns,
            feature_name_columns,
            timestamp_field,
            created_timestamp_column,
        ) = _get_column_names(feature_view, entities)

        job_id = f"{feature_view.name}-{start_date}-{end_date}"

        try:
            offline_job = cast(
                SparkRetrievalJob,
                self.offline_store.pull_latest_from_table_or_query(
                    config=self.repo_config,
                    data_source=feature_view.batch_source,
                    join_key_columns=join_key_columns,
                    feature_name_columns=feature_name_columns,
                    timestamp_field=timestamp_field,
                    created_timestamp_column=created_timestamp_column,
                    start_date=start_date,
                    end_date=end_date,
                ),
            )

            serialized_artifacts = SerializedArtifacts.serialize(
                feature_view=feature_view, repo_config=self.repo_config
            )

            spark_df = offline_job.to_spark_df()
            if self.repo_config.batch_engine.partitions != 0:
                spark_df = spark_df.repartition(
                    self.repo_config.batch_engine.partitions
                )

            spark_df.mapInPandas(
                lambda x: map_in_pandas(x, serialized_artifacts), "status int"
            ).count()  # dummy action to force evaluation

            return SparkMaterializationJob(
                job_id=job_id, status=MaterializationJobStatus.SUCCEEDED
            )
        except BaseException as e:
            return SparkMaterializationJob(
                job_id=job_id, status=MaterializationJobStatus.ERROR, error=e
            )

    def get_historical_features(
        self, registry: BaseRegistry, task: HistoricalRetrievalTask
    ) -> RetrievalJob:
        if isinstance(task.entity_df, str):
            raise NotImplementedError("SQL-based entity_df is not yet supported in DAG")

        # ✅ 1. Build typed execution context
        context = self.get_execution_context(registry, task)

        spark_session = self._get_feature_view_spark_session(task.feature_view)

        try:
            # ✅ 2. Construct Feature Builder and run it
            builder = SparkFeatureBuilder(
                registry=registry,
                spark_session=spark_session,
                task=task,
            )
            plan = builder.build()

            return SparkDAGRetrievalJob(
                plan=plan,
                spark_session=spark_session,
                context=context,
                config=self.repo_config,
                full_feature_names=task.full_feature_name,
            )
        except Exception as e:
            # 🛑 Handle failure
            return SparkDAGRetrievalJob(
                plan=None,
                spark_session=spark_session,
                context=context,
                config=self.repo_config,
                full_feature_names=task.full_feature_name,
                error=e,
            )
