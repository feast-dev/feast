import logging
from datetime import datetime
from typing import Sequence, Union

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
from feast.infra.compute_engines.ray.config import RayComputeEngineConfig
from feast.infra.compute_engines.ray.feature_builder import RayFeatureBuilder
from feast.infra.compute_engines.ray.job import (
    RayDAGRetrievalJob,
    RayMaterializationJob,
)
from feast.infra.compute_engines.ray.utils import write_to_online_store
from feast.infra.offline_stores.offline_store import RetrievalJob
from feast.infra.ray_initializer import (
    ensure_ray_initialized,
    get_ray_wrapper,
)
from feast.infra.registry.base_registry import BaseRegistry

logger = logging.getLogger(__name__)


class RayComputeEngine(ComputeEngine):
    """
    Ray-based compute engine for distributed feature computation.
    This engine uses Ray for distributed processing of features, enabling
    efficient point-in-time joins, aggregations, and transformations across
    large datasets.
    """

    def __init__(
        self,
        offline_store,
        online_store,
        repo_config,
        **kwargs,
    ):
        super().__init__(
            offline_store=offline_store,
            online_store=online_store,
            repo_config=repo_config,
            **kwargs,
        )
        self.config = repo_config.batch_engine
        assert isinstance(self.config, RayComputeEngineConfig)
        self._ensure_ray_initialized()

    def _ensure_ray_initialized(self):
        """Ensure Ray is initialized with proper configuration."""
        ensure_ray_initialized(self.config)

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
        """Ray compute engine doesn't require infrastructure updates."""
        pass

    def teardown_infra(
        self,
        project: str,
        fvs: Sequence[Union[BatchFeatureView, StreamFeatureView, FeatureView]],
        entities: Sequence[Entity],
    ):
        """Ray compute engine doesn't require infrastructure teardown."""
        pass

    def _materialize_one(
        self,
        registry: BaseRegistry,
        task: MaterializationTask,
        from_offline_store: bool = False,
        **kwargs,
    ) -> MaterializationJob:
        """Materialize features for a single feature view."""
        job_id = f"{task.feature_view.name}-{task.start_time}-{task.end_time}"

        if from_offline_store:
            logger.warning(
                "Materializing from offline store will be deprecated. "
                "Please use the new materialization API."
            )
            return self._materialize_from_offline_store(
                registry=registry,
                feature_view=task.feature_view,
                start_date=task.start_time,
                end_date=task.end_time,
                project=task.project,
            )

        try:
            # Build typed execution context
            context = self.get_execution_context(registry, task)

            # Construct Feature Builder and execute
            builder = RayFeatureBuilder(registry, task.feature_view, task, self.config)
            plan = builder.build()
            result = plan.execute(context)

            # Log execution results
            logger.info(f"Materialization completed for {task.feature_view.name}")

            return RayMaterializationJob(
                job_id=job_id,
                status=MaterializationJobStatus.SUCCEEDED,
                result=result,
            )

        except Exception as e:
            logger.error(f"Materialization failed for {task.feature_view.name}: {e}")
            return RayMaterializationJob(
                job_id=job_id,
                status=MaterializationJobStatus.ERROR,
                error=e,
            )

    def _materialize_from_offline_store(
        self,
        registry: BaseRegistry,
        feature_view: Union[BatchFeatureView, StreamFeatureView, FeatureView],
        start_date: datetime,
        end_date: datetime,
        project: str,
    ) -> MaterializationJob:
        """Legacy materialization method for backward compatibility."""
        from feast.utils import _get_column_names

        job_id = f"{feature_view.name}-{start_date}-{end_date}"

        try:
            # Get column information
            entities = [
                registry.get_entity(name, project) for name in feature_view.entities
            ]
            (
                join_key_columns,
                feature_name_columns,
                timestamp_field,
                created_timestamp_column,
            ) = _get_column_names(feature_view, entities)

            # Pull data from offline store
            retrieval_job = self.offline_store.pull_latest_from_table_or_query(
                config=self.repo_config,
                data_source=feature_view.batch_source,
                join_key_columns=join_key_columns,
                feature_name_columns=feature_name_columns,
                timestamp_field=timestamp_field,
                created_timestamp_column=created_timestamp_column,
                start_date=start_date,
                end_date=end_date,
            )

            # Convert to Arrow Table and write to online/offline stores
            arrow_table = retrieval_job.to_arrow()

            # Write to online store if enabled
            write_to_online_store(
                arrow_table=arrow_table,
                feature_view=feature_view,
                online_store=self.online_store,
                repo_config=self.repo_config,
            )

            # Write to offline store if enabled (this handles sink_source automatically for derived views)
            if getattr(feature_view, "offline", False):
                self.offline_store.offline_write_batch(
                    config=self.repo_config,
                    feature_view=feature_view,
                    table=arrow_table,
                    progress=lambda x: None,
                )

            # For derived views, also ensure data is written to sink_source if it exists
            # This is critical for feature view chaining to work properly
            sink_source = getattr(feature_view, "sink_source", None)
            if sink_source is not None:
                logger.debug(
                    f"Writing derived view {feature_view.name} to sink_source: {sink_source.path}"
                )

                # Write to sink_source using Ray data
                try:
                    ray_wrapper = get_ray_wrapper()
                    ray_dataset = ray_wrapper.from_arrow(arrow_table)
                    ray_dataset.write_parquet(sink_source.path)
                except Exception as e:
                    logger.error(
                        f"Failed to write to sink_source {sink_source.path}: {e}"
                    )
            return RayMaterializationJob(
                job_id=job_id,
                status=MaterializationJobStatus.SUCCEEDED,
            )

        except Exception as e:
            logger.error(f"Legacy materialization failed: {e}")
            return RayMaterializationJob(
                job_id=job_id,
                status=MaterializationJobStatus.ERROR,
                error=e,
            )

    def get_historical_features(
        self, registry: BaseRegistry, task: HistoricalRetrievalTask
    ) -> RetrievalJob:
        """Get historical features using Ray DAG execution."""
        if isinstance(task.entity_df, str):
            raise NotImplementedError(
                "SQL-based entity_df is not yet supported in Ray DAG"
            )

        try:
            # Build typed execution context
            context = self.get_execution_context(registry, task)

            # Construct Feature Builder and build execution plan
            builder = RayFeatureBuilder(registry, task.feature_view, task, self.config)
            plan = builder.build()

            return RayDAGRetrievalJob(
                plan=plan,
                context=context,
                config=self.repo_config,
                full_feature_names=task.full_feature_name,
                on_demand_feature_views=getattr(task, "on_demand_feature_views", None),
                feature_refs=getattr(task, "feature_refs", None),
            )

        except Exception as e:
            logger.error(f"Historical feature retrieval failed: {e}")
            return RayDAGRetrievalJob(
                plan=None,
                context=None,
                config=self.repo_config,
                full_feature_names=task.full_feature_name,
                on_demand_feature_views=getattr(task, "on_demand_feature_views", None),
                feature_refs=getattr(task, "feature_refs", None),
                error=e,
            )
