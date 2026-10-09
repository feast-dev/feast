from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Literal, Optional, Sequence, Union

from pydantic import ConfigDict, Field, StrictStr
from trino.exceptions import TrinoConnectionError

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
from feast.infra.compute_engines.trino.feature_builder import TrinoFeatureBuilder
from feast.infra.compute_engines.trino.job import (
    TrinoDAGRetrievalJob,
    TrinoMaterializationJob,
)
from feast.infra.compute_engines.trino.utils import (
    get_or_create_trino_client,
    stream_trino_arrow_batches,
    write_arrow_batches_to_online_store,
)
from feast.infra.offline_stores.contrib.trino_offline_store.trino import (
    AuthConfig,
    TrinoOfflineStoreConfig,
)
from feast.infra.offline_stores.contrib.trino_offline_store.trino_queries import Trino
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.registry.base_registry import BaseRegistry
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.utils import _get_column_names

logger = logging.getLogger(__name__)


class TrinoComputeEngineConfig(FeastConfigBaseModel):
    """Configuration for Trino Compute Engine."""

    type: Literal["trino.engine", "trino"] = "trino.engine"
    """ Compute engine type selector """

    host: Optional[StrictStr] = None
    """ Host of the Trino coordinator """

    port: Optional[int] = 8080
    """ Port of the Trino coordinator """

    catalog: Optional[StrictStr] = None
    """ Target Trino catalog (e.g. iceberg, hive, delta, memory) """

    dataset: Optional[StrictStr] = "feast"
    """ Schema/dataset name for temporary staging tables """

    user: Optional[StrictStr] = None
    """ Username for connecting to Trino """

    source: Optional[StrictStr] = "trino-python-client"
    """ Application source identifier for Trino query tracking """

    http_scheme: Literal["http", "https"] = Field(default="http", alias="http-scheme")
    """ Protocol scheme (http or https) """

    verify: bool = True
    """ Whether to verify SSL certificates """

    extra_credential: Optional[StrictStr] = None
    """ Optional extra credential header passed to Trino """

    connector: Optional[Dict[str, str]] = None
    """ Connector arguments for Trino offline store table operations """

    auth: Optional[AuthConfig] = None
    """ Authentication mechanism (kerberos, basic, oauth2) """

    batch_size: int = 10000
    """ Streaming batch chunk size for PyArrow record batches """

    write_concurrency: int = 4
    """ Number of concurrent threads used when writing batches to online store """

    offline_write_mode: Literal["append", "overwrite", "merge"] = "append"
    """ Write mode for offline feature view materialization ('append', 'overwrite', or 'merge') """

    model_config = ConfigDict(populate_by_name=True, extra="allow")


class TrinoComputeEngine(ComputeEngine):
    """Distributed Trino SQL compute engine for Feast materialization and retrieval."""

    def __init__(
        self,
        *,
        repo_config: RepoConfig,
        offline_store: OfflineStore,
        online_store: OnlineStore,
        **kwargs: Any,
    ):
        super().__init__(
            repo_config=repo_config,
            offline_store=offline_store,
            online_store=online_store,
            **kwargs,
        )

        batch_engine_config = repo_config.batch_engine
        if isinstance(batch_engine_config, TrinoComputeEngineConfig):
            self.config = batch_engine_config
        elif isinstance(batch_engine_config, dict):
            self.config = TrinoComputeEngineConfig(**batch_engine_config)
        elif isinstance(batch_engine_config, str):
            self.config = TrinoComputeEngineConfig()
        else:
            self.config = TrinoComputeEngineConfig()

        # Automatic fallback: Inherit connection details from Trino offline store if unspecified
        if (
            isinstance(repo_config.offline_store, TrinoOfflineStoreConfig)
            and self.config.host is None
        ):
            offline_conf = repo_config.offline_store
            self.config.host = offline_conf.host
            self.config.port = offline_conf.port
            self.config.catalog = offline_conf.catalog
            self.config.user = offline_conf.user
            self.config.source = offline_conf.source
            self.config.http_scheme = offline_conf.http_scheme
            self.config.verify = offline_conf.verify
            self.config.extra_credential = offline_conf.extra_credential
            self.config.connector = offline_conf.connector
            self.config.dataset = offline_conf.dataset
            self.config.auth = offline_conf.auth

        self.client: Optional[Trino] = None
        if self.config.host and self.config.catalog and self.config.user:
            self.client = get_or_create_trino_client(self.config)

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
        """Trino compute engine does not require persistent infrastructure updates."""
        logger.debug("Trino compute engine does not require updating")
        pass

    def teardown_infra(
        self,
        project: str,
        fvs: Sequence[Union[BatchFeatureView, StreamFeatureView, FeatureView]],
        entities: Sequence[Entity],
    ) -> None:
        """Trino compute engine does not require infrastructure teardown."""
        logger.debug("Trino compute engine does not require teardown")
        pass

    def _materialize_one(
        self,
        registry: BaseRegistry,
        task: MaterializationTask,
        from_offline_store: bool = False,
        **kwargs: Any,
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
        context = self.get_execution_context(registry, task)

        try:
            if self.client is None:
                raise RuntimeError(
                    "Trino client is not configured. Set host, catalog, and user "
                    "in batch_engine config or use a TrinoOfflineStoreConfig."
                )
            builder = TrinoFeatureBuilder(
                registry=registry,
                client=self.client,
                task=task,
            )
            plan = builder.build()
            plan.execute(context)

            return TrinoMaterializationJob(
                job_id=job_id, status=MaterializationJobStatus.SUCCEEDED
            )
        except TrinoConnectionError as ce:
            logger.error("Connection to Trino failed during materialization: %s", ce)
            return TrinoMaterializationJob(
                job_id=job_id, status=MaterializationJobStatus.ERROR, error=ce
            )
        except Exception as e:
            logger.debug("Materialization failed for %s: %s", task.feature_view.name, e)
            return TrinoMaterializationJob(
                job_id=job_id, status=MaterializationJobStatus.ERROR, error=e
            )

    def _materialize_from_offline_store(
        self,
        registry: BaseRegistry,
        feature_view: Union[BatchFeatureView, StreamFeatureView, FeatureView],
        start_date: datetime,
        end_date: datetime,
        project: str,
    ) -> MaterializationJob:
        """Legacy materialization fallback method."""
        logger.warning(
            "Materializing from offline store will be deprecated in the future. "
            "Please use the unified DAG materialization engine."
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
            offline_job = self.offline_store.pull_latest_from_table_or_query(
                config=self.repo_config,
                data_source=feature_view.batch_source,  # type: ignore[arg-type]
                join_key_columns=join_key_columns,
                feature_name_columns=feature_name_columns,
                timestamp_field=timestamp_field,
                created_timestamp_column=created_timestamp_column,
                start_date=start_date,
                end_date=end_date,
            )

            if not self.client:
                raise RuntimeError("Trino client required for materialization.")

            batches = stream_trino_arrow_batches(
                client=self.client,
                query_text=offline_job.to_sql(),
                batch_size=self.config.batch_size,
            )
            write_arrow_batches_to_online_store(
                batches=batches,
                feature_view=feature_view,  # type: ignore[arg-type]
                online_store=self.online_store,
                repo_config=self.repo_config,
                concurrency=self.config.write_concurrency,
            )

            return TrinoMaterializationJob(
                job_id=job_id, status=MaterializationJobStatus.SUCCEEDED
            )
        except Exception as e:
            return TrinoMaterializationJob(
                job_id=job_id, status=MaterializationJobStatus.ERROR, error=e
            )

    def get_historical_features(
        self, registry: BaseRegistry, task: HistoricalRetrievalTask
    ) -> RetrievalJob:
        """Compile and return lazy Trino historical retrieval job."""
        context = self.get_execution_context(registry, task)

        try:
            if self.client is None:
                raise RuntimeError(
                    "Trino client is not configured. Set host, catalog, and user "
                    "in batch_engine config or use a TrinoOfflineStoreConfig."
                )
            builder = TrinoFeatureBuilder(
                registry=registry,
                client=self.client,
                task=task,
            )
            plan = builder.build()

            return TrinoDAGRetrievalJob(
                client=self.client,
                plan=plan,
                context=context,
                config=self.repo_config,
                full_feature_names=task.full_feature_name,
                on_demand_feature_views=getattr(task, "on_demand_feature_views", None),
                metadata=getattr(task, "metadata", None),
            )
        except Exception as e:
            logger.error("Failed to build historical feature retrieval DAG: %s", e)
            return TrinoDAGRetrievalJob(
                client=self.client,
                plan=None,
                context=context,
                config=self.repo_config,
                full_feature_names=task.full_feature_name,
                on_demand_feature_views=getattr(task, "on_demand_feature_views", None),
                metadata=getattr(task, "metadata", None),
                error=e,
            )
