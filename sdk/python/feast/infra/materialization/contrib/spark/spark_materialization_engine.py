from dataclasses import dataclass
from datetime import datetime
from typing import Callable, List, Literal, Optional, Sequence, Union, cast

import pandas as pd
import pyarrow
from tqdm import tqdm

from feast.batch_feature_view import BatchFeatureView
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.infra.common.materialization_job import (
    MaterializationJob,
    MaterializationJobStatus,
    MaterializationTask,
)
from feast.infra.common.serde import SerializedArtifacts
from feast.infra.materialization.batch_materialization_engine import (
    BatchMaterializationEngine,
)
from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
    SparkOfflineStore,
    SparkRetrievalJob,
)
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.registry.base_registry import BaseRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.stream_feature_view import StreamFeatureView
from feast.utils import (
    _convert_arrow_to_proto,
    _get_column_names,
    _run_pyarrow_field_mapping,
)


class SparkMaterializationEngineConfig(FeastConfigBaseModel):
    """Batch Materialization Engine config for spark engine"""

    type: Literal["spark.engine"] = "spark.engine"
    """ Type selector"""

    partitions: int = 0
    """Number of partitions to use when writing data to online store. If 0, no repartitioning is done"""


@dataclass
class SparkMaterializationJob(MaterializationJob):
    def __init__(
        self,
        job_id: str,
        status: MaterializationJobStatus,
        error: Optional[BaseException] = None,
    ) -> None:
        super().__init__()
        self._job_id: str = job_id
        self._status: MaterializationJobStatus = status
        self._error: Optional[BaseException] = error

    def status(self) -> MaterializationJobStatus:
        return self._status

    def error(self) -> Optional[BaseException]:
        return self._error

    def should_be_retried(self) -> bool:
        return False

    def job_id(self) -> str:
        return self._job_id

    def url(self) -> Optional[str]:
        return None


class SparkMaterializationEngine(BatchMaterializationEngine):
    def update(
        self,
        project: str,
        views_to_delete: Sequence[
            Union[BatchFeatureView, StreamFeatureView, FeatureView, OnDemandFeatureView]
        ],
        views_to_keep: Sequence[
            Union[BatchFeatureView, StreamFeatureView, FeatureView, OnDemandFeatureView]
        ],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
    ):
        # Nothing to set up.
        pass

    def teardown_infra(
        self,
        project: str,
        fvs: Sequence[Union[BatchFeatureView, StreamFeatureView, FeatureView]],
        entities: Sequence[Entity],
    ):
        # Nothing to tear down.
        pass

    def __init__(
        self,
        *,
        repo_config: RepoConfig,
        offline_store: SparkOfflineStore,
        online_store: OnlineStore,
        **kwargs,
    ):
        if not isinstance(offline_store, SparkOfflineStore):
            raise TypeError(
                "SparkMaterializationEngine is only compatible with the SparkOfflineStore"
            )
        super().__init__(
            repo_config=repo_config,
            offline_store=offline_store,
            online_store=online_store,
            **kwargs,
        )

    def materialize(
        self, registry, tasks: List[MaterializationTask]
    ) -> List[MaterializationJob]:
        return [
            self._materialize_one(
                registry,
                task.feature_view,
                task.start_time,
                task.end_time,
                task.project,
                task.tqdm_builder,
            )
            for task in tasks
        ]

    def _materialize_one(
        self,
        registry: BaseRegistry,
        feature_view: Union[BatchFeatureView, StreamFeatureView, FeatureView],
        start_date: datetime,
        end_date: datetime,
        project: str,
        tqdm_builder: Callable[[int], tqdm],
    ):
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
                lambda x: _map_by_partition(x, serialized_artifacts), "status int"
            ).count()  # dummy action to force evaluation

            return SparkMaterializationJob(
                job_id=job_id, status=MaterializationJobStatus.SUCCEEDED
            )
        except BaseException as e:
            return SparkMaterializationJob(
                job_id=job_id, status=MaterializationJobStatus.ERROR, error=e
            )


def _map_by_partition(iterator, serialized_artifacts: SerializedArtifacts):
    for pdf in iterator:
        if pdf.shape[0] == 0:
            print("Skipping")
            return

        table = pyarrow.Table.from_pandas(pdf)

        (
            feature_view,
            online_store,
            _,
            repo_config,
        ) = serialized_artifacts.unserialize()

        if feature_view.batch_source.field_mapping is not None:
            # Spark offline store does the field mapping in pull_latest_from_table_or_query() call
            # This may be needed in future if this materialization engine supports other offline stores
            table = _run_pyarrow_field_mapping(
                table, feature_view.batch_source.field_mapping
            )

        join_key_to_value_type = {
            entity.name: entity.dtype.to_value_type()
            for entity in feature_view.entity_columns
        }

        rows_to_write = _convert_arrow_to_proto(
            table, feature_view, join_key_to_value_type
        )
        online_store.online_write_batch(
            repo_config,
            feature_view,
            rows_to_write,
            lambda x: None,
        )

    yield pd.DataFrame(
        [pd.Series(range(1, 2))]
    )  # dummy result because mapInPandas needs to return something
