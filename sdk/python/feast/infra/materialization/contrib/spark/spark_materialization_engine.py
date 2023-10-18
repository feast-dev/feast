from dataclasses import dataclass
from datetime import datetime
from typing import Callable, List, Literal, Optional, Sequence, Union, cast

import dill
import pandas as pd
import pyarrow
from tqdm import tqdm

from feast.batch_feature_view import BatchFeatureView
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.infra.materialization.batch_materialization_engine import (
    BatchMaterializationEngine,
    MaterializationJob,
    MaterializationJobStatus,
    MaterializationTask,
)
from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
    SparkOfflineStore,
    SparkRetrievalJob,
)
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.passthrough_provider import PassthroughProvider
from feast.infra.registry.base_registry import BaseRegistry
from feast.protos.feast.core.FeatureView_pb2 import FeatureView as FeatureViewProto
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
            Union[BatchFeatureView, StreamFeatureView, FeatureView]
        ],
        views_to_keep: Sequence[
            Union[BatchFeatureView, StreamFeatureView, FeatureView]
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

            spark_serialized_artifacts = _SparkSerializedArtifacts.serialize(
                feature_view=feature_view, repo_config=self.repo_config
            )

            spark_df = offline_job.to_spark_df()
            if self.repo_config.batch_engine.partitions != 0:
                spark_df = spark_df.repartition(
                    self.repo_config.batch_engine.partitions
                )

            spark_df.foreachPartition(
                lambda x: _process_by_partition(x, spark_serialized_artifacts)
            )

            return SparkMaterializationJob(
                job_id=job_id, status=MaterializationJobStatus.SUCCEEDED
            )
        except BaseException as e:
            return SparkMaterializationJob(
                job_id=job_id, status=MaterializationJobStatus.ERROR, error=e
            )


@dataclass
class _SparkSerializedArtifacts:
    """Class to assist with serializing unpicklable artifacts to the spark workers"""

    feature_view_proto: str
    repo_config_byte: str

    @classmethod
    def serialize(cls, feature_view, repo_config):

        # serialize to proto
        feature_view_proto = feature_view.to_proto().SerializeToString()

        # serialize repo_config to disk. Will be used to instantiate the online store
        repo_config_byte = dill.dumps(repo_config)

        return _SparkSerializedArtifacts(
            feature_view_proto=feature_view_proto, repo_config_byte=repo_config_byte
        )

    def unserialize(self):
        # unserialize
        proto = FeatureViewProto()
        proto.ParseFromString(self.feature_view_proto)
        feature_view = FeatureView.from_proto(proto)

        # load
        repo_config = dill.loads(self.repo_config_byte)

        provider = PassthroughProvider(repo_config)
        online_store = provider.online_store
        return feature_view, online_store, repo_config


def _process_by_partition(rows, spark_serialized_artifacts: _SparkSerializedArtifacts):
    """Load pandas df to online store"""

    # convert to pyarrow table
    dicts = []
    for row in rows:
        dicts.append(row.asDict())

    df = pd.DataFrame.from_records(dicts)
    if df.shape[0] == 0:
        print("Skipping")
        return

    table = pyarrow.Table.from_pandas(df)

    # unserialize artifacts
    feature_view, online_store, repo_config = spark_serialized_artifacts.unserialize()

    if feature_view.batch_source.field_mapping is not None:
        table = _run_pyarrow_field_mapping(
            table, feature_view.batch_source.field_mapping
        )

    join_key_to_value_type = {
        entity.name: entity.dtype.to_value_type()
        for entity in feature_view.entity_columns
    }

    rows_to_write = _convert_arrow_to_proto(table, feature_view, join_key_to_value_type)
    online_store.online_write_batch(
        repo_config,
        feature_view,
        rows_to_write,
        lambda x: None,
    )
