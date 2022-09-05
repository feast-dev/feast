from dataclasses import dataclass
from datetime import datetime
import os
import tempfile
from typing import Callable, List, Literal, Optional, Sequence, Union

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
from feast.infra.offline_stores.contrib.spark_offline_store.spark import SparkOfflineStore, SparkRetrievalJob
from feast.infra.offline_stores.offline_store import OfflineStore
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.registry.base_registry import BaseRegistry
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.stream_feature_view import StreamFeatureView
from feast.utils import (
    _convert_arrow_to_proto,
    _get_column_names,
    _run_pyarrow_field_mapping,
)
import pyarrow.parquet as pq
from pyspark.sql.functions import udf, col, explode
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
from pyspark.sql import Row
import pandas as pd

DEFAULT_BATCH_SIZE = 10_000

from feast.protos.feast.core.FeatureView_pb2 import FeatureView as FeatureViewProto
from feast.infra.passthrough_provider import PassthroughProvider

import dill

class SparkMaterializationEngineConfig(FeastConfigBaseModel):
    """Batch Materialization Engine config for spark engine"""

    type: Literal["spark"] = "spark"
    """ Type selector"""


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
        if not isinstance(offline_store,SparkOfflineStore):
            raise TypeError("SparkMaterializationEngine is only compatible with the SparkOfflineStore")
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
            offline_job: SparkRetrievalJob = self.offline_store.pull_latest_from_table_or_query(
                config=self.repo_config,
                data_source=feature_view.batch_source,
                join_key_columns=join_key_columns,
                feature_name_columns=feature_name_columns,
                timestamp_field=timestamp_field,
                created_timestamp_column=created_timestamp_column,
                start_date=start_date,
                end_date=end_date,
            )

            paths = offline_job.to_remote_storage()
            paths = [x for x in paths if x.endswith(".parquet")]
            
            # lazy load data
            spark_session = offline_job.spark_session
            spark_df = spark_session.createDataFrame([tuple(paths)],schema=["path"])
            
            # serialize to send to executors
            feature_view_proto = feature_view.to_proto().SerializeToString()
            
            # execute
            repo_config_file = tempfile.NamedTemporaryFile(delete=False).name
            with open(repo_config_file,"wb") as f:
                dill.dump(self.repo_config,f)
                
            rdd2 = spark_df.rdd.map(lambda row: _process_spark_data_to_online_store(row=row,feature_view_proto=feature_view_proto,repo_config_file=repo_config_file))
            rdd2.toDF().collect()
            
            
            
            # table = offline_job.to_arrow()

            # if feature_view.batch_source.field_mapping is not None:
            #     table = _run_pyarrow_field_mapping(
            #         table, feature_view.batch_source.field_mapping
            #     )

            # join_key_to_value_type = {
            #     entity.name: entity.dtype.to_value_type()
            #     for entity in feature_view.entity_columns
            # }

            # with tqdm_builder(table.num_rows) as pbar:
            #     for batch in table.to_batches(DEFAULT_BATCH_SIZE):
            #         rows_to_write = _convert_arrow_to_proto(
            #             batch, feature_view, join_key_to_value_type
            #         )
            #         self.online_store.online_write_batch(
            #             self.repo_config,
            #             feature_view,
            #             rows_to_write,
            #             lambda x: pbar.update(x),
            #         )
            return SparkMaterializationJob(
                job_id=job_id, status=MaterializationJobStatus.SUCCEEDED
            )
        except BaseException as e:
            return SparkMaterializationJob(
                job_id=job_id, status=MaterializationJobStatus.ERROR, error=e
            )

def _process_spark_data_to_online_store(row,feature_view_proto,repo_config_file):
    
    # try:
        
        path = row["path"]
        
        # unserialize
        proto = FeatureViewProto()
        proto.ParseFromString(feature_view_proto)
        feature_view = FeatureView.from_proto(proto)
        
        # load
        with open(repo_config_file,"rb") as f:
            repo_config = dill.load(f)
        
        provider = PassthroughProvider(repo_config)
        online_store = provider.online_store
        
        table = pq.read_table(path)
        
        content = f"{path}: {pd.read_parquet(path).shape[0]}"
        with open("/Users/niklasvonmaltzahn/Documents/feast/log.txt","a") as f:
            f.write(content)
        
        if feature_view.batch_source.field_mapping is not None:
            table = _run_pyarrow_field_mapping(
                table, feature_view.batch_source.field_mapping
            )

        join_key_to_value_type = {
            entity.name: entity.dtype.to_value_type()
            for entity in feature_view.entity_columns
        }

        DEFAULT_BATCH_SIZE = 200
        for batch in table.to_batches(DEFAULT_BATCH_SIZE):
            rows_to_write = _convert_arrow_to_proto(
                batch, feature_view, join_key_to_value_type
            )
            online_store.online_write_batch(
                repo_config,
                feature_view,
                rows_to_write,
                lambda x: None,
            )
        
        return ("SUCCESS",)
    # except Exception as e:
    #     return (str(e),)