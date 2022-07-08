import base64
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, List, Literal, Optional, Sequence, Union

import boto3
from tqdm import tqdm

from feast.batch_feature_view import BatchFeatureView
from feast.constants import FEATURE_STORE_YAML_ENV_NAME
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.infra.materialization.batch_materialization_engine import (
    BatchMaterializationEngine,
    MaterializationJob,
    MaterializationJobStatus,
    MaterializationTask,
)
from feast.infra.offline_stores.offline_store import OfflineStore
from feast.infra.online_stores.online_store import OnlineStore
from feast.registry import BaseRegistry
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.stream_feature_view import StreamFeatureView
from feast.utils import _get_column_names

DEFAULT_BATCH_SIZE = 10_000


class LambdaMaterializationEngineConfig(FeastConfigBaseModel):
    """Batch Materialization Engine config for lambda based engine"""

    type: Literal["lambda"] = "lambda"
    """ Type selector"""


@dataclass
class LambdaMaterializationJob(MaterializationJob):
    def __init__(self, job_id: str, status: MaterializationJobStatus) -> None:
        super().__init__()
        self._job_id: str = job_id
        self._status = status
        self._error = None

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


class LambdaMaterializationEngine(BatchMaterializationEngine):

    LAMBDA_NAME = "feast-lambda-consumer"

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
        # This should be setting up the lambda function.
        pass

    def teardown_infra(
        self,
        project: str,
        fvs: Sequence[Union[BatchFeatureView, StreamFeatureView, FeatureView]],
        entities: Sequence[Entity],
    ):
        # This should be tearing down the lambda function.
        pass

    def __init__(
        self,
        *,
        repo_config: RepoConfig,
        offline_store: OfflineStore,
        online_store: OnlineStore,
        **kwargs,
    ):
        super().__init__(
            repo_config=repo_config,
            offline_store=offline_store,
            online_store=online_store,
            **kwargs,
        )
        repo_path = self.repo_config.repo_path
        assert repo_path
        feature_store_path = repo_path / "feature_store.yaml"
        self.feature_store_base64 = str(
            base64.b64encode(bytes(feature_store_path.read_text(), "UTF-8")), "UTF-8"
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

        offline_job = self.offline_store.pull_latest_from_table_or_query(
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

        for path in paths:
            payload = {
                FEATURE_STORE_YAML_ENV_NAME: self.feature_store_base64,
                "view_name": feature_view.name,
                "view_type": "batch",
                "path": path,
            }
            # Invoke a lambda to materialize this file.
            lambda_client = boto3.client("lambda")
            response = lambda_client.invoke(
                FunctionName=self.LAMBDA_NAME,
                InvocationType="Event",
                Payload=json.dumps(payload),
            )
            print(response)

        return LambdaMaterializationJob(
            job_id=job_id, status=MaterializationJobStatus.SUCCEEDED
        )
