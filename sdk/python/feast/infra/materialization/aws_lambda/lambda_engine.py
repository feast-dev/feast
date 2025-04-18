import base64
import json
import logging
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, List, Literal, Optional, Sequence, Union

import boto3
from pydantic import StrictStr
from tqdm import tqdm

from feast import utils
from feast.batch_feature_view import BatchFeatureView
from feast.constants import FEATURE_STORE_YAML_ENV_NAME
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.infra.common.materialization_job import (
    MaterializationJob,
    MaterializationJobStatus,
    MaterializationTask,
)
from feast.infra.materialization.batch_materialization_engine import (
    BatchMaterializationEngine,
)
from feast.infra.offline_stores.offline_store import OfflineStore
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.registry.base_registry import BaseRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.stream_feature_view import StreamFeatureView
from feast.utils import _get_column_names
from feast.version import get_version

DEFAULT_BATCH_SIZE = 10_000

logger = logging.getLogger(__name__)


class LambdaMaterializationEngineConfig(FeastConfigBaseModel):
    """Batch Materialization Engine config for lambda based engine"""

    type: Literal["lambda"] = "lambda"
    """ Type selector"""

    materialization_image: StrictStr
    """ The URI of a container image in the Amazon ECR registry, which should be used for materialization. """

    lambda_role: StrictStr
    """ Role that should be used by the materialization lambda """


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
    """
    WARNING: This engine should be considered "Alpha" functionality.
    """

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
        # This should be setting up the lambda function.
        r = self.lambda_client.create_function(
            FunctionName=self.lambda_name,
            PackageType="Image",
            Role=self.repo_config.batch_engine.lambda_role,
            Code={"ImageUri": self.repo_config.batch_engine.materialization_image},
            Timeout=600,
            Tags={
                "feast-owned": "True",
                "project": project,
                "feast-sdk-version": get_version(),
            },
        )
        logger.info(
            "Creating lambda function %s, %s",
            self.lambda_name,
            r["ResponseMetadata"]["RequestId"],
        )

        logger.info("Waiting for function %s to be active", self.lambda_name)
        waiter = self.lambda_client.get_waiter("function_active")
        waiter.wait(FunctionName=self.lambda_name)

    def teardown_infra(
        self,
        project: str,
        fvs: Sequence[Union[BatchFeatureView, StreamFeatureView, FeatureView]],
        entities: Sequence[Entity],
    ):
        # This should be tearing down the lambda function.
        logger.info("Tearing down lambda %s", self.lambda_name)
        r = self.lambda_client.delete_function(FunctionName=self.lambda_name)
        logger.info("Finished tearing down lambda %s: %s", self.lambda_name, r)

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
        feature_store_path = utils.get_default_yaml_file_path(repo_path)
        self.feature_store_base64 = str(
            base64.b64encode(bytes(feature_store_path.read_text(), "UTF-8")), "UTF-8"
        )

        self.lambda_name = f"feast-materialize-{self.repo_config.project}"
        if len(self.lambda_name) > 64:
            self.lambda_name = self.lambda_name[:64]
        self.lambda_client = boto3.client("lambda")

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
        max_workers = len(paths) if len(paths) <= 20 else 20
        executor = ThreadPoolExecutor(max_workers=max_workers)
        futures = []

        for path in paths:
            payload = {
                FEATURE_STORE_YAML_ENV_NAME: self.feature_store_base64,
                "view_name": feature_view.name,
                "view_type": "batch",
                "path": path,
            }
            # Invoke a lambda to materialize this file.

            logger.info("Invoking materialization for %s", path)
            futures.append(
                executor.submit(
                    self.lambda_client.invoke,
                    FunctionName=self.lambda_name,
                    InvocationType="RequestResponse",
                    Payload=json.dumps(payload),
                )
            )

        done, not_done = wait(futures)
        logger.info("Done: %s Not Done: %s", done, not_done)
        for f in done:
            response = f.result()
            output = json.loads(response["Payload"].read())

            logger.info(
                f"Ingested task; request id {response['ResponseMetadata']['RequestId']}, "
                f"Output: {output}"
            )

        for f in not_done:
            response = f.result()
            logger.error(f"Ingestion failed: {response}")

        return LambdaMaterializationJob(
            job_id=job_id,
            status=MaterializationJobStatus.SUCCEEDED
            if not not_done
            else MaterializationJobStatus.ERROR,
        )
