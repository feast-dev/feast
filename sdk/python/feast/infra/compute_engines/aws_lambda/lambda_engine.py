import base64
import json
import logging
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import Literal, Optional, Sequence, Union

import boto3
import pyarrow as pa
from botocore.config import Config
from pydantic import StrictStr

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
from feast.infra.common.retrieval_task import HistoricalRetrievalTask
from feast.infra.compute_engines.base import ComputeEngine
from feast.infra.offline_stores.offline_store import OfflineStore
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.registry.base_registry import BaseRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.stream_feature_view import StreamFeatureView
from feast.utils import _get_column_names
from feast.version import get_version

DEFAULT_BATCH_SIZE = 10_000
DEFAULT_TIMEOUT = 600
LAMBDA_TIMEOUT_RETRIES = 5

logger = logging.getLogger(__name__)


class LambdaComputeEngineConfig(FeastConfigBaseModel):
    """Batch Compute Engine config for lambda based engine"""

    type: Literal["lambda"] = "lambda"
    """ Type selector"""

    materialization_image: StrictStr
    """ The URI of a container image in the Amazon ECR registry, which should be used for materialization. """

    lambda_role: StrictStr
    """ Role that should be used by the materialization lambda """


@dataclass
class LambdaMaterializationJob(MaterializationJob):
    def __init__(
        self,
        job_id: str,
        status: MaterializationJobStatus,
        error: Optional[BaseException] = None,
    ) -> None:
        super().__init__()
        self._job_id: str = job_id
        self._status = status
        self._error = error

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


class LambdaComputeEngine(ComputeEngine):
    """
    WARNING: This engine should be considered "Alpha" functionality.
    """

    def get_historical_features(
        self, registry: BaseRegistry, task: HistoricalRetrievalTask
    ) -> pa.Table:
        raise NotImplementedError(
            "Lambda Compute Engine does not support get_historical_features"
        )

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
            Timeout=DEFAULT_TIMEOUT,
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
        config = Config(read_timeout=DEFAULT_TIMEOUT + 10)
        self.lambda_client = boto3.client("lambda", config=config)

    def _materialize_one(
        self, registry: BaseRegistry, task: MaterializationTask, **kwargs
    ):
        feature_view = task.feature_view
        start_date = task.start_time
        end_date = task.end_time
        project = task.project

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
        if (num_files := len(paths)) == 0:
            logger.warning("No values to update for the given time range.")
            return LambdaMaterializationJob(
                job_id=job_id, status=MaterializationJobStatus.SUCCEEDED
            )
        else:
            max_workers = num_files if num_files <= 20 else 20
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
                        self.invoke_with_retries,
                        FunctionName=self.lambda_name,
                        InvocationType="RequestResponse",
                        Payload=json.dumps(payload),
                    )
                )

            done, not_done = wait(futures)
            logger.info("Done: %s Not Done: %s", done, not_done)
            errors = []
            for f in done:
                response, payload = f.result()

                logger.info(
                    f"Ingested task; request id {response['ResponseMetadata']['RequestId']}, "
                    f"Output: {payload}"
                )
                if "errorMessage" in payload.keys():
                    errors.append(payload["errorMessage"])

            for f in not_done:
                response, payload = f.result()
                logger.error(f"Ingestion failed: {response=}, {payload=}")

            if len(not_done) == 0 and len(errors) == 0:
                return LambdaMaterializationJob(
                    job_id=job_id, status=MaterializationJobStatus.SUCCEEDED
                )
            else:
                return LambdaMaterializationJob(
                    job_id=job_id,
                    status=MaterializationJobStatus.ERROR,
                    error=RuntimeError(
                        f"Lambda functions did not finish successfully: {errors}"
                    ),
                )

    def invoke_with_retries(self, **kwargs):
        """Invoke the Lambda function and retry if it times out.

        The Lambda function may time out initially if many values are updated
        and DynamoDB throttles requests. As soon as the DynamoDB tables
        are scaled up, the Lambda function can succeed upon retry with higher
        throughput.

        """
        retries = 0
        while retries < LAMBDA_TIMEOUT_RETRIES:
            response = self.lambda_client.invoke(**kwargs)
            payload = json.loads(response["Payload"].read()) or {}
            if "Task timed out after" not in payload.get("errorMessage", ""):
                break
            retries += 1
            logger.warning(
                "Retrying lambda function after lambda timeout in request"
                f"{response['ResponseMetadata']['RequestId']}"
            )
        return response, payload
