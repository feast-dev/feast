import logging
import uuid
from datetime import datetime
from time import sleep
from typing import Callable, List, Literal, Sequence, Union

import yaml
from pydantic import StrictStr
from tqdm import tqdm

from feast import FeatureView, RepoConfig
from feast.batch_feature_view import BatchFeatureView
from feast.entity import Entity
from feast.infra.materialization.batch_materialization_engine import (
    BatchMaterializationEngine,
    MaterializationJob,
    MaterializationJobStatus,
    MaterializationTask,
)
from feast.infra.offline_stores.offline_store import OfflineStore
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.registry.base_registry import BaseRegistry
from feast.repo_config import FeastConfigBaseModel
from feast.stream_feature_view import StreamFeatureView
from feast.utils import _get_column_names

from ...local_engine import LocalMaterializationEngine, LocalMaterializationEngineConfig

logger = logging.getLogger(__name__)


class BytewaxMaterializationEngineConfig(LocalMaterializationEngineConfig):
    """Batch Materialization Engine config for Bytewax"""

    type: Literal["bytewax"] = "bytewax"
    """ Materialization type selector"""

    namespace: StrictStr = "default"
    """ (optional) The namespace in Kubernetes to use when creating services, configuration maps and jobs.
    """

    image: StrictStr = "bytewax/bytewax-feast:latest"
    """ (optional) The container image to use when running the materialization job."""

    env: List[dict] = []
    """ (optional) A list of environment variables to set in the created Kubernetes pods.
    These environment variables can be used to reference Kubernetes secrets.
    """

    image_pull_secrets: List[dict] = []
    """ (optional) The secrets to use when pulling the image to run for the materialization job """

    resources: dict = {}
    """ (optional) The resource requests and limits for the materialization containers """

    service_account_name: StrictStr = ""
    """ (optional) The service account name to use when running the job """

    annotations: dict = {}
    """ (optional) Annotations to apply to the job container. Useful for linking the service account to IAM roles, operational metadata, etc  """

    include_security_context_capabilities: bool = True
    """ (optional)  Include security context capabilities in the init and job container spec """

    labels: dict = {}
    """ (optional) additional labels to append to kubernetes objects """

    max_parallelism: int = 10
    """ (optional) Maximum number of pods allowed to run in parallel"""

    synchronous: bool = False
    """ (optional) If true, wait for materialization for one feature to complete before moving to the next """

    retry_limit: int = 2
    """ (optional) Maximum number of times to retry a materialization worker pod"""

    mini_batch_size: int = 1000
    """ (optional) Number of rows to process per write operation (default 1000)"""

    active_deadline_seconds: int = 86400
    """ (optional) Maximum amount of time a materialization job is allowed to run"""

    job_batch_size: int = 100
    """ (optional) Maximum number of pods to process per job.  Only applies to synchronous materialization"""

    print_pod_logs_on_failure: bool = True
    """(optional) Print pod logs on job failure.  Only applies to synchronous materialization"""


class BytewaxMaterializationEngine(LocalMaterializationEngine):
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
        self.repo_config = repo_config
        self.offline_store = offline_store
        self.online_store = online_store

        self.batch_engine_config = repo_config.batch_engine
        self.namespace = self.batch_engine_config.namespace

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
        """This method ensures that any necessary infrastructure or resources needed by the
        engine are set up ahead of materialization."""
        pass

    def teardown_infra(
        self,
        project: str,
        fvs: Sequence[Union[BatchFeatureView, StreamFeatureView, FeatureView]],
        entities: Sequence[Entity],
    ):
        """This method ensures that any infrastructure or resources set up by ``update()``are torn down."""
        pass

    def materialize(
        self,
        registry: BaseRegistry,
        tasks: List[MaterializationTask],
    ) -> List[MaterializationJob]:
        super().materialize(self, registry, tasks)
